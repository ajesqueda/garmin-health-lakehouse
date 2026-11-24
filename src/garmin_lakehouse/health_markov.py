"""Predict tomorrow's sleep/HRV state with a Markov chain learned from your own history.

Each day is put in one of four states by two numbers: hours slept and overnight HRV.

                     HRV at/above your usual     HRV below your usual
    slept >= 7 h     well rested                 slept well, HRV low
    slept <  7 h     short sleep, HRV ok         run down

"Your usual" HRV is the median of your own history, since HRV is very personal.
Counting how often each state is followed by each other state gives a transition
matrix P (columns are "from", like the course). Today's state picks a column, and
P^k gives the odds k days ahead; the eigenvector of P for λ = 1 is the long-run mix.

    python -m garmin_lakehouse.health_markov --sleep 6.5 --hrv 42
    python -m garmin_lakehouse.health_markov --sleep 8 --hrv 55 --days 3

History comes live from gold_daily_recovery through the Databricks CLI (profile
`free` by default), so it needs a valid `databricks auth login`.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import statistics
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, timedelta

import numpy as np

STATES = ["well rested", "short sleep, HRV ok", "slept well, HRV low", "run down"]
DEFAULT_SLEEP_HOURS = 7.0
CLI_TIMEOUT_S = 120
QUERY_DEADLINE_S = 300
TABLE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}")
QUERY = (
    "SELECT calendar_date, sleep_hours, hrv_last_night "
    "FROM {table} ORDER BY calendar_date"
)


@dataclass(frozen=True)
class Day:
    day: date
    sleep_hours: float | None
    hrv: float | None


@dataclass
class Model:
    counts: np.ndarray  # counts[to, from]
    P: np.ndarray  # smoothed, columns sum to 1
    sleep_cut: float
    hrv_cut: float
    days_used: int
    hrv_cut_is_median: bool = True


def classify(sleep_hours: float, hrv: float, sleep_cut: float, hrv_cut: float) -> int:
    """Index into STATES. Exactly on a cut counts as the better side."""
    short = sleep_hours < sleep_cut
    low = hrv < hrv_cut
    return 2 * low + short


def transitions(days: Sequence[Day], sleep_cut: float, hrv_cut: float) -> list[tuple[int, int]]:
    """(from, to) state pairs for consecutive calendar days that both have data.

    A gap (a missing day, or a day without sleep or HRV) breaks the chain: pairing
    Monday with Wednesday would count a two-day move as a one-day move.
    """
    pairs = []
    ordered = sorted(days, key=lambda d: d.day)
    for prev, cur in zip(ordered, ordered[1:], strict=False):
        if cur.day - prev.day != timedelta(days=1):
            continue
        if None in (prev.sleep_hours, prev.hrv, cur.sleep_hours, cur.hrv):
            continue
        pairs.append((classify(prev.sleep_hours, prev.hrv, sleep_cut, hrv_cut),
                      classify(cur.sleep_hours, cur.hrv, sleep_cut, hrv_cut)))
    return pairs


def fit(days: Sequence[Day], sleep_cut: float = DEFAULT_SLEEP_HOURS,
        hrv_cut: float | None = None, smoothing: float = 1.0) -> Model:
    """Count transitions and turn each "from" column into probabilities.

    Add-one smoothing adds `smoothing` to every count first, so a move you haven't
    seen yet gets a small chance instead of exactly 0. With a few weeks of data,
    many moves simply haven't happened yet.
    """
    if smoothing < 0:
        raise ValueError("smoothing can't be negative: it would make negative odds")
    hrvs = [d.hrv for d in days if d.hrv is not None]
    from_median = hrv_cut is None
    if hrv_cut is None:
        if not hrvs:
            raise ValueError("no HRV values in the history")
        hrv_cut = statistics.median(hrvs)
    counts = np.zeros((4, 4))
    pairs = transitions(days, sleep_cut, hrv_cut)
    for frm, to in pairs:
        counts[to, frm] += 1
    smoothed = counts + smoothing
    totals = smoothed.sum(axis=0, keepdims=True)
    # A state never seen as "from" (possible only without smoothing) has no
    # information, so spread its column evenly instead of dividing 0 by 0.
    P = np.where(totals > 0, smoothed / np.where(totals > 0, totals, 1), 1 / 4)
    return Model(counts, P, sleep_cut, hrv_cut, len(pairs), from_median)


def predict(model: Model, state: int, k: int = 1) -> np.ndarray:
    """Probabilities of each state k days after a day in `state`: P^k e_state."""
    start = np.zeros(4)
    start[state] = 1.0
    return np.linalg.matrix_power(model.P, k) @ start


def long_run(model: Model) -> np.ndarray:
    """The steady state: P's eigenvector for λ = 1, scaled to add up to 1."""
    vals, vecs = np.linalg.eig(model.P)
    v = vecs[:, int(np.argmin(np.abs(vals - 1)))].real
    return v / v.sum()


# --- data -----------------------------------------------------------------------------

def _databricks(args: list[str], profile: str) -> dict:
    """Run the Databricks CLI (it handles the login) and parse its JSON output."""
    try:
        run = subprocess.run(["databricks", *args, "--profile", profile, "-o", "json"],
                             capture_output=True, text=True, timeout=CLI_TIMEOUT_S)
    except FileNotFoundError:
        raise RuntimeError("the databricks CLI isn't installed or isn't on PATH") from None
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"databricks {args[0]} didn't answer within {CLI_TIMEOUT_S} s") from None
    except OSError as exc:
        raise RuntimeError(f"couldn't run the databricks CLI: {exc}") from None
    if run.returncode != 0:
        if "invalid_grant" in run.stderr or "auth login" in run.stderr:
            raise RuntimeError(f"Databricks login expired: run `databricks auth login "
                               f"--profile {profile}` and try again")
        raise RuntimeError(f"databricks CLI failed: {run.stderr.strip()[:300]}")
    return json.loads(run.stdout)


def find_warehouse(profile: str) -> str:
    """The first SQL warehouse in the workspace (Free Edition has one)."""
    found = _databricks(["warehouses", "list"], profile)
    warehouses = found if isinstance(found, list) else found.get("warehouses", [])
    if not warehouses:
        raise RuntimeError("no SQL warehouse found; pass --warehouse")
    return warehouses[0]["id"]


def load_from_databricks(table: str, profile: str, warehouse: str | None,
                         sleep: Callable[[float], None] = time.sleep,
                         clock: Callable[[], float] = time.monotonic) -> list[Day]:
    """Read sleep and HRV for every day from the gold table.

    A cold warehouse can take longer than the first 50 s wait, so a statement that is
    still pending or running is polled until it finishes. Big results come back in
    chunks, and every chunk is read.
    """
    if not TABLE_NAME.fullmatch(table):
        raise ValueError(f"not a table name: {table!r} (expected catalog.schema.table)")
    warehouse = warehouse or find_warehouse(profile)
    body = json.dumps({"warehouse_id": warehouse, "statement": QUERY.format(table=table),
                       "wait_timeout": "50s"})
    result = _databricks(["api", "post", "/api/2.0/sql/statements", "--json", body], profile)
    deadline = clock() + QUERY_DEADLINE_S
    while result.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        if clock() > deadline:
            raise RuntimeError(f"query still running after {QUERY_DEADLINE_S} s")
        sleep(5)
        result = _databricks(["api", "get", f"/api/2.0/sql/statements/{result['statement_id']}"],
                             profile)
    state = result.get("status", {}).get("state")
    if state != "SUCCEEDED":
        raise RuntimeError(f"query {state}: {result.get('status', {}).get('error')}")
    chunk = result.get("result") or {}
    rows = list(chunk.get("data_array") or [])
    while chunk.get("next_chunk_index") is not None:
        chunk = _databricks(["api", "get", f"/api/2.0/sql/statements/{result['statement_id']}"
                             f"/result/chunks/{chunk['next_chunk_index']}"], profile)
        rows += chunk.get("data_array") or []
    return [Day(date.fromisoformat(d), None if s is None else float(s),
                None if h is None else float(h)) for d, s, h in rows]


# --- CLI ------------------------------------------------------------------------------------

def _pct(x: float) -> str:
    return f"{100 * x:5.1f}%"


def report(model: Model, sleep_hours: float, hrv: float, days_ahead: int) -> str:
    state = classify(sleep_hours, hrv, model.sleep_cut, model.hrv_cut)
    seen_from = int(model.counts[:, state].sum())
    lines = [
        f"Today: {sleep_hours:g} h sleep, HRV {hrv:g} ms -> {STATES[state]}",
        f"  (cuts: sleep {model.sleep_cut:g} h, HRV {model.hrv_cut:g} ms "
        f"{'= your median' if model.hrv_cut_is_median else 'from --hrv-cut'})",
        "",
        f"In {days_ahead} day{'s' if days_ahead != 1 else ''}:",
    ]
    probs = predict(model, state, days_ahead)
    for i in np.argsort(-probs):
        lines.append(f"  {_pct(probs[i])}  {STATES[i]}")
    lines += ["", "Long run, share of days:"]
    for i, p in enumerate(long_run(model)):
        lines.append(f"  {_pct(p)}  {STATES[i]}")
    lines += [
        "",
        f"Based on {model.days_used} day-to-day transitions; {seen_from} of them start from "
        f"'{STATES[state]}'.",
    ]
    if seen_from < 10:
        lines.append("Few examples from this state, so these odds lean on smoothing and will "
                     "firm up as more days come in.")
    return "\n".join(lines)


def main(argv: list[str] | None = None,
         loader: Callable[[str, str, str | None], list[Day]] = load_from_databricks) -> int:
    parser = argparse.ArgumentParser(
        description="Predict your sleep/HRV state with a Markov chain learned from your history")
    parser.add_argument("--sleep", type=float, required=True, help="hours slept last night")
    parser.add_argument("--hrv", type=float, required=True, help="overnight HRV in ms")
    parser.add_argument("--days", type=int, default=1, help="how many days ahead (default 1)")
    parser.add_argument("--sleep-cut", type=float, default=DEFAULT_SLEEP_HOURS)
    parser.add_argument("--hrv-cut", type=float, help="default: median of your history")
    parser.add_argument("--table", default="workspace.garmin.gold_daily_recovery")
    parser.add_argument("--profile", default="free")
    parser.add_argument("--warehouse", default=os.environ.get("DATABRICKS_WAREHOUSE_ID"),
                        help="SQL warehouse id (default: $DATABRICKS_WAREHOUSE_ID or the first)")
    args = parser.parse_args(argv)
    if args.days < 1:
        parser.error("--days must be at least 1")
    numbers = [args.sleep, args.hrv, args.sleep_cut] + (
        [] if args.hrv_cut is None else [args.hrv_cut])
    if not all(math.isfinite(x) for x in numbers):
        parser.error("--sleep, --hrv and the cuts must be real numbers (not nan or inf)")
    if args.sleep < 0 or args.hrv <= 0:
        parser.error("--sleep must be >= 0 and --hrv > 0")

    try:
        days = loader(args.table, args.profile, args.warehouse)
        model = fit(days, args.sleep_cut, args.hrv_cut)
    except (RuntimeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(report(model, args.sleep, args.hrv, args.days))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

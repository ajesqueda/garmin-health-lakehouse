"""Make sure the daily Garmin job ran today; start it if it didn't.

On Databricks Free Edition the scheduler has twice stopped starting the 6 AM run
without pausing the job or logging anything, both times after a few days with no
interactive use of the workspace (see docs/setup.md). This script runs later in the
morning from your own computer (launchd on a Mac, see docs/setup.md):

- a run that started today and succeeded, or is still running -> nothing to do
- no run today, or only failed ones -> start one, at most once per day
- a run this script started today already failed -> leave it alone and say so

    python scripts/ensure_daily_run.py --profile free-pat
    python scripts/ensure_daily_run.py --profile free-pat --dry-run

Standard library only. It calls the Databricks CLI, so the profile needs a login
that doesn't expire after a week: a personal access token profile.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections.abc import Callable
from datetime import datetime
from zoneinfo import ZoneInfo

Cli = Callable[[list[str]], object]
ACTIVE = {"PENDING", "QUEUED", "RUNNING", "TERMINATING", "BLOCKED", "WAITING_FOR_RETRY"}


def databricks_cli(profile: str) -> Cli:
    def run(args: list[str]) -> object:
        proc = subprocess.run(["databricks", *args, "--profile", profile, "-o", "json"],
                              capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip()[:300] or f"databricks {args[0]} failed")
        return json.loads(proc.stdout) if proc.stdout.strip() else {}
    return run


def _items(found: object, key: str) -> list[dict]:
    return found if isinstance(found, list) else (found or {}).get(key, [])


def find_job(cli: Cli, name: str) -> int:
    jobs = _items(cli(["jobs", "list", "--name", name]), "jobs")
    if len(jobs) != 1:
        raise RuntimeError(f"expected one job named {name!r}, found {len(jobs)}")
    return int(jobs[0]["job_id"])


def start_of_today_ms(now: datetime) -> int:
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(midnight.timestamp() * 1000)


def decide(runs: list[dict]) -> tuple[str, str]:
    """("ok" | "start" | "leave", reason) from today's runs."""
    for r in runs:
        state = r.get("state", {})
        if state.get("result_state") == "SUCCESS":
            return "ok", f"run {r['run_id']} ({r.get('trigger')}) succeeded today"
        if state.get("life_cycle_state") in ACTIVE:
            return "ok", f"run {r['run_id']} ({r.get('trigger')}) is in progress"
    manual = [r for r in runs if r.get("trigger") == "ONE_TIME"]
    if manual:
        return "leave", (f"a manual run ({manual[0]['run_id']}) already failed today; "
                         "not retrying, check the run's error")
    if runs:
        return "start", f"{len(runs)} run(s) today, none succeeded"
    return "start", "no run today: the scheduler didn't start one"


def ensure(cli: Cli, job_name: str, now: datetime, dry_run: bool = False) -> tuple[str, str]:
    job_id = find_job(cli, job_name)
    runs = _items(cli(["jobs", "list-runs", "--job-id", str(job_id),
                       "--start-time-from", str(start_of_today_ms(now))]), "runs")
    action, reason = decide(runs)
    if action == "start" and not dry_run:
        started = cli(["jobs", "run-now", "--no-wait", "--json", json.dumps({"job_id": job_id})])
        reason += f"; started run {started.get('run_id')}"
    elif action == "start":
        reason += " (dry run: nothing started)"
    return action, reason


def main(argv: list[str] | None = None, cli_factory: Callable[[str], Cli] = databricks_cli,
         clock: Callable[[ZoneInfo], datetime] = datetime.now) -> int:
    parser = argparse.ArgumentParser(description="Start the daily Garmin job if it hasn't run")
    parser.add_argument("--profile", default="free-pat")
    parser.add_argument("--job-name", default="garmin-daily")
    parser.add_argument("--timezone", default="America/Chicago")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    now = clock(ZoneInfo(args.timezone))
    stamp = now.strftime("%Y-%m-%d %H:%M %Z")
    try:
        action, reason = ensure(cli_factory(args.profile), args.job_name, now, args.dry_run)
    except (RuntimeError, ValueError, KeyError) as exc:
        print(f"{stamp} ERROR {exc}")
        return 1
    print(f"{stamp} {action.upper()} {reason}")
    return 2 if action == "leave" else 0


if __name__ == "__main__":
    sys.exit(main())

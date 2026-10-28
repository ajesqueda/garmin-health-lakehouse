"""Pull daily Garmin Connect data into a landing folder as raw JSON files.

Each (endpoint, day) becomes one file:

    <landing>/<endpoint>/<YYYY-MM-DD>.json
    {"endpoint": "sleep", "calendar_date": "2026-10-26",
     "fetched_at": "2026-10-27T06:00:03+00:00", "payload": {...exactly what Garmin returned...}}

The payload is stored untouched. Bronze keeps it as a raw string, so a change in
Garmin's response shape can never break ingestion. Parsing happens in silver.

Authentication uses saved Garmin tokens (see scripts/create_garmin_tokens.py),
never a stored password. The landing folder can be a local directory or a
Unity Catalog Volume path such as /Volumes/workspace/garmin/landing, since
Databricks mounts Volumes as ordinary files.

    python -m garmin_lakehouse.ingest --landing ./data/landing --days 7
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

log = logging.getLogger(__name__)


class GarminClient(Protocol):
    """The subset of garminconnect.Garmin this module uses (makes testing easy)."""

    def get_user_summary(self, cdate: str) -> dict[str, Any]: ...
    def get_sleep_data(self, cdate: str) -> dict[str, Any]: ...
    def get_hrv_data(self, cdate: str) -> dict[str, Any] | None: ...
    def get_training_readiness(self, cdate: str) -> list[dict[str, Any]]: ...
    def get_activities_by_date(
        self, startdate: str, enddate: str | None = None
    ) -> list[dict[str, Any]]: ...


# endpoint name -> how to fetch one calendar day of it
ENDPOINTS: dict[str, Callable[[GarminClient, str], Any]] = {
    "daily_summary": lambda c, d: c.get_user_summary(d),
    "sleep": lambda c, d: c.get_sleep_data(d),
    "hrv": lambda c, d: c.get_hrv_data(d),
    "training_readiness": lambda c, d: c.get_training_readiness(d),
    # Activities are a date-range query; asking for a one-day range keeps
    # every endpoint at the same one-file-per-day grain.
    "activities": lambda c, d: c.get_activities_by_date(d, d),
}


@dataclass
class IngestReport:
    written: list[Path] = field(default_factory=list)
    skipped_existing: int = 0
    empty: int = 0
    failed: dict[str, str] = field(default_factory=dict)


def date_range(end: date, days: int) -> list[date]:
    """The `days` calendar days ending on `end`, oldest first."""
    return [end - timedelta(days=offset) for offset in reversed(range(days))]


def _write_atomic(path: Path, text: str) -> None:
    # Write to a temp file and rename, so Auto Loader never picks up a
    # half-written file if the job dies mid-write.
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(text, encoding="utf-8")
    try:
        os.replace(tmp, path)
    except OSError:
        # Some mounted filesystems (cloud Volume mounts) may not support
        # rename. The pipeline only reads *.json, so a direct write is still
        # safe from half-read temp files; it just loses crash atomicity.
        path.write_text(text, encoding="utf-8")
        tmp.unlink(missing_ok=True)


def ingest(
    client: GarminClient,
    landing: Path,
    days: Iterable[date],
    endpoints: Iterable[str] = ENDPOINTS,
    overwrite_recent: int = 2,
    today: date | None = None,
    pause_seconds: float = 1.0,
    sleep: Callable[[float], None] = time.sleep,
) -> IngestReport:
    """Fetch each endpoint for each day and write it to the landing folder.

    Files that already exist are skipped, so re-running is cheap and safe.
    The exception is the most recent `overwrite_recent` days: Garmin keeps
    updating "today" and "last night" as the watch syncs, so those are
    re-fetched every run. Bronze keeps every version; silver picks the latest.
    """
    today = today or date.today()
    report = IngestReport()
    fresh_cutoff = today - timedelta(days=overwrite_recent - 1)

    for day in days:
        cdate = day.isoformat()
        for endpoint in endpoints:
            fetch = ENDPOINTS[endpoint]
            target = landing / endpoint / f"{cdate}.json"
            refetch = day >= fresh_cutoff
            if target.exists() and not refetch:
                report.skipped_existing += 1
                continue
            try:
                payload = fetch(client, cdate)
            except Exception as exc:  # noqa: BLE001 - keep going on one bad day
                name = type(exc).__name__
                if "TooManyRequests" in name:
                    # Garmin rate limits; stop instead of hammering the API.
                    report.failed[f"{endpoint}/{cdate}"] = name
                    log.warning("rate limited by Garmin; stopping this run early")
                    return report
                report.failed[f"{endpoint}/{cdate}"] = f"{name}: {exc}"
                log.warning("failed %s %s: %s", endpoint, cdate, name)
                continue
            finally:
                sleep(pause_seconds)

            is_empty = payload in (None, {}, [])
            if is_empty:
                # No data for that day (watch not worn, feature unsupported).
                # Still write a file with a null payload: it records "we asked",
                # so later runs skip the day instead of re-asking forever.
                payload = None
                report.empty += 1
            if refetch and target.exists():
                # Keep the older version under a new name so bronze sees both.
                stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
                target = landing / endpoint / f"{cdate}__{stamp}.json"
            envelope = {
                "endpoint": endpoint,
                "calendar_date": cdate,
                "fetched_at": datetime.now(UTC).isoformat(timespec="seconds"),
                "payload": payload,
            }
            _write_atomic(target, json.dumps(envelope, sort_keys=True))
            if not is_empty:
                report.written.append(target)
    return report


def login_with_tokens(tokens: str) -> GarminClient:
    """Log in with saved token JSON (or a path to a token file). No password."""
    from garminconnect import Garmin

    client = Garmin()
    client.login(tokenstore=tokens)
    return client


def login_with_password(email: str, password: str) -> GarminClient:
    """Log in with email + password. Fails if the account has MFA turned on,
    because an unattended job has no one to type the code; use tokens then."""
    from garminconnect import Garmin

    client = Garmin(email, password)
    client.login()
    return client


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Pull Garmin Connect data to a landing folder")
    parser.add_argument("--landing", type=Path, required=True)
    parser.add_argument("--days", type=int, default=7, help="how many days back to pull")
    parser.add_argument("--end", type=date.fromisoformat, help="last day, YYYY-MM-DD")
    parser.add_argument(
        "--tokens",
        default=os.environ.get("GARMINTOKENS"),
        help="token JSON or token file path (default: $GARMINTOKENS)",
    )
    args = parser.parse_args(argv)
    if not args.tokens:
        parser.error("no tokens: run scripts/create_garmin_tokens.py first, then set GARMINTOKENS")

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    client = login_with_tokens(args.tokens)
    end = args.end or date.today()
    report = ingest(client, args.landing, date_range(end, args.days), today=end)
    log.info(
        "wrote %d files, skipped %d existing, %d empty, %d failed",
        len(report.written), report.skipped_existing, report.empty, len(report.failed),
    )
    return 1 if report.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())

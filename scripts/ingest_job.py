"""Databricks job entry point: read Garmin credentials from secrets, pull data to the Volume.

Secrets live in a Databricks secret scope (default name: garmin):

    tokens     saved token JSON from scripts/create_garmin_tokens.py  (preferred)
    email      Garmin account email      } used only if `tokens` is missing
    password   Garmin account password   }

Secret values are never printed; Databricks also redacts them in job output.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


def read_secret(dbutils, scope: str, key: str) -> str | None:
    try:
        return dbutils.secrets.get(scope=scope, key=key)
    except Exception:  # noqa: BLE001 - a missing key raises; treat it as "not set"
        return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", required=True, help="workspace path of the bundle's src/")
    parser.add_argument("--landing", required=True, help="Volume path for raw JSON")
    parser.add_argument("--secret-scope", default="garmin")
    parser.add_argument("--days", type=int, default=7)
    # Garmin days are local calendar days; serverless clocks run on UTC.
    parser.add_argument("--timezone", default="America/Chicago")
    args, _ = parser.parse_known_args()

    sys.path.insert(0, args.src)
    from databricks.sdk.runtime import dbutils

    from garmin_lakehouse.ingest import (
        date_range,
        ingest,
        login_with_password,
        login_with_tokens,
    )

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    log = logging.getLogger("ingest_job")

    tokens = read_secret(dbutils, args.secret_scope, "tokens")
    if tokens:
        log.info("logging in to Garmin with saved tokens")
        client = login_with_tokens(tokens)
    else:
        email = read_secret(dbutils, args.secret_scope, "email")
        password = read_secret(dbutils, args.secret_scope, "password")
        if not (email and password):
            raise SystemExit(
                f"No Garmin credentials in secret scope '{args.secret_scope}'. "
                "Add `tokens`, or `email` and `password` (see docs/setup.md)."
            )
        log.info("logging in to Garmin with email and password")
        client = login_with_password(email, password)

    today = datetime.now(ZoneInfo(args.timezone)).date()
    report = ingest(client, Path(args.landing), date_range(today, args.days), today=today)
    log.info(
        "wrote %d files, skipped %d existing, %d empty, %d failed",
        len(report.written), report.skipped_existing, report.empty, len(report.failed),
    )
    for key, reason in report.failed.items():
        log.warning("failed %s: %s", key, reason)
    # Partial failures (one endpoint, one day) shouldn't block the pipeline
    # refresh; a login failure has already raised above.
    return 0


if __name__ == "__main__":
    # Databricks marks the task failed on any SystemExit, even SystemExit(0),
    # so only raise when there's a real error code.
    exit_code = main()
    if exit_code:
        raise SystemExit(exit_code)

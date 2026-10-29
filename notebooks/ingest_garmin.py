# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Garmin Connect → landing Volume
# MAGIC
# MAGIC First task of the `garmin-daily` job. Logs in to Garmin with credentials from the
# MAGIC `garmin` secret scope and writes one JSON file per endpoint per day to the landing
# MAGIC Volume, where the pipeline's Auto Loader picks them up.
# MAGIC
# MAGIC | Secret | Used for |
# MAGIC |--------|----------|
# MAGIC | `tokens` | saved token JSON from `scripts/create_garmin_tokens.py` (preferred; works with MFA) |
# MAGIC | `email` + `password` | fallback, only if `tokens` is missing |
# MAGIC
# MAGIC Secret values are never printed; Databricks also redacts them in output.

# COMMAND ----------

# MAGIC %pip install garminconnect==0.3.20 --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC Set by the job (`base_parameters` in `databricks.yml`); the defaults work for interactive runs.

# COMMAND ----------

dbutils.widgets.text("src", "", "Bundle src/ path")
dbutils.widgets.text("landing", "/Volumes/workspace/garmin/landing", "Landing Volume")
dbutils.widgets.text("secret_scope", "garmin", "Secret scope")
dbutils.widgets.text("days", "7", "Days to pull")
dbutils.widgets.text("timezone", "America/Chicago", "Local timezone")

src = dbutils.widgets.get("src")
landing = dbutils.widgets.get("landing")
scope = dbutils.widgets.get("secret_scope")
days = int(dbutils.widgets.get("days"))
timezone = dbutils.widgets.get("timezone")

# COMMAND ----------

import logging
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

if src:
    sys.path.insert(0, src)

from garmin_lakehouse.ingest import date_range, ingest, login_with_password, login_with_tokens

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s", force=True)
log = logging.getLogger("ingest_garmin")


def read_secret(key: str) -> str | None:
    try:
        return dbutils.secrets.get(scope=scope, key=key)
    except Exception:  # a missing key raises; treat it as "not set"
        return None


# COMMAND ----------

# MAGIC %md
# MAGIC ## Log in (tokens first, password as fallback)

# COMMAND ----------

tokens = read_secret("tokens")
if tokens:
    log.info("logging in to Garmin with saved tokens")
    client = login_with_tokens(tokens)
else:
    email, password = read_secret("email"), read_secret("password")
    if not (email and password):
        raise RuntimeError(
            f"No Garmin credentials in secret scope '{scope}'. "
            "Add `tokens`, or `email` and `password` (see docs/setup.md)."
        )
    log.info("logging in to Garmin with email and password")
    client = login_with_password(email, password)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pull the last N days
# MAGIC Garmin dates are local calendar days, and serverless clocks run on UTC, so "today"
# MAGIC is computed in the configured timezone.

# COMMAND ----------

today = datetime.now(ZoneInfo(timezone)).date()
report = ingest(client, Path(landing), date_range(today, days), today=today)
log.info(
    "wrote %d files, skipped %d existing, %d empty, %d failed",
    len(report.written), report.skipped_existing, report.empty, len(report.failed),
)
for key, reason in report.failed.items():
    log.warning("failed %s: %s", key, reason)
# Partial failures (one endpoint, one day) don't block the pipeline refresh;
# a login failure has already raised above.

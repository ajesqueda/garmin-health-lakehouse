# Databricks notebook source
# MAGIC %md
# MAGIC # Garmin medallion pipeline
# MAGIC
# MAGIC Lakeflow declarative pipeline, incremental end to end through silver:
# MAGIC
# MAGIC | Step | Table | Type |
# MAGIC |------|-------|------|
# MAGIC | Landing JSON → **bronze** | `bronze_garmin_raw` | streaming table (Auto Loader), append-only, Change Data Feed on |
# MAGIC | bronze stream → parse | `<endpoint>_changes` | temporary views, parsing only |
# MAGIC | parse → **silver** | `silver_<endpoint>` | streaming tables via Auto CDC, **SCD type 2** (activities: SCD type 1) |
# MAGIC | current silver → **gold** | `gold_daily_recovery` | materialized view |
# MAGIC
# MAGIC Each run only reads landing files Auto Loader hasn't seen, and Auto CDC only
# MAGIC touches the keys those files change. Garmin's API has no change feed of its own,
# MAGIC so each re-fetch arrives as a new bronze row; SCD2 turns those into versioned
# MAGIC history, opening a new version only when a value actually changes.
# MAGIC
# MAGIC All parsing lives in `src/garmin_lakehouse/transforms.py`, which is unit-tested locally.

# COMMAND ----------

import sys

sys.path.append(spark.conf.get("bundle.sourcePath", "."))

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from garmin_lakehouse import transforms as T

LANDING = spark.conf.get("garmin.landing_path")
ENDPOINTS = "('daily_summary', 'sleep', 'hrv', 'training_readiness', 'activities')"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: every landing file, exactly as received

# COMMAND ----------


@dp.table(
    name="bronze_garmin_raw",
    comment="One row per landing file. Payload kept as raw JSON text. Append-only stream.",
    table_properties={
        "quality": "bronze",
        # Lets downstream consumers read row-level changes instead of rescanning.
        "delta.enableChangeDataFeed": "true",
    },
)
@dp.expect_or_drop("known_endpoint", f"endpoint IN {ENDPOINTS}")
@dp.expect_or_drop("has_calendar_date", "calendar_date IS NOT NULL")
def bronze_garmin_raw():
    raw = (
        spark.readStream.format("cloudFiles")  # Auto Loader
        .option("cloudFiles.format", "text")
        .option("wholetext", "true")  # one file = one JSON document = one row
        .option("pathGlobFilter", "*.json")  # never read ingestion temp files
        .load(LANDING)
        .select("value", F.col("_metadata.file_path").alias("source_file"))
    )
    return T.bronze_from_files(raw)


def _bronze_stream():
    return spark.readStream.table("bronze_garmin_raw")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: SCD type 2 via Auto CDC
# MAGIC
# MAGIC For each endpoint: a temporary view parses the bronze stream, a streaming table is
# MAGIC declared, and `create_auto_cdc_flow` upserts into it keyed on the business key and
# MAGIC ordered by `fetched_at`. A re-fetch with identical values doesn't open a new version.

# COMMAND ----------

# endpoint -> (parse function, sequence_by, SCD type, expectations)
SILVER = {
    "daily_summary": (
        lambda b: T.parse_daily(b, "daily_summary"),
        "fetched_at",
        2,
        {
            "plausible_resting_hr": "resting_hr IS NULL OR resting_hr BETWEEN 25 AND 120",
            "plausible_stress": "avg_stress IS NULL OR avg_stress BETWEEN 0 AND 100",
        },
    ),
    "sleep": (
        T.parse_sleep,
        "fetched_at",
        2,
        {
            "plausible_sleep_length": "sleep_hours IS NULL OR sleep_hours BETWEEN 0 AND 16",
            "sleep_score_range": "sleep_score IS NULL OR sleep_score BETWEEN 0 AND 100",
        },
    ),
    "hrv": (
        lambda b: T.parse_daily(b, "hrv"),
        "fetched_at",
        2,
        {"plausible_hrv": "hrv_last_night IS NULL OR hrv_last_night BETWEEN 5 AND 250"},
    ),
    # Several readiness measurements can share one fetch; the struct breaks the
    # tie so the day's latest measurement from the latest fetch wins.
    "training_readiness": (
        T.parse_training_readiness,
        F.struct("fetched_at", "measured_at"),
        2,
        {"readiness_range": "readiness_score IS NULL OR readiness_score BETWEEN 0 AND 100"},
    ),
    # A finished activity doesn't change, so history adds nothing: SCD type 1.
    "activities": (
        T.parse_activities,
        "fetched_at",
        1,
        {"has_activity_date": "activity_date IS NOT NULL"},
    ),
}


def _register_silver(endpoint, parse, sequence_by, scd_type, expectations):
    source = f"{endpoint}_changes"

    @dp.temporary_view(name=source)
    def changes():
        return parse(_bronze_stream())

    target = f"silver_{endpoint}"
    dp.create_streaming_table(
        name=target,
        comment=f"{endpoint}: SCD type {scd_type} on {T.KEYS[endpoint]}, built by Auto CDC.",
        table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"},
        expect_all=expectations,
    )
    dp.create_auto_cdc_flow(
        target=target,
        source=source,
        keys=T.KEYS[endpoint],
        sequence_by=sequence_by,
        stored_as_scd_type=scd_type,
        **({"track_history_except_column_list": ["fetched_at"]} if scd_type == 2 else {}),
    )


for _endpoint, _spec in SILVER.items():
    _register_silver(_endpoint, *_spec)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: one row per day of recovery features
# MAGIC
# MAGIC Reads only the *current* silver rows (open SCD2 versions). Rolling 7- and 28-day
# MAGIC windows span every day, so gold is a materialized view; at one row per day the
# MAGIC recompute stays tiny.

# COMMAND ----------


def _current(endpoint):
    """Current version of each key: open SCD2 rows, or the whole SCD1 table."""
    df = spark.read.table(f"silver_{endpoint}")
    if SILVER[endpoint][2] == 2:
        df = df.where("__END_AT IS NULL").drop("__START_AT", "__END_AT")
    return df


@dp.materialized_view(
    name="gold_daily_recovery",
    comment="Daily recovery features vs personal 7-day baselines; input to the recovery model.",
    table_properties={"quality": "gold"},
)
@dp.expect("has_sleep_or_hrv", "sleep_score IS NOT NULL OR hrv_last_night IS NOT NULL")
def gold_daily_recovery():
    return T.gold_daily_recovery(
        _current("daily_summary"),
        _current("sleep"),
        _current("hrv"),
        _current("training_readiness"),
        _current("activities"),
    )

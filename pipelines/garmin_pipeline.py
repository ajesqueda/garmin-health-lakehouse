# Lakeflow declarative pipeline: bronze -> silver -> gold for Garmin Connect data.
#
# This file only declares tables and data-quality expectations. All logic lives
# in src/garmin_lakehouse/transforms.py, which is unit-tested outside Databricks.
# `spark` is provided by the pipeline runtime.

import sys

sys.path.append(spark.conf.get("bundle.sourcePath", "."))  # noqa: F821

import dlt  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from garmin_lakehouse import transforms as T  # noqa: E402

LANDING = spark.conf.get("garmin.landing_path")  # noqa: F821
ENDPOINTS = "('daily_summary', 'sleep', 'hrv', 'training_readiness', 'activities')"


# --- Bronze: every landing file, exactly as received -------------------------


@dlt.table(
    name="bronze_garmin_raw",
    comment="One row per landing file. Payload kept as raw JSON text.",
    table_properties={"quality": "bronze"},
)
@dlt.expect_or_drop("known_endpoint", f"endpoint IN {ENDPOINTS}")
@dlt.expect_or_drop("has_calendar_date", "calendar_date IS NOT NULL")
def bronze_garmin_raw():
    raw = (
        spark.readStream.format("cloudFiles")  # noqa: F821  (Auto Loader)
        .option("cloudFiles.format", "text")
        .option("wholetext", "true")  # one file = one JSON document = one row
        .option("pathGlobFilter", "*.json")  # never read ingestion temp files
        .load(LANDING)
        .select("value", F.col("_metadata.file_path").alias("source_file"))
    )
    return T.bronze_from_files(raw)


def _bronze():
    return spark.read.table("bronze_garmin_raw")  # noqa: F821


# --- Silver: typed, one row per day (latest fetch wins) ----------------------


@dlt.table(name="silver_daily_summary", table_properties={"quality": "silver"})
@dlt.expect("plausible_resting_hr", "resting_hr IS NULL OR resting_hr BETWEEN 25 AND 120")
@dlt.expect("plausible_stress", "avg_stress IS NULL OR avg_stress BETWEEN 0 AND 100")
def silver_daily_summary():
    return T.silver_daily_summary(_bronze())


@dlt.table(name="silver_sleep", table_properties={"quality": "silver"})
@dlt.expect("plausible_sleep_length", "sleep_hours IS NULL OR sleep_hours BETWEEN 0 AND 16")
@dlt.expect("sleep_score_range", "sleep_score IS NULL OR sleep_score BETWEEN 0 AND 100")
def silver_sleep():
    return T.silver_sleep(_bronze())


@dlt.table(name="silver_hrv", table_properties={"quality": "silver"})
@dlt.expect("plausible_hrv", "hrv_last_night IS NULL OR hrv_last_night BETWEEN 5 AND 250")
def silver_hrv():
    return T.silver_hrv(_bronze())


@dlt.table(name="silver_training_readiness", table_properties={"quality": "silver"})
@dlt.expect("readiness_range", "readiness_score IS NULL OR readiness_score BETWEEN 0 AND 100")
def silver_training_readiness():
    return T.silver_training_readiness(_bronze())


@dlt.table(name="silver_activities", table_properties={"quality": "silver"})
@dlt.expect_or_drop("has_activity_date", "activity_date IS NOT NULL")
def silver_activities():
    return T.silver_activities(_bronze())


# --- Gold: one row per day of recovery features ------------------------------


@dlt.table(
    name="gold_daily_recovery",
    comment="Daily recovery features vs personal 7-day baselines; input to the future model.",
    table_properties={"quality": "gold"},
)
@dlt.expect("has_sleep_or_hrv", "sleep_score IS NOT NULL OR hrv_last_night IS NOT NULL")
def gold_daily_recovery():
    read = spark.read.table  # noqa: F821
    return T.gold_daily_recovery(
        read("silver_daily_summary"),
        read("silver_sleep"),
        read("silver_hrv"),
        read("silver_training_readiness"),
        read("silver_activities"),
    )

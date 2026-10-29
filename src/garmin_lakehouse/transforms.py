"""Bronze -> silver -> gold transformations as plain PySpark functions.

They take and return DataFrames and know nothing about Lakeflow, so the same
code runs in the Databricks pipeline (pipelines/garmin_pipeline.py) and in
local pytest with a small Spark session.

    bronze  one row per landing file, payload kept as a raw JSON string
    silver  one typed table per endpoint, one row per day (latest fetch wins)
    gold    one row per day of recovery features, ready to feed a model
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

# --------------------------------------------------------------------------
# Bronze
# --------------------------------------------------------------------------


def bronze_from_files(raw: DataFrame) -> DataFrame:
    """Parse the ingestion envelope. Expects columns `value` (file text) and `source_file`.

    Only the envelope fields are parsed. `payload` stays a JSON string on
    purpose: if Garmin renames a field, bronze still loads and silver is the
    only place that needs fixing.
    """
    return raw.select(
        F.get_json_object("value", "$.endpoint").alias("endpoint"),
        F.to_date(F.get_json_object("value", "$.calendar_date")).alias("calendar_date"),
        F.to_timestamp(F.get_json_object("value", "$.fetched_at")).alias("fetched_at"),
        # For an object this returns its JSON text; for a JSON null, a SQL NULL.
        F.get_json_object("value", "$.payload").alias("payload"),
        F.col("source_file"),
        F.current_timestamp().alias("ingested_at"),
    )


# --------------------------------------------------------------------------
# Silver
# --------------------------------------------------------------------------

# Where each silver column lives inside Garmin's JSON: column -> (JSON path, type).
# Garmin's API is unofficial and undocumented, so these paths are the one place
# to adjust after inspecting real payloads (see docs/setup.md, "Verify fields").
FIELDS: dict[str, dict[str, tuple[str, str]]] = {
    "daily_summary": {
        "total_steps": ("$.totalSteps", "int"),
        "resting_hr": ("$.restingHeartRate", "int"),
        "min_hr": ("$.minHeartRate", "int"),
        "max_hr": ("$.maxHeartRate", "int"),
        "avg_stress": ("$.averageStressLevel", "int"),
        "max_stress": ("$.maxStressLevel", "int"),
        "body_battery_high": ("$.bodyBatteryHighestValue", "int"),
        "body_battery_low": ("$.bodyBatteryLowestValue", "int"),
        "body_battery_charged": ("$.bodyBatteryChargedValue", "int"),
        "body_battery_drained": ("$.bodyBatteryDrainedValue", "int"),
        "active_kcal": ("$.activeKilocalories", "double"),
        "moderate_intensity_min": ("$.moderateIntensityMinutes", "int"),
        "vigorous_intensity_min": ("$.vigorousIntensityMinutes", "int"),
    },
    "sleep": {
        "sleep_seconds": ("$.dailySleepDTO.sleepTimeSeconds", "int"),
        "deep_sleep_seconds": ("$.dailySleepDTO.deepSleepSeconds", "int"),
        "light_sleep_seconds": ("$.dailySleepDTO.lightSleepSeconds", "int"),
        "rem_sleep_seconds": ("$.dailySleepDTO.remSleepSeconds", "int"),
        "awake_seconds": ("$.dailySleepDTO.awakeSleepSeconds", "int"),
        "sleep_score": ("$.dailySleepDTO.sleepScores.overall.value", "int"),
        "avg_sleep_stress": ("$.dailySleepDTO.avgSleepStress", "double"),
        "avg_respiration": ("$.dailySleepDTO.averageRespirationValue", "double"),
    },
    "hrv": {
        "hrv_last_night": ("$.hrvSummary.lastNightAvg", "int"),
        "hrv_weekly_avg": ("$.hrvSummary.weeklyAvg", "int"),
        "hrv_status": ("$.hrvSummary.status", "string"),
        "hrv_balanced_low": ("$.hrvSummary.baseline.balancedLow", "int"),
        "hrv_balanced_high": ("$.hrvSummary.baseline.balancedUpper", "int"),
    },
    # Readiness and activities are JSON arrays; their paths apply to each element.
    "training_readiness": {
        "readiness_score": ("$.score", "int"),
        "readiness_level": ("$.level", "string"),
        "recovery_time_min": ("$.recoveryTime", "int"),
        "measured_at": ("$.timestamp", "timestamp"),
    },
    "activities": {
        "activity_id": ("$.activityId", "bigint"),
        "activity_name": ("$.activityName", "string"),
        "activity_type": ("$.activityType.typeKey", "string"),
        "start_time_local": ("$.startTimeLocal", "timestamp"),
        "duration_seconds": ("$.duration", "double"),
        "distance_m": ("$.distance", "double"),
        "avg_hr": ("$.averageHR", "double"),
        "max_hr": ("$.maxHR", "double"),
        "training_load": ("$.activityTrainingLoad", "double"),
        "aerobic_effect": ("$.aerobicTrainingEffect", "double"),
        "anaerobic_effect": ("$.anaerobicTrainingEffect", "double"),
    },
}


def _extract(json_col: Column, endpoint: str) -> list[Column]:
    return [
        F.get_json_object(json_col, path).cast(dtype).alias(name)
        for name, (path, dtype) in FIELDS[endpoint].items()
    ]


def _latest(df: DataFrame, keys: list[str], order: list[Column]) -> DataFrame:
    """Keep one row per key: the first by `order` (newest first)."""
    w = Window.partitionBy(*keys).orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def _endpoint(bronze: DataFrame, endpoint: str) -> DataFrame:
    # Null payloads are "asked, but no data that day" markers from ingestion.
    return bronze.filter((F.col("endpoint") == endpoint) & F.col("payload").isNotNull())


# Silver is built in two steps:
#
#   parse_*   typed columns, one row per bronze row/element. No windows or
#             aggregations, so these work on a *stream* of bronze rows. The
#             pipeline feeds them to Auto CDC, which does the deduplication
#             incrementally and keeps SCD type 2 history.
#   silver_*  parse_* plus "latest version per key", as one batch query. Used
#             by tests and local runs; gives the same result as the current
#             rows of the pipeline's SCD2 tables.

# Business keys and the column that orders versions of the same key.
KEYS = {
    "daily_summary": ["calendar_date"],
    "sleep": ["calendar_date"],
    "hrv": ["calendar_date"],
    "training_readiness": ["calendar_date"],
    "activities": ["activity_id"],
}


def parse_daily(bronze: DataFrame, endpoint: str) -> DataFrame:
    """Object-per-day endpoints: typed columns, every fetch kept."""
    return _endpoint(bronze, endpoint).select(
        "calendar_date", "fetched_at", *_extract(F.col("payload"), endpoint)
    )


def parse_sleep(bronze: DataFrame) -> DataFrame:
    df = parse_daily(bronze, "sleep")
    total = F.col("sleep_seconds")
    return df.select(
        "*",
        F.round(total / 3600, 2).alias("sleep_hours"),
        F.round(F.col("deep_sleep_seconds") / total * 100, 1).alias("deep_sleep_pct"),
        F.round(F.col("rem_sleep_seconds") / total * 100, 1).alias("rem_sleep_pct"),
    )


def _explode(bronze: DataFrame, endpoint: str) -> DataFrame:
    # Parse the array but keep each element as JSON text, then pull fields
    # from each element with the same path mechanism as the daily endpoints.
    elements = F.explode(F.from_json("payload", ArrayType(StringType())))
    return _endpoint(bronze, endpoint).select(
        "calendar_date", "fetched_at", elements.alias("element")
    )


def parse_training_readiness(bronze: DataFrame) -> DataFrame:
    """One row per readiness measurement. Garmin re-scores during the day."""
    return _explode(bronze, "training_readiness").select(
        "calendar_date", "fetched_at", *_extract(F.col("element"), "training_readiness")
    )


def parse_activities(bronze: DataFrame) -> DataFrame:
    """One row per activity per fetch."""
    rows = _explode(bronze, "activities").select(
        "fetched_at", *_extract(F.col("element"), "activities")
    )
    return rows.filter(F.col("activity_id").isNotNull()).withColumn(
        "activity_date", F.to_date("start_time_local")
    )


def silver_daily(bronze: DataFrame, endpoint: str) -> DataFrame:
    """Latest fetch per day for an object-per-day endpoint."""
    return _latest(parse_daily(bronze, endpoint), ["calendar_date"], [F.col("fetched_at").desc()])


def silver_daily_summary(bronze: DataFrame) -> DataFrame:
    return silver_daily(bronze, "daily_summary")


def silver_sleep(bronze: DataFrame) -> DataFrame:
    return _latest(parse_sleep(bronze), ["calendar_date"], [F.col("fetched_at").desc()])


def silver_hrv(bronze: DataFrame) -> DataFrame:
    return silver_daily(bronze, "hrv")


def silver_training_readiness(bronze: DataFrame) -> DataFrame:
    """Latest readiness score per day (the pipeline orders by the same pair)."""
    return _latest(
        parse_training_readiness(bronze),
        ["calendar_date"],
        [F.col("fetched_at").desc(), F.col("measured_at").desc_nulls_last()],
    )


def silver_activities(bronze: DataFrame) -> DataFrame:
    """One row per activity. A refetch of the same day must not duplicate it."""
    return _latest(parse_activities(bronze), ["activity_id"], [F.col("fetched_at").desc()])


# --------------------------------------------------------------------------
# Gold
# --------------------------------------------------------------------------


def _days(col: str = "calendar_date") -> Column:
    # Whole days since 1970 so window frames can be expressed in days.
    return F.datediff(F.col(col), F.lit("1970-01-01"))


def gold_daily_recovery(
    daily: DataFrame,
    sleep: DataFrame,
    hrv: DataFrame,
    readiness: DataFrame,
    activities: DataFrame,
) -> DataFrame:
    """One row per day: today's signals next to the athlete's own recent baseline.

    Baselines use the *previous* 7 days (not today), so "HRV is 15% below
    baseline" compares last night with the week before it. Training load uses
    the acute:chronic ratio: last 7 days of load vs the average week of the
    last 28. Above ~1.3 is commonly read as ramping up faster than you've
    adapted to.
    """
    load = activities.groupBy(F.col("activity_date").alias("calendar_date")).agg(
        F.round(F.sum("training_load"), 1).alias("training_load"),
        F.round(F.sum("duration_seconds") / 60, 1).alias("activity_minutes"),
        F.count("*").alias("activity_count"),
    )

    spine = (
        daily.select("calendar_date")
        .union(sleep.select("calendar_date"))
        .union(hrv.select("calendar_date"))
        .union(load.select("calendar_date"))
        .distinct()
    )

    df = (
        spine.join(
            daily.select(
                "calendar_date", "resting_hr", "avg_stress", "total_steps",
                "body_battery_high", "body_battery_low", "body_battery_charged",
            ),
            "calendar_date", "left",
        )
        .join(
            sleep.select(
                "calendar_date", "sleep_score", "sleep_hours", "deep_sleep_pct",
                "rem_sleep_pct", "avg_sleep_stress",
            ),
            "calendar_date", "left",
        )
        .join(
            hrv.select("calendar_date", "hrv_last_night", "hrv_weekly_avg", "hrv_status"),
            "calendar_date", "left",
        )
        .join(
            readiness.select("calendar_date", "readiness_score", "readiness_level"),
            "calendar_date", "left",
        )
        .join(load, "calendar_date", "left")
        .fillna(0, subset=["training_load", "activity_minutes", "activity_count"])
        .withColumn("_day", _days())
    )

    # A single person's history is small, so one ordered window is fine.
    ordered = Window.orderBy("_day")
    prev_7 = ordered.rangeBetween(-7, -1)
    last_7 = ordered.rangeBetween(-6, 0)
    last_28 = ordered.rangeBetween(-27, 0)

    hrv_base = F.avg("hrv_last_night").over(prev_7)
    rhr_base = F.avg("resting_hr").over(prev_7)
    acute = F.sum("training_load").over(last_7)
    chronic_weekly = F.sum("training_load").over(last_28) / 4

    return (
        df.withColumn("hrv_baseline_7d", F.round(hrv_base, 1))
        .withColumn(
            "hrv_vs_baseline_pct",
            F.round((F.col("hrv_last_night") / hrv_base - 1) * 100, 1),
        )
        .withColumn("resting_hr_baseline_7d", F.round(rhr_base, 1))
        .withColumn("resting_hr_delta", F.round(F.col("resting_hr") - rhr_base, 1))
        .withColumn("sleep_score_avg_7d", F.round(F.avg("sleep_score").over(last_7), 1))
        .withColumn("acute_load_7d", F.round(acute, 1))
        .withColumn("chronic_load_weekly_28d", F.round(chronic_weekly, 1))
        .withColumn(
            "acute_chronic_ratio",
            F.when(chronic_weekly > 0, F.round(acute / chronic_weekly, 2)),
        )
        # How much history backs the baselines; a model should trust a 1-day
        # baseline much less than a 7-day one.
        .withColumn("baseline_days", F.count("hrv_last_night").over(prev_7))
        .drop("_day")
        .orderBy("calendar_date")
    )

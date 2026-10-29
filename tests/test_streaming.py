"""The pipeline runs parse_* on a *stream* of bronze rows. Spark refuses some
operations on streams (row_number windows, for example), so run each parse
function on a real local stream to prove it's stream-safe."""

import pytest
from conftest import envelope
from pyspark.sql import functions as F

from garmin_lakehouse import transforms as T

PARSERS = {
    "daily_summary": lambda b: T.parse_daily(b, "daily_summary"),
    "sleep": T.parse_sleep,
    "hrv": lambda b: T.parse_daily(b, "hrv"),
    "training_readiness": T.parse_training_readiness,
    "activities": T.parse_activities,
}

FILES = {
    "daily_summary/2026-10-01.json": envelope(
        "daily_summary", "2026-10-01", {"restingHeartRate": 50, "totalSteps": 9000}
    ),
    "sleep/2026-10-01.json": envelope(
        "sleep", "2026-10-01",
        {"dailySleepDTO": {"sleepTimeSeconds": 28800, "deepSleepSeconds": 5760,
                           "remSleepSeconds": 7200}},
    ),
    "hrv/2026-10-01.json": envelope("hrv", "2026-10-01", {"hrvSummary": {"lastNightAvg": 55}}),
    "hrv/2026-10-02.json": envelope("hrv", "2026-10-02", None),  # no-data marker
    "training_readiness/2026-10-01.json": envelope(
        "training_readiness", "2026-10-01",
        [{"score": 40, "timestamp": "2026-10-01T06:00:00"},
         {"score": 65, "timestamp": "2026-10-01T14:00:00"}],
    ),
    "activities/2026-10-01.json": envelope(
        "activities", "2026-10-01",
        [{"activityId": 7, "startTimeLocal": "2026-10-01 06:30:00", "activityTrainingLoad": 80}],
    ),
}


@pytest.fixture(scope="module")
def bronze_stream(spark, tmp_path_factory):
    landing = tmp_path_factory.mktemp("landing")
    for rel, text in FILES.items():
        path = landing / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    raw = (
        spark.readStream.format("text")
        .option("wholetext", "true")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.json")
        .load(str(landing))
        .select("value", F.col("_metadata.file_path").alias("source_file"))
    )
    return T.bronze_from_files(raw)


def run_stream(spark, df, name):
    query = df.writeStream.format("memory").queryName(name).trigger(availableNow=True).start()
    query.awaitTermination(60)
    rows = spark.sql(f"SELECT * FROM {name}").collect()
    query.stop()
    return rows


@pytest.mark.parametrize("endpoint", list(PARSERS))
def test_parse_runs_on_a_stream(spark, bronze_stream, endpoint):
    assert bronze_stream.isStreaming
    rows = run_stream(spark, PARSERS[endpoint](bronze_stream), f"parsed_{endpoint}")
    assert rows, f"{endpoint} produced no rows"
    for key in T.KEYS[endpoint]:
        assert all(getattr(r, key) is not None for r in rows)


def test_stream_skips_no_data_markers(spark, bronze_stream):
    rows = run_stream(spark, PARSERS["hrv"](bronze_stream), "hrv_markers")
    assert [str(r.calendar_date) for r in rows] == ["2026-10-01"]


def test_readiness_keeps_every_measurement_for_auto_cdc_to_order(spark, bronze_stream):
    rows = run_stream(spark, PARSERS["training_readiness"](bronze_stream), "readiness_all")
    # Both re-scores reach silver; Auto CDC's sequence_by picks the latest.
    assert sorted(r.readiness_score for r in rows) == [40, 65]


def test_batch_silver_is_not_stream_safe(bronze_stream):
    # The batch helpers dedupe with row_number(), which streams don't support.
    # That's why the pipeline uses parse_* + Auto CDC instead.
    from pyspark.errors import AnalysisException

    with pytest.raises(AnalysisException):
        T.silver_hrv(bronze_stream).writeStream.format("memory").queryName("bad").start()

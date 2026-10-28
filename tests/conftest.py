import json
import os
import sys

import pytest

# The Spark worker must use the same Python as the test process, and binding
# to localhost avoids hostname lookups failing on laptops and CI runners.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("garmin-lakehouse-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def envelope(endpoint: str, day: str, payload, fetched_at: str = "2026-10-27T06:00:00+00:00"):
    """A landing file's text, in the exact format ingest.py writes."""
    return json.dumps(
        {"endpoint": endpoint, "calendar_date": day, "fetched_at": fetched_at, "payload": payload}
    )


@pytest.fixture
def make_bronze(spark):
    from garmin_lakehouse.transforms import bronze_from_files

    def build(files: list[str]):
        raw = spark.createDataFrame(
            [(text, f"/landing/file_{i}.json") for i, text in enumerate(files)],
            ["value", "source_file"],
        )
        return bronze_from_files(raw)

    return build

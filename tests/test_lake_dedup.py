"""Test lake de-duplication without Kafka or PostgreSQL.

Run inside the Spark container from the project root:
    spark-submit tests/test_lake_dedup.py
"""

import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, "src/streaming")

from pyspark.sql import SparkSession  # noqa: E402

import spark_stream_processor as processor  # noqa: E402


def make_events(spark, event_ids):
    """Build a small DataFrame of events with the given IDs."""

    return spark.createDataFrame(
        [(event_id, float(index)) for index, event_id in enumerate(event_ids)],
        ["event_id", "value"],
    )


def main():
    """Run the de-duplication checks."""

    spark = (
        SparkSession.builder
        .appName("TestLakeDedup")
        .master("local[1]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    temp_dir = tempfile.mkdtemp()
    processor.VALID_EVENTS_PATH = str(Path(temp_dir) / "sensor_events")

    try:
        # Batch 1: "b" appears twice inside the same batch.
        batch_1 = processor.drop_already_stored_events(
            spark, make_events(spark, ["a", "b", "b", "c"])
        )
        assert batch_1.count() == 3, "within-batch duplicate not removed"
        batch_1.write.mode("append").parquet(processor.VALID_EVENTS_PATH)

        # Batch 2: "b" and "c" were stored by batch 1; "d" is new
        # and appears twice.
        batch_2 = processor.drop_already_stored_events(
            spark, make_events(spark, ["b", "c", "d", "d"])
        )
        assert batch_2.count() == 1, "already-stored events not dropped"
        batch_2.write.mode("append").parquet(processor.VALID_EVENTS_PATH)

        # Batch 3: a full replay of everything adds nothing.
        batch_3 = processor.drop_already_stored_events(
            spark, make_events(spark, ["a", "b", "c", "d"])
        )
        assert batch_3.count() == 0, "full replay was not ignored"

        stored = spark.read.parquet(processor.VALID_EVENTS_PATH)
        total = stored.count()
        distinct = stored.select("event_id").distinct().count()
        assert total == distinct == 4, f"expected 4 unique, got {total}/{distinct}"

        print("\nPASS: lake de-duplication works (4 unique events stored).")

    finally:
        spark.stop()
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
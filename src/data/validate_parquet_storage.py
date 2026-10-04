"""Validate the historical Parquet sensor-event storage."""

from pyspark.sql import SparkSession


PARQUET_PATH = "data/lake/sensor_events"


def main():
    """Validate stored sensor events."""

    spark = (
        SparkSession.builder
        .appName("ValidateParquetStorage")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        events = spark.read.parquet(PARQUET_PATH)

        total_rows = events.count()
        distinct_event_ids = events.select("event_id").distinct().count()
        duplicate_rows = total_rows - distinct_event_ids

        print("\nParquet row count:")
        print(total_rows)

        print("\nDistinct event_id count:")
        print(distinct_event_ids)

        print("\nParquet schema:")
        events.printSchema()

        print("\nSample records:")
        events.orderBy("event_time", "sensor_type", "sensor_id").show(
            5,
            truncate=False,
        )

        if duplicate_rows > 0:
            raise SystemExit(
                f"VALIDATION FAILED: {duplicate_rows} duplicate "
                "event_id rows found in the Parquet lake."
            )

        print("\nVALIDATION PASSED: every event_id is unique.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
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

        print("\nParquet row count:")
        print(events.count())

        print("\nParquet schema:")
        events.printSchema()

        print("\nSample records:")
        events.orderBy("event_time", "sensor_type", "sensor_id").show(
            5,
            truncate=False,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
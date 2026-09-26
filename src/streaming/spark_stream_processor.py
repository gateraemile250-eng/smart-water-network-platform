"""Process water sensor events from Kafka using Spark Structured Streaming."""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    from_json,
    lit,
    max as spark_max,
    min as spark_min,
    try_to_timestamp,
    when,
    window,
)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SENSOR_TOPIC = os.getenv("SENSOR_TOPIC", "water-sensor-events")

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

WINDOW_DURATION = "15 minutes"
WATERMARK_DELAY = "10 minutes"

VALID_EVENTS_PATH = "data/lake/sensor_events"
REJECTED_EVENTS_PATH = "data/lake/rejected_sensor_events"

EVENT_CHECKPOINT_PATH = "data/checkpoints/events_archive"
METRICS_CHECKPOINT_PATH = "data/checkpoints/metrics"

SENSOR_UNITS = {
    "pressure": "m",
    "flow": "m3/h",
    "level": "m",
    "demand": "L/h",
}

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("sensor_id", StringType(), True),
        StructField("sensor_type", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("unit", StringType(), True),
    ]
)


def create_spark_session():
    """Create the Spark session."""

    spark = (
        SparkSession.builder
        .appName("SmartWaterStreamProcessor")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_stream(spark):
    """Read sensor events from Kafka."""

    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", SENSOR_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )


def parse_sensor_events(stream):
    """Parse Kafka JSON messages and retain Kafka metadata."""

    parsed = stream.select(
        from_json(
            col("value").cast("string"),
            EVENT_SCHEMA,
        ).alias("event"),
        col("key").cast("string").alias("message_key"),
        col("partition"),
        col("offset"),
    )

    return parsed.select(
        "event.*",
        "message_key",
        "partition",
        "offset",
    )


def validate_sensor_events(stream):
    """Parse event time and add structural validation results."""

    stream = (
        stream
        .withColumn("raw_event_time", col("event_time"))
        .withColumn(
            "event_time",
            try_to_timestamp(
                col("event_time"),
                lit("yyyy-MM-dd'T'HH:mm:ss"),
            ),
        )
    )

    expected_unit = (
        when(col("sensor_type") == "pressure", "m")
        .when(col("sensor_type") == "flow", "m3/h")
        .when(col("sensor_type") == "level", "m")
        .when(col("sensor_type") == "demand", "L/h")
    )

    rejection_reason = (
        when(
            col("event_id").isNull() | (col("event_id") == ""),
            "missing_event_id",
        )
        .when(col("event_time").isNull(), "invalid_event_time")
        .when(
            col("sensor_id").isNull() | (col("sensor_id") == ""),
            "missing_sensor_id",
        )
        .when(
            col("sensor_type").isNull()
            | ~col("sensor_type").isin(list(SENSOR_UNITS)),
            "invalid_sensor_type",
        )
        .when(col("value").isNull(), "missing_value")
        .when(
            col("unit").isNull() | (col("unit") != expected_unit),
            "invalid_unit",
        )
    )

    return (
        stream
        .withColumn("rejection_reason", rejection_reason)
        .withColumn(
            "is_valid",
            col("rejection_reason").isNull(),
        )
    )


def select_valid_events(stream):
    """Return valid sensor events."""

    return stream.filter(col("is_valid"))


def calculate_window_metrics(stream):
    """Calculate 15-minute sensor metrics."""

    return (
        stream
        .withWatermark("event_time", WATERMARK_DELAY)
        .groupBy(
            window(col("event_time"), WINDOW_DURATION),
            "sensor_id",
            "sensor_type",
            "unit",
        )
        .agg(
            count("*").alias("reading_count"),
            avg("value").alias("avg_value"),
            spark_min("value").alias("min_value"),
            spark_max("value").alias("max_value"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "sensor_id",
            "sensor_type",
            "unit",
            "reading_count",
            "avg_value",
            "min_value",
            "max_value",
        )
    )


def write_event_batch(batch_df, batch_id):
    """Store valid and rejected events as Parquet."""

    valid = (
        batch_df
        .filter(col("is_valid"))
        .select(
            "event_id",
            "event_time",
            "sensor_id",
            "sensor_type",
            "value",
            "unit",
            "message_key",
            "partition",
            "offset",
        )
    )

    rejected = (
        batch_df
        .filter(~col("is_valid"))
        .select(
            "event_id",
            "raw_event_time",
            "sensor_id",
            "sensor_type",
            "value",
            "unit",
            "message_key",
            "partition",
            "offset",
            "rejection_reason",
        )
    )

    if not valid.isEmpty():
        valid.write.mode("append").parquet(VALID_EVENTS_PATH)

    if not rejected.isEmpty():
        rejected.write.mode("append").parquet(REJECTED_EVENTS_PATH)

    print(f"Parquet event batch {batch_id} persisted.")


def validate_postgres_config():
    """Verify required PostgreSQL settings."""

    settings = {
        "POSTGRES_HOST": POSTGRES_HOST,
        "POSTGRES_PORT": POSTGRES_PORT,
        "POSTGRES_DB": POSTGRES_DB,
        "POSTGRES_USER": POSTGRES_USER,
        "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
    }

    missing = [name for name, value in settings.items() if not value]

    if missing:
        raise EnvironmentError(
            "Missing PostgreSQL settings: " + ", ".join(missing)
        )


def write_metrics_batch(batch_df, batch_id):
    """Write one metrics micro-batch to PostgreSQL staging."""

    if batch_df.isEmpty():
        return

    jdbc_url = (
        f"jdbc:postgresql://{POSTGRES_HOST}:"
        f"{POSTGRES_PORT}/{POSTGRES_DB}"
    )

    (
        batch_df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "sensor_metrics_15min_staging")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )

    print(f"PostgreSQL staging batch {batch_id} persisted.")


def main():
    """Run the streaming pipeline."""

    validate_postgres_config()
    spark = create_spark_session()

    try:
        events = read_kafka_stream(spark)
        events = parse_sensor_events(events)
        events = validate_sensor_events(events)

        valid_events = select_valid_events(events)
        metrics = calculate_window_metrics(valid_events)

        event_query = (
            events.writeStream
            .foreachBatch(write_event_batch)
            .option(
                "checkpointLocation",
                EVENT_CHECKPOINT_PATH,
            )
            .trigger(availableNow=True)
            .start()
        )

        metrics_query = (
            metrics.writeStream
            .foreachBatch(write_metrics_batch)
            .outputMode("update")
            .option(
                "checkpointLocation",
                METRICS_CHECKPOINT_PATH,
            )
            .trigger(availableNow=True)
            .start()
        )

        event_query.awaitTermination()
        metrics_query.awaitTermination()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
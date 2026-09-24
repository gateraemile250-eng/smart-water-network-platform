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
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)
SENSOR_TOPIC = os.getenv(
    "SENSOR_TOPIC",
    "water-sensor-events",
)

WINDOW_DURATION = "15 minutes"
WATERMARK_DELAY = "10 minutes"

SENSOR_UNITS = {
    "pressure": "m",
    "flow": "m3/h",
    "level": "m",
    "demand": "L/h",
}


SENSOR_EVENT_SCHEMA = StructType(
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
    """Create the Spark session used by the streaming processor."""

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
    """Read raw sensor messages from the Kafka topic."""

    return (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS,
        )
        .option("subscribe", SENSOR_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )


def select_kafka_message_fields(kafka_stream):
    """Select Kafka fields required for processing and traceability."""

    return kafka_stream.select(
        col("key").cast("string").alias("message_key"),
        col("value").cast("string").alias("message_value"),
        col("partition"),
        col("offset"),
    )


def parse_sensor_events(message_stream):
    """Parse Kafka JSON values using the sensor-event schema."""

    parsed_stream = message_stream.withColumn(
        "event",
        from_json(
            col("message_value"),
            SENSOR_EVENT_SCHEMA,
        ),
    )

    return parsed_stream.select(
        col("event.event_id").alias("event_id"),
        col("event.event_time").alias("event_time"),
        col("event.sensor_id").alias("sensor_id"),
        col("event.sensor_type").alias("sensor_type"),
        col("event.value").alias("value"),
        col("event.unit").alias("unit"),
        col("message_key"),
        col("partition"),
        col("offset"),
    )


def convert_event_time(parsed_stream):
    """Safely convert event time from text to Spark timestamp."""

    return parsed_stream.withColumn(
        "event_time",
        try_to_timestamp(
            col("event_time"),
            lit("yyyy-MM-dd'T'HH:mm:ss"),
        ),
    )


def validate_sensor_events(timestamped_stream):
    """Add structural validity status to each sensor event."""

    valid_sensor_types = list(SENSOR_UNITS)

    expected_unit = (
        when(
            col("sensor_type") == "pressure",
            lit(SENSOR_UNITS["pressure"]),
        )
        .when(
            col("sensor_type") == "flow",
            lit(SENSOR_UNITS["flow"]),
        )
        .when(
            col("sensor_type") == "level",
            lit(SENSOR_UNITS["level"]),
        )
        .when(
            col("sensor_type") == "demand",
            lit(SENSOR_UNITS["demand"]),
        )
    )

    is_valid = (
        col("event_id").isNotNull()
        & (col("event_id") != "")
        & col("event_time").isNotNull()
        & col("sensor_id").isNotNull()
        & (col("sensor_id") != "")
        & col("sensor_type").isin(valid_sensor_types)
        & col("value").isNotNull()
        & (col("unit") == expected_unit)
    )

    return timestamped_stream.withColumn(
        "is_valid",
        is_valid,
    )


def select_valid_events(validated_stream):
    """Return events that pass structural validation."""

    return validated_stream.filter(col("is_valid"))


def select_invalid_events(validated_stream):
    """Return events that fail structural validation."""

    return validated_stream.filter(
        ~col("is_valid") | col("is_valid").isNull()
    )


def calculate_window_metrics(valid_stream):
    """Calculate event-time sensor metrics using fixed windows."""

    return (
        valid_stream
        .withWatermark(
            "event_time",
            WATERMARK_DELAY,
        )
        .groupBy(
            window(
                col("event_time"),
                WINDOW_DURATION,
            ),
            col("sensor_type"),
            col("sensor_id"),
            col("unit"),
        )
        .agg(
            count("*").alias("reading_count"),
            avg("value").alias("avg_value"),
            spark_min("value").alias("min_value"),
            spark_max("value").alias("max_value"),
        )
    )


def start_metrics_console_stream(metrics_stream):
    """Process available events and display windowed metrics."""

    return (
        metrics_stream.writeStream
        .format("console")
        .outputMode("update")
        .option("truncate", False)
        .option("numRows", 50)
        .trigger(availableNow=True)
        .start()
    )


def main():
    """Run the Kafka-to-Spark streaming processing pipeline."""

    spark = create_spark_session()

    try:
        kafka_stream = read_kafka_stream(spark)
        message_stream = select_kafka_message_fields(kafka_stream)
        parsed_stream = parse_sensor_events(message_stream)
        timestamped_stream = convert_event_time(parsed_stream)
        validated_stream = validate_sensor_events(timestamped_stream)
        valid_stream = select_valid_events(validated_stream)

        window_metrics_stream = calculate_window_metrics(
            valid_stream
        )

        query = start_metrics_console_stream(
            window_metrics_stream
        )

        query.awaitTermination()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
"""Data-quality checks for the Parquet sensor-event lake.

The rules and the evaluation logic are plain Python so they can be
unit-tested without Spark. Spark is imported only when the checks are
run against the lake.

Run inside the Spark container from the project root:
    spark-submit src/data/quality_checks.py [parquet_path]

Exit code 1 means at least one ERROR check failed. WARNING checks are
reported but never fail the run.
"""

import sys

ERROR = "ERROR"
WARNING = "WARNING"

DEFAULT_PARQUET_PATH = "data/lake/sensor_events"

REQUIRED_COLUMNS = [
    "event_id",
    "event_time",
    "sensor_id",
    "sensor_type",
    "value",
    "unit",
]

# Must match SENSOR_UNITS in src/simulator/sensor_simulator.py
# (tests/test_quality_checks.py fails if the two ever drift apart).
EXPECTED_UNITS = {
    "pressure": "m",
    "flow": "m3/h",
    "level": "m",
    "demand": "L/h",
}

# Generous physical upper bounds. The 2018 BattLeDIM data peaks at
# pressure 56.97 m, flow 195.28 m3/h, level 3.90 m and demand
# 37,795.88 L/h, so these only catch clearly implausible readings.
PLAUSIBLE_MAX = {
    "pressure": 100.0,
    "flow": 500.0,
    "level": 10.0,
    "demand": 100000.0,
}

# 33 pressure + 3 flow + 1 level + 82 demand sensors report at every
# 5-minute timestamp (see TOTAL_SENSORS in baseline_detector.py).
EXPECTED_EVENTS_PER_TIMESTAMP = 119


def evaluate_quality(metrics):
    """Turn lake metrics into a list of check results.

    Each result is a dict with name, severity, passed and detail.
    """

    results = []

    def record(name, passed, detail, severity=ERROR):
        results.append(
            {
                "name": name,
                "severity": severity,
                "passed": bool(passed),
                "detail": detail,
            }
        )

    total_rows = metrics["total_rows"]
    record("rows_present", total_rows > 0, f"{total_rows} rows")

    duplicate_rows = total_rows - metrics["distinct_event_ids"]
    record(
        "event_id_unique",
        duplicate_rows == 0,
        f"{duplicate_rows} duplicate rows",
    )

    null_columns = {
        column: count
        for column, count in metrics["null_counts"].items()
        if count
    }
    record(
        "required_columns_not_null",
        not null_columns,
        f"nulls found: {null_columns}" if null_columns else "no nulls",
    )

    non_finite = metrics["non_finite_values"]
    record(
        "values_finite",
        non_finite == 0,
        f"{non_finite} NaN or infinite values",
    )

    type_unit_counts = metrics["type_unit_counts"]

    unknown_type_rows = sum(
        count
        for (sensor_type, _unit), count in type_unit_counts.items()
        if sensor_type not in EXPECTED_UNITS
    )
    record(
        "sensor_type_known",
        unknown_type_rows == 0,
        f"{unknown_type_rows} rows with an unknown sensor_type",
    )

    unit_mismatch_rows = sum(
        count
        for (sensor_type, unit), count in type_unit_counts.items()
        if sensor_type in EXPECTED_UNITS
        and unit != EXPECTED_UNITS[sensor_type]
    )
    record(
        "unit_matches_sensor_type",
        unit_mismatch_rows == 0,
        f"{unit_mismatch_rows} rows with the wrong unit",
    )

    value_ranges = metrics["value_ranges"]

    negative_types = {
        sensor_type: bounds["min"]
        for sensor_type, bounds in value_ranges.items()
        if bounds["min"] is not None and bounds["min"] < 0
    }
    record(
        "values_non_negative",
        not negative_types,
        f"negative minimums: {negative_types}"
        if negative_types
        else "no negative values",
    )

    too_high_types = {
        sensor_type: bounds["max"]
        for sensor_type, bounds in value_ranges.items()
        if sensor_type in PLAUSIBLE_MAX
        and bounds["max"] is not None
        and bounds["max"] > PLAUSIBLE_MAX[sensor_type]
    }
    record(
        "values_within_plausible_range",
        not too_high_types,
        f"maximums above plausible limit: {too_high_types}"
        if too_high_types
        else "all maximums within plausible limits",
        severity=WARNING,
    )

    per_timestamp = metrics["events_per_timestamp"]
    complete = (
        per_timestamp["min"]
        == per_timestamp["max"]
        == EXPECTED_EVENTS_PER_TIMESTAMP
    )
    record(
        "timestamps_complete",
        complete,
        f"{per_timestamp['timestamps']} timestamps with "
        f"{per_timestamp['min']} to {per_timestamp['max']} events each "
        f"(expected {EXPECTED_EVENTS_PER_TIMESTAMP})",
    )

    return results


def has_errors(results):
    """Return True when any ERROR-severity check failed."""

    return any(
        not result["passed"] and result["severity"] == ERROR
        for result in results
    )


def format_report(results):
    """Format check results as readable text."""

    lines = []

    for result in results:
        if result["passed"]:
            status = "PASS"
        elif result["severity"] == WARNING:
            status = "WARN"
        else:
            status = "FAIL"

        lines.append(
            f"[{status}] {result['name']}: {result['detail']}"
        )

    return "\n".join(lines)


def compute_quality_metrics(events):
    """Collect the metrics the checks need from a Spark DataFrame."""

    from pyspark.sql import functions as F

    non_finite = (
        F.isnan("value")
        | (F.col("value") == float("inf"))
        | (F.col("value") == float("-inf"))
    )

    overall = events.agg(
        F.count("*").alias("total_rows"),
        F.countDistinct("event_id").alias("distinct_event_ids"),
        F.sum(F.when(non_finite, 1).otherwise(0)).alias("non_finite"),
        *[
            F.sum(
                F.when(F.col(column).isNull(), 1).otherwise(0)
            ).alias(f"null_{column}")
            for column in REQUIRED_COLUMNS
        ],
    ).first()

    type_unit_counts = {
        (row["sensor_type"], row["unit"]): row["count"]
        for row in events.groupBy("sensor_type", "unit").count().collect()
    }

    value_ranges = {
        row["sensor_type"]: {
            "min": row["min_value"],
            "max": row["max_value"],
        }
        for row in events.groupBy("sensor_type")
        .agg(
            F.min("value").alias("min_value"),
            F.max("value").alias("max_value"),
        )
        .collect()
    }

    timestamp_stats = (
        events.groupBy("event_time")
        .agg(F.count("*").alias("events"))
        .agg(
            F.min("events").alias("min_events"),
            F.max("events").alias("max_events"),
            F.count("*").alias("timestamps"),
        )
        .first()
    )

    return {
        "total_rows": overall["total_rows"] or 0,
        "distinct_event_ids": overall["distinct_event_ids"] or 0,
        "non_finite_values": overall["non_finite"] or 0,
        "null_counts": {
            column: overall[f"null_{column}"] or 0
            for column in REQUIRED_COLUMNS
        },
        "type_unit_counts": type_unit_counts,
        "value_ranges": value_ranges,
        "events_per_timestamp": {
            "min": timestamp_stats["min_events"],
            "max": timestamp_stats["max_events"],
            "timestamps": timestamp_stats["timestamps"] or 0,
        },
    }


def main():
    """Run the quality checks against the Parquet lake."""

    from pyspark.sql import SparkSession

    parquet_path = (
        sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PARQUET_PATH
    )

    spark = (
        SparkSession.builder
        .appName("LakeQualityChecks")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        events = spark.read.parquet(parquet_path)
        results = evaluate_quality(compute_quality_metrics(events))

        print("\nData-quality report for", parquet_path)
        print(format_report(results))

        if has_errors(results):
            raise SystemExit("\nQUALITY CHECKS FAILED.")

        print("\nQUALITY CHECKS PASSED.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

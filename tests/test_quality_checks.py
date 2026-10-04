"""Tests for the Parquet lake data-quality rules."""

from src.data.quality_checks import (
    ERROR,
    EXPECTED_EVENTS_PER_TIMESTAMP,
    EXPECTED_UNITS,
    REQUIRED_COLUMNS,
    WARNING,
    evaluate_quality,
    format_report,
    has_errors,
)
from src.anomaly_detection.baseline_detector import TOTAL_SENSORS
from src.simulator.sensor_simulator import SENSOR_UNITS


def good_metrics():
    """Metrics for the clean 12-timestamp replay (12 x 119 events)."""

    return {
        "total_rows": 1428,
        "distinct_event_ids": 1428,
        "non_finite_values": 0,
        "null_counts": {column: 0 for column in REQUIRED_COLUMNS},
        "type_unit_counts": {
            ("pressure", "m"): 396,
            ("flow", "m3/h"): 36,
            ("level", "m"): 12,
            ("demand", "L/h"): 984,
        },
        "value_ranges": {
            "pressure": {"min": 27.0, "max": 56.9},
            "flow": {"min": 0.0, "max": 175.0},
            "level": {"min": 2.0, "max": 3.9},
            "demand": {"min": 0.0, "max": 7000.0},
        },
        "events_per_timestamp": {
            "min": EXPECTED_EVENTS_PER_TIMESTAMP,
            "max": EXPECTED_EVENTS_PER_TIMESTAMP,
            "timestamps": 12,
        },
    }


def result_for(results, name):
    return next(result for result in results if result["name"] == name)


def test_clean_lake_passes_every_check():
    results = evaluate_quality(good_metrics())

    assert all(result["passed"] for result in results)
    assert has_errors(results) is False


def test_expected_units_match_the_simulator_contract():
    assert EXPECTED_UNITS == SENSOR_UNITS


def test_expected_events_per_timestamp_matches_the_sensor_count():
    assert EXPECTED_EVENTS_PER_TIMESTAMP == TOTAL_SENSORS


def test_empty_lake_fails():
    metrics = good_metrics()
    metrics["total_rows"] = 0
    metrics["distinct_event_ids"] = 0

    results = evaluate_quality(metrics)

    assert result_for(results, "rows_present")["passed"] is False
    assert has_errors(results) is True


def test_duplicate_event_ids_fail():
    metrics = good_metrics()
    metrics["total_rows"] = 2856

    results = evaluate_quality(metrics)

    check = result_for(results, "event_id_unique")

    assert check["passed"] is False
    assert "1428" in check["detail"]
    assert has_errors(results) is True


def test_null_in_a_required_column_fails():
    metrics = good_metrics()
    metrics["null_counts"]["sensor_id"] = 3

    results = evaluate_quality(metrics)

    assert result_for(results, "required_columns_not_null")["passed"] is False


def test_non_finite_values_fail():
    metrics = good_metrics()
    metrics["non_finite_values"] = 2

    results = evaluate_quality(metrics)

    assert result_for(results, "values_finite")["passed"] is False


def test_unknown_sensor_type_fails():
    metrics = good_metrics()
    metrics["type_unit_counts"][("temperature", "C")] = 5

    results = evaluate_quality(metrics)

    assert result_for(results, "sensor_type_known")["passed"] is False


def test_wrong_unit_fails():
    metrics = good_metrics()
    metrics["type_unit_counts"][("pressure", "psi")] = 4

    results = evaluate_quality(metrics)

    assert result_for(results, "unit_matches_sensor_type")["passed"] is False


def test_negative_value_fails():
    metrics = good_metrics()
    metrics["value_ranges"]["pressure"]["min"] = -0.5

    results = evaluate_quality(metrics)

    assert result_for(results, "values_non_negative")["passed"] is False
    assert has_errors(results) is True


def test_implausibly_high_value_only_warns():
    metrics = good_metrics()
    metrics["value_ranges"]["pressure"]["max"] = 250.0

    results = evaluate_quality(metrics)

    check = result_for(results, "values_within_plausible_range")

    assert check["passed"] is False
    assert check["severity"] == WARNING
    assert has_errors(results) is False


def test_incomplete_timestamp_fails():
    metrics = good_metrics()
    metrics["events_per_timestamp"]["min"] = EXPECTED_EVENTS_PER_TIMESTAMP - 1

    results = evaluate_quality(metrics)

    assert result_for(results, "timestamps_complete")["passed"] is False
    assert has_errors(results) is True


def test_report_marks_failures_and_warnings():
    metrics = good_metrics()
    metrics["total_rows"] = 2856
    metrics["value_ranges"]["pressure"]["max"] = 250.0

    report = format_report(evaluate_quality(metrics))

    assert "[FAIL] event_id_unique" in report
    assert "[WARN] values_within_plausible_range" in report
    assert "[PASS] rows_present" in report


def test_error_severity_constant():
    assert ERROR == "ERROR"

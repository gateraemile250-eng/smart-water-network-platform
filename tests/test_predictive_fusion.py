import pandas as pd
import pytest

from src.predictive.predictive_anomaly_fusion import (
    PREDICTIVE_THRESHOLD,
    classify_intelligence_state,
    load_anomaly_results,
)


def test_normal_state():
    assert classify_intelligence_state(False, 0.20) == "NORMAL"


def test_early_warning_state():
    assert (
        classify_intelligence_state(False, PREDICTIVE_THRESHOLD)
        == "EARLY_WARNING"
    )


def test_current_anomaly_state():
    assert classify_intelligence_state(True, 0.20) == "CURRENT_ANOMALY"


def test_high_priority_state():
    assert (
        classify_intelligence_state(True, PREDICTIVE_THRESHOLD)
        == "HIGH_PRIORITY"
    )


def test_threshold_is_valid():
    assert 0 < PREDICTIVE_THRESHOLD < 1


def test_state_inputs_are_boolean_and_numeric():
    assert classify_intelligence_state(False, 0.0) == "NORMAL"
    assert classify_intelligence_state(True, 1.0) == "HIGH_PRIORITY"


def test_anomaly_results_have_unique_timestamps():
    df = load_anomaly_results()

    assert not df.empty
    assert df["event_time"].notna().all()
    assert not df["event_time"].duplicated().any()


def test_anomaly_results_required_fields_are_complete():
    df = load_anomaly_results()

    required = [
        "event_time",
        "is_baseline_anomaly",
        "baseline_sensor_count",
        "baseline_sensor_fraction",
        "ml_anomaly_score",
        "is_ml_anomaly",
    ]

    assert df[required].notna().all().all()


def test_anomaly_results_are_temporally_orderable():
    df = load_anomaly_results()

    ordered = df.sort_values("event_time")

    assert ordered["event_time"].is_monotonic_increasing
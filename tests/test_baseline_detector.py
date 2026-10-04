"""Tests for the baseline (z-score) anomaly detector."""

import numpy as np
import pandas as pd
import pytest

from src.anomaly_detection.baseline_detector import (
    CALIBRATION_PERIOD,
    NETWORK_SENSOR_THRESHOLD,
    SENSOR_Z_THRESHOLD,
    TOTAL_SENSORS,
    calculate_baseline_scores,
    calculate_network_evidence,
    flag_network_anomalies,
)


def make_weekly_frame(row_count):
    """Two noisy weekly-deviation columns with a fixed random seed."""

    rng = np.random.default_rng(42)

    return pd.DataFrame(
        {
            "pressure__n1__weekly_dev": rng.normal(0, 1, row_count),
            "flow__f1__weekly_dev": rng.normal(0, 1, row_count),
        }
    )


def test_scores_are_missing_during_calibration():
    scored = calculate_baseline_scores(make_weekly_frame(CALIBRATION_PERIOD + 10))
    column = scored["pressure__n1__weekly_z"]

    assert column.iloc[:CALIBRATION_PERIOD].isna().all()
    assert column.iloc[CALIBRATION_PERIOD:].notna().all()


def test_scores_never_look_ahead():
    # A z-score at time t may only use data before t. If it did,
    # appending future rows (including a huge spike) would change
    # scores that were already calculated.
    full = make_weekly_frame(CALIBRATION_PERIOD + 100)
    full.iloc[-1, 0] = 500.0
    truncated = full.iloc[: CALIBRATION_PERIOD + 50].copy()

    scores_full = calculate_baseline_scores(full)["pressure__n1__weekly_z"]
    scores_truncated = calculate_baseline_scores(truncated)[
        "pressure__n1__weekly_z"
    ]

    pd.testing.assert_series_equal(
        scores_full.iloc[: len(scores_truncated)],
        scores_truncated,
    )


def test_large_spike_gets_a_large_score():
    frame = make_weekly_frame(CALIBRATION_PERIOD + 20)
    frame.iloc[-1, 0] = 500.0

    scores = calculate_baseline_scores(frame)["pressure__n1__weekly_z"]

    assert scores.iloc[-1] > SENSOR_Z_THRESHOLD * 10


def test_score_excludes_the_current_observation():
    # The baseline for time t is built from rows before t only, so a
    # reading can never hide itself by inflating its own average.
    frame = make_weekly_frame(CALIBRATION_PERIOD + 20)
    frame.iloc[-1, 0] = 500.0

    history = frame["pressure__n1__weekly_dev"].iloc[:-1]
    expected = (500.0 - history.mean()) / history.std()

    score = calculate_baseline_scores(frame)["pressure__n1__weekly_z"].iloc[-1]

    assert score == pytest.approx(expected)


def test_detection_thresholds_are_pinned():
    # These values define detector behaviour and are documented in the
    # README. Changing one should be a deliberate decision, so update
    # this test together with the documentation.
    assert SENSOR_Z_THRESHOLD == 3.0
    assert NETWORK_SENSOR_THRESHOLD == 15
    assert TOTAL_SENSORS == 119
    assert CALIBRATION_PERIOD == 12 * 24 * 7


def test_network_evidence_counts_sensors_at_or_above_threshold():
    frame = pd.DataFrame(
        {
            "a__weekly_z": [0.5, 0.0, np.nan, 3.0],
            "b__weekly_z": [-3.0, 0.0, 5.0, 3.0],
            "c__weekly_z": [3.5, 0.0, 5.0, 3.0],
        }
    )

    result = calculate_network_evidence(frame)

    counts = result["anomalous_sensor_count"]

    # Row 0: |-3.0| and 3.5 reach the threshold. Row 2 has a missing
    # score, so no evidence is calculated for it.
    assert counts.iloc[0] == 2
    assert counts.iloc[1] == 0
    assert np.isnan(counts.iloc[2])
    assert counts.iloc[3] == 3
    assert result["anomalous_sensor_fraction"].iloc[0] == pytest.approx(
        2 / TOTAL_SENSORS
    )


def test_network_anomaly_flag_uses_the_count_threshold():
    frame = pd.DataFrame(
        {
            "anomalous_sensor_count": [
                NETWORK_SENSOR_THRESHOLD - 1,
                NETWORK_SENSOR_THRESHOLD,
                TOTAL_SENSORS,
                np.nan,
            ]
        }
    )

    flags = flag_network_anomalies(frame)["is_network_anomaly"]

    simplified = [None if pd.isna(flag) else bool(flag) for flag in flags]

    assert simplified == [False, True, True, None]

"""Tests for the SCADA feature-engineering functions."""

import numpy as np
import pandas as pd
import pytest

from src.anomaly_detection.feature_engineering import (
    HOURLY_LAG,
    WEEKLY_LAG,
    add_change_features,
    add_hourly_change_features,
    add_temporal_features,
    add_weekly_deviation_features,
    prepare_detector_features,
)


def make_frame(row_count):
    """Two sensors on a 5-minute grid; values increase steadily."""

    return pd.DataFrame(
        {
            "Timestamp": pd.date_range(
                "2018-01-01", periods=row_count, freq="5min"
            ),
            "pressure__n1": np.arange(row_count, dtype=float),
            "flow__f1": np.arange(row_count, dtype=float) * 2,
        }
    )


def test_lags_match_the_five_minute_grid():
    assert HOURLY_LAG == 12
    assert WEEKLY_LAG == 12 * 24 * 7 == 2016


def test_temporal_features_hour_and_weekday():
    result = add_temporal_features(make_frame(300))

    # 2018-01-01 is a Monday (day_of_week 0). Row 156 is 13 hours in.
    assert result.loc[0, "hour"] == 0
    assert result.loc[156, "hour"] == 13
    assert (result.loc[:287, "day_of_week"] == 0).all()
    assert result.loc[288, "day_of_week"] == 1


def test_temporal_features_do_not_modify_the_input():
    frame = make_frame(10)

    add_temporal_features(frame)

    assert "hour" not in frame.columns


def test_change_features_are_five_minute_differences():
    result = add_change_features(make_frame(5))

    assert np.isnan(result.loc[0, "pressure__n1__diff"])
    assert (result.loc[1:, "pressure__n1__diff"] == 1.0).all()
    assert (result.loc[1:, "flow__f1__diff"] == 2.0).all()


def test_hourly_features_use_a_twelve_step_lag():
    result = add_hourly_change_features(make_frame(30))
    column = result["pressure__n1__hourly_diff"]

    assert column.iloc[:HOURLY_LAG].isna().all()
    assert (column.iloc[HOURLY_LAG:] == 12.0).all()


def test_hourly_features_ignore_derived_columns():
    frame = add_change_features(make_frame(30))

    result = add_hourly_change_features(frame)

    hourly_columns = {
        column
        for column in result.columns
        if column.endswith("__hourly_diff")
    }

    assert hourly_columns == {
        "pressure__n1__hourly_diff",
        "flow__f1__hourly_diff",
    }


def test_weekly_deviation_uses_a_one_week_lag():
    result = add_weekly_deviation_features(make_frame(WEEKLY_LAG + 5))
    column = result["pressure__n1__weekly_dev"]

    assert column.iloc[:WEEKLY_LAG].isna().all()
    assert (column.iloc[WEEKLY_LAG:] == float(WEEKLY_LAG)).all()


def test_prepare_detector_features_drops_warmup_rows():
    frame = make_frame(WEEKLY_LAG + 5)
    frame = add_temporal_features(frame)
    frame = add_change_features(frame)
    frame = add_hourly_change_features(frame)
    frame = add_weekly_deviation_features(frame)

    result = prepare_detector_features(frame)

    derived = [
        column
        for column in result.columns
        if column.endswith(("__diff", "__hourly_diff", "__weekly_dev"))
    ]

    assert len(result) == 5
    assert result[derived].isna().sum().sum() == 0
    assert list(result.index) == [0, 1, 2, 3, 4]
    assert result.loc[0, "Timestamp"] == pd.Timestamp(
        "2018-01-01"
    ) + pd.Timedelta(minutes=5 * WEEKLY_LAG)

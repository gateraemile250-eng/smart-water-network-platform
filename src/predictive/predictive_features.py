"""Predictive feature engineering for water-network leakage forecasting."""

import pandas as pd

from src.anomaly_detection.feature_engineering import (
    build_scada_feature_table,
)


ROLLING_WINDOW = 72  # 72 × 5 minutes = 6 hours
TREND_LAG = 72       # 72 × 5 minutes = 6 hours


SENSOR_GROUPS = {
    "pressure": "pressure__",
    "flow": "flow__",
    "level": "level__",
    "demand": "demand__",
}


def add_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add calendar context available at prediction time."""

    result = df.copy()
    timestamp_column = result.columns[0]

    result["hour"] = result[timestamp_column].dt.hour
    result["day_of_week"] = result[timestamp_column].dt.dayofweek

    return result


def add_network_rolling_features(
    df: pd.DataFrame,
    window: int = ROLLING_WINDOW,
) -> pd.DataFrame:
    """
    Add trailing network-level rolling statistics.

    Only observations at or before the current timestamp are used.
    """

    result = df.copy()

    sensor_columns = [
        column
        for column in result.columns
        if "__" in column
    ]

    for sensor_group, prefix in SENSOR_GROUPS.items():
        group_columns = [
            column
            for column in sensor_columns
            if column.startswith(prefix)
        ]

        if not group_columns:
            continue

        network_series = result[group_columns].mean(axis=1)

        rolling = network_series.rolling(
            window=window,
            min_periods=window,
        )

        result[f"{sensor_group}_network_rolling_mean"] = (
            rolling.mean()
        )

        result[f"{sensor_group}_network_rolling_std"] = (
            rolling.std()
        )

        result[f"{sensor_group}_network_rolling_min"] = (
            rolling.min()
        )

        result[f"{sensor_group}_network_rolling_max"] = (
            rolling.max()
        )

    return result


def add_network_trend_features(
    df: pd.DataFrame,
    lag: int = TREND_LAG,
) -> pd.DataFrame:
    """
    Add six-hour network-level change features.

    Each feature compares the current network-level value with
    the value six hours earlier.
    """

    result = df.copy()

    sensor_columns = [
        column
        for column in result.columns
        if "__" in column
    ]

    for sensor_group, prefix in SENSOR_GROUPS.items():
        group_columns = [
            column
            for column in sensor_columns
            if column.startswith(prefix)
        ]

        if not group_columns:
            continue

        network_series = result[group_columns].mean(axis=1)

        result[f"{sensor_group}_network_6h_change"] = (
            network_series - network_series.shift(lag)
        )

    return result


def build_predictive_feature_table() -> pd.DataFrame:
    """
    Build the predictive feature table from the existing SCADA data.

    The existing anomaly-detection feature-engineering module is
    reused without modifying it.
    """

    features = build_scada_feature_table()

    features = add_temporal_features(features)
    features = add_network_rolling_features(features)
    features = add_network_trend_features(features)

    return features


def prepare_predictive_features(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Remove rows without sufficient historical information.

    A complete six-hour history is required for the rolling
    statistics and six-hour change features.
    """

    result = df.copy()

    predictive_columns = []

    for sensor_group in SENSOR_GROUPS:
        predictive_columns.extend(
            [
                f"{sensor_group}_network_rolling_mean",
                f"{sensor_group}_network_rolling_std",
                f"{sensor_group}_network_rolling_min",
                f"{sensor_group}_network_rolling_max",
                f"{sensor_group}_network_6h_change",
            ]
        )

    result = (
        result
        .dropna(subset=predictive_columns)
        .reset_index(drop=True)
    )

    return result


def summarize_feature_table(
    raw_features: pd.DataFrame,
    prepared_features: pd.DataFrame,
) -> None:
    """Print a concise validation summary."""

    timestamp_column = raw_features.columns[0]

    raw_sensor_columns = [
        column
        for column in raw_features.columns
        if "__" in column
    ]

    rolling_columns = [
        column
        for column in raw_features.columns
        if "_network_rolling_" in column
    ]

    trend_columns = [
        column
        for column in raw_features.columns
        if "_network_6h_change" in column
    ]

    print("Predictive feature table built successfully.")

    print("\nRaw feature table:")
    print("Shape:", raw_features.shape)
    print(
        "First timestamp:",
        raw_features[timestamp_column].iloc[0],
    )
    print(
        "Last timestamp:",
        raw_features[timestamp_column].iloc[-1],
    )

    print("\nFeature counts:")
    print("Individual sensor columns:", len(raw_sensor_columns))
    print("Rolling network columns:", len(rolling_columns))
    print("Network trend columns:", len(trend_columns))

    print("\nExpected history:")
    print("Rolling window:", ROLLING_WINDOW, "observations")
    print(
        "Rolling window hours:",
        ROLLING_WINDOW * 5 / 60,
    )
    print("Trend lag:", TREND_LAG, "observations")
    print(
        "Trend lag hours:",
        TREND_LAG * 5 / 60,
    )

    print("\nPrepared predictive table:")
    print("Shape:", prepared_features.shape)
    print(
        "First timestamp:",
        prepared_features[timestamp_column].iloc[0],
    )
    print(
        "Last timestamp:",
        prepared_features[timestamp_column].iloc[-1],
    )
    print(
        "Missing values:",
        prepared_features.isna().sum().sum(),
    )


if __name__ == "__main__":
    raw_features = build_predictive_feature_table()

    predictive_features = prepare_predictive_features(
        raw_features
    )

    summarize_feature_table(
        raw_features,
        predictive_features,
    )
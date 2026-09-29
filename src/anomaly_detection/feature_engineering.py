from pathlib import Path

import pandas as pd

from src.data.battledim_loader import load_scada_dataset


SCADA_FILES = {
    "pressure": "2018_SCADA_Pressures.csv",
    "flow": "2018_SCADA_Flows.csv",
    "level": "2018_SCADA_Levels.csv",
    "demand": "2018_SCADA_Demands.csv",
}

HOURLY_LAG = 12  # 12 five-minute observations = 1 hour
WEEKLY_LAG = 12 * 24 * 7  # 2,016 five-minute observations = 1 week


def build_scada_feature_table():
    """Load and combine all SCADA measurements into one timestamp-aligned table."""

    combined = None

    for sensor_type, filename in SCADA_FILES.items():
        df = load_scada_dataset(filename).copy()

        timestamp_column = df.columns[0]
        df[timestamp_column] = pd.to_datetime(df[timestamp_column])

        sensor_columns = df.columns[1:]
        df = df.rename(
            columns={
                column: f"{sensor_type}__{column}"
                for column in sensor_columns
            }
        )

        if combined is None:
            combined = df
        else:
            combined = combined.merge(
                df,
                on=timestamp_column,
                how="inner",
                validate="one_to_one",
            )

    return combined


def add_temporal_features(df):
    """Add time-based context used to model normal network behaviour."""

    result = df.copy()
    timestamp_column = result.columns[0]

    result["hour"] = result[timestamp_column].dt.hour
    result["day_of_week"] = result[timestamp_column].dt.dayofweek

    return result


def add_change_features(df):
    """Add 5-minute change features for every raw SCADA sensor."""

    sensor_columns = [
        column
        for column in df.columns
        if "__" in column
    ]

    changes = df[sensor_columns].diff()

    changes.columns = [
        f"{column}__diff"
        for column in sensor_columns
    ]

    return pd.concat(
        [df, changes],
        axis=1,
    )


def add_hourly_change_features(df):
    """Add one-hour change features for every raw SCADA sensor."""

    sensor_columns = [
        column
        for column in df.columns
        if "__" in column
        and not column.endswith("__diff")
        and not column.endswith("__hourly_diff")
        and not column.endswith("__weekly_dev")
    ]

    hourly_changes = (
        df[sensor_columns]
        - df[sensor_columns].shift(HOURLY_LAG)
    )

    hourly_changes.columns = [
        f"{column}__hourly_diff"
        for column in sensor_columns
    ]

    return pd.concat(
        [df, hourly_changes],
        axis=1,
    )


def add_weekly_deviation_features(df):
    """Add deviations from the same 5-minute period one week earlier."""

    sensor_columns = [
        column
        for column in df.columns
        if "__" in column
        and not column.endswith("__diff")
        and not column.endswith("__hourly_diff")
        and not column.endswith("__weekly_dev")
    ]

    weekly_deviation = (
        df[sensor_columns]
        - df[sensor_columns].shift(WEEKLY_LAG)
    )

    weekly_deviation.columns = [
        f"{column}__weekly_dev"
        for column in sensor_columns
    ]

    return pd.concat(
        [df, weekly_deviation],
        axis=1,
    )


def prepare_detector_features(df):
    """Remove feature warm-up rows that do not have complete history."""

    derived_feature_columns = [
        column
        for column in df.columns
        if column.endswith("__diff")
        or column.endswith("__hourly_diff")
        or column.endswith("__weekly_dev")
    ]

    detector_features = (
        df.dropna(subset=derived_feature_columns)
        .reset_index(drop=True)
    )

    return detector_features


if __name__ == "__main__":
    features = build_scada_feature_table()
    features = add_temporal_features(features)
    features = add_change_features(features)
    features = add_hourly_change_features(features)
    features = add_weekly_deviation_features(features)

    detector_features = prepare_detector_features(features)

    raw_sensor_columns = [
        column
        for column in features.columns
        if "__" in column
        and not column.endswith("__diff")
        and not column.endswith("__hourly_diff")
        and not column.endswith("__weekly_dev")
    ]

    change_columns = [
        column
        for column in features.columns
        if column.endswith("__diff")
        and not column.endswith("__hourly_diff")
    ]

    hourly_change_columns = [
        column
        for column in features.columns
        if column.endswith("__hourly_diff")
    ]

    weekly_deviation_columns = [
        column
        for column in features.columns
        if column.endswith("__weekly_dev")
    ]

    print("SCADA feature table built successfully.")
    print("Shape:", features.shape)
    print("Duplicate timestamps:", features.iloc[:, 0].duplicated().sum())
    print("First timestamp:", features.iloc[0, 0])
    print("Last timestamp:", features.iloc[-1, 0])

    print("\nFeature counts:")
    print("Raw sensor columns:", len(raw_sensor_columns))
    print("5-minute change columns:", len(change_columns))
    print("1-hour change columns:", len(hourly_change_columns))
    print("Weekly deviation columns:", len(weekly_deviation_columns))

    print("\nExpected feature warm-up:")
    print(
        "5-minute change missing values:",
        features[change_columns].isna().sum().sum(),
    )
    print(
        "1-hour change missing values:",
        features[hourly_change_columns].isna().sum().sum(),
    )
    print(
        "Weekly-deviation missing values:",
        features[weekly_deviation_columns].isna().sum().sum(),
    )

    print("\nTemporal features:")
    print(features[["hour", "day_of_week"]].head())

    print("\nDetector-ready table:")
    print("Shape:", detector_features.shape)
    print("Missing values:", detector_features.isna().sum().sum())
    print("First timestamp:", detector_features.iloc[0, 0])
    print("Last timestamp:", detector_features.iloc[-1, 0])
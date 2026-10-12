"""Build future leakage-onset prediction targets."""

from datetime import timedelta

import pandas as pd

from src.data.battledim_loader import load_leakage_events


PREDICTION_HORIZON = timedelta(hours=24)


def build_leakage_target(
    feature_df: pd.DataFrame,
    leakage_events: pd.DataFrame | None = None,
    horizon: timedelta = PREDICTION_HORIZON,
) -> pd.DataFrame:
    """
    Add a future leakage-onset target to a predictive feature table.

    A timestamp is positive when a new 2018 leakage event starts
    strictly after the prediction timestamp and no later than the
    end of the prediction horizon.

    Leakage-event metadata is used only to construct the target.
    It must not be used as model input features.
    """

    result = feature_df.copy()
    timestamp_column = result.columns[0]

    result[timestamp_column] = pd.to_datetime(
        result[timestamp_column]
    )

    if leakage_events is None:
        leakage_events = load_leakage_events()

    events = leakage_events.copy()

    events["start_time"] = pd.to_datetime(
        events["start_time"]
    )

    # Only leakage events whose onset occurs during the available
    # predictive feature period are eligible as prediction targets.
    observation_start = result[timestamp_column].min()
    observation_end = result[timestamp_column].max()

    events = events[
        (events["start_time"] >= observation_start)
        & (events["start_time"] <= observation_end)
    ].copy()

    event_starts = (
        events["start_time"]
        .drop_duplicates()
        .sort_values()
        .tolist()
    )

    result["future_leakage_24h"] = False

    for event_start in event_starts:
        window_start = event_start - horizon

        mask = (
            (result[timestamp_column] >= window_start)
            & (result[timestamp_column] < event_start)
        )

        result.loc[mask, "future_leakage_24h"] = True

    result["future_leakage_24h"] = (
        result["future_leakage_24h"].astype(bool)
    )

    return result


def summarize_prediction_target(
    df: pd.DataFrame,
) -> None:
    """Print a concise target validation summary."""

    target = df["future_leakage_24h"]

    print("Prediction target built successfully.")

    print("\nTarget distribution:")
    print(target.value_counts().to_string())

    print("\nTarget proportions:")
    print(
        target.value_counts(normalize=True)
        .mul(100)
        .round(3)
        .to_string()
    )

    print("\nPositive timestamps:", int(target.sum()))
    print("Negative timestamps:", int((~target).sum()))

    print("\nMissing target values:", int(target.isna().sum()))


if __name__ == "__main__":
    from src.predictive.predictive_features import (
        build_predictive_feature_table,
        prepare_predictive_features,
    )

    features = build_predictive_feature_table()

    prepared_features = prepare_predictive_features(
        features
    )

    predictive_dataset = build_leakage_target(
        prepared_features
    )

    summarize_prediction_target(
        predictive_dataset
    )
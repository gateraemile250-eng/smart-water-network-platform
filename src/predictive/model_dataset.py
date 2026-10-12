"""Assemble the final predictive modeling dataset."""

import pandas as pd

from src.predictive.predictive_features import (
    build_predictive_feature_table,
    prepare_predictive_features,
)
from src.predictive.prediction_target import (
    build_leakage_target,
)


TARGET_COLUMN = "future_leakage_24h"


def build_modeling_dataset() -> pd.DataFrame:
    """
    Build the final dataset used by predictive models.

    Features are constructed only from information available at
    the prediction timestamp. Leakage metadata is used only to
    construct the future prediction target.
    """

    raw_features = build_predictive_feature_table()

    predictive_features = prepare_predictive_features(
        raw_features
    )

    modeling_dataset = build_leakage_target(
        predictive_features
    )

    return modeling_dataset


def get_feature_columns(
    df: pd.DataFrame,
) -> list[str]:
    """Return columns allowed as model predictors."""

    timestamp_column = df.columns[0]

    return [
        column
        for column in df.columns
        if column not in {
            timestamp_column,
            TARGET_COLUMN,
        }
    ]


def validate_modeling_dataset(
    df: pd.DataFrame,
) -> None:
    """Validate the modeling dataset before model training."""

    timestamp_column = df.columns[0]
    feature_columns = get_feature_columns(df)

    assert TARGET_COLUMN in df.columns
    assert timestamp_column == "Timestamp"

    assert df[timestamp_column].isna().sum() == 0
    assert df[TARGET_COLUMN].isna().sum() == 0

    assert df[timestamp_column].duplicated().sum() == 0

    assert df[feature_columns].isna().sum().sum() == 0

    assert df[TARGET_COLUMN].dtype == bool

    # The target must not accidentally appear among predictors.
    assert TARGET_COLUMN not in feature_columns

    print("Modeling dataset validation passed.")
    print("Rows:", len(df))
    print("Total columns:", len(df.columns))
    print("Feature columns:", len(feature_columns))
    print("Target column:", TARGET_COLUMN)
    print("Missing values:", int(df.isna().sum().sum()))
    print(
        "Duplicate timestamps:",
        int(df[timestamp_column].duplicated().sum()),
    )


def summarize_modeling_dataset(
    df: pd.DataFrame,
) -> None:
    """Print a concise modeling-dataset summary."""

    timestamp_column = df.columns[0]
    feature_columns = get_feature_columns(df)

    print("\nFinal modeling dataset:")
    print("Shape:", df.shape)

    print("\nTime range:")
    print(df[timestamp_column].min(), "to", df[timestamp_column].max())

    print("\nFeature count:")
    print(len(feature_columns))

    print("\nTarget distribution:")
    print(df[TARGET_COLUMN].value_counts().to_string())

    print("\nTarget rate:")
    print(
        df[TARGET_COLUMN]
        .value_counts(normalize=True)
        .mul(100)
        .round(3)
        .to_string()
    )

    print("\nFirst columns:")
    print(df.columns[:10].tolist())

    print("\nLast columns:")
    print(df.columns[-10:].tolist())


if __name__ == "__main__":
    dataset = build_modeling_dataset()

    validate_modeling_dataset(dataset)

    summarize_modeling_dataset(dataset)
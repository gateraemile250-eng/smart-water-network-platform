"""Train a predictive model for future leakage risk."""

from __future__ import annotations

import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


TARGET_COLUMN = "future_leakage_24h"


def prepare_model_data(
    modeling_data: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.Series]:
    """Separate timestamps, features, and prediction target."""

    if TARGET_COLUMN not in modeling_data.columns:
        raise ValueError(
            f"Missing target column: {TARGET_COLUMN}"
        )

    required_columns = {"Timestamp", TARGET_COLUMN}

    missing_columns = (
        required_columns
        - set(modeling_data.columns)
    )

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {sorted(missing_columns)}"
        )

    data = modeling_data.copy()

    data["Timestamp"] = pd.to_datetime(
        data["Timestamp"]
    )

    data = data.sort_values(
        "Timestamp"
    ).reset_index(drop=True)

    feature_columns = [
        column
        for column in data.columns
        if column not in {
            "Timestamp",
            TARGET_COLUMN,
        }
    ]

    X = data[feature_columns].copy()
    y = data[TARGET_COLUMN].astype(int)

    return X, y


def build_logistic_regression_model() -> Pipeline:
    """Build the initial predictive model."""

    return Pipeline(
        steps=[
            (
                "scaler",
                StandardScaler(),
            ),
            (
                "model",
                LogisticRegression(
                    max_iter=2000,
                    class_weight="balanced",
                    random_state=42,
                ),
            ),
        ]
    )


def train_predictive_model(
    modeling_data: pd.DataFrame,
) -> tuple[Pipeline, pd.DataFrame]:
    """
    Train the predictive model on the chronological modeling dataset.

    This function intentionally does not perform random train/test
    splitting. Temporal validation is handled separately in Phase 8.4.
    """

    X, y = prepare_model_data(
        modeling_data
    )

    model = build_logistic_regression_model()

    model.fit(X, y)

    probabilities = model.predict_proba(X)[:, 1]
    predictions = (
        probabilities >= 0.5
    )

    results = pd.DataFrame(
        {
            "predicted_probability": probabilities,
            "predicted_leakage": predictions,
            "actual_leakage": y.to_numpy(),
        }
    )

    return model, results


def summarize_training_results(
    results: pd.DataFrame,
) -> None:
    """Print basic training results for validation."""

    print(
        "Predictive model training completed."
    )

    print(
        "Observations:",
        len(results),
    )

    print(
        "Actual positives:",
        int(
            results["actual_leakage"].sum()
        ),
    )

    print(
        "Predicted positives:",
        int(
            results["predicted_leakage"].sum()
        ),
    )

    print(
        "Mean predicted risk:",
        round(
            results[
                "predicted_probability"
            ].mean(),
            6,
        ),
    )

    print(
        "Minimum predicted risk:",
        round(
            results[
                "predicted_probability"
            ].min(),
            6,
        ),
    )

    print(
        "Maximum predicted risk:",
        round(
            results[
                "predicted_probability"
            ].max(),
            6,
        ),
    )


if __name__ == "__main__":
    from src.predictive.model_dataset import (
        build_modeling_dataset,
    )

    modeling_data = (
        build_modeling_dataset()
    )

    model, results = (
        train_predictive_model(
            modeling_data
        )
    )

    summarize_training_results(
        results
    )
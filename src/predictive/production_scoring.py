"""Generate production predictive leakage-risk scores."""

from __future__ import annotations

import pandas as pd

from src.predictive.model_dataset import build_modeling_dataset
from src.predictive.predictive_model import (
    build_logistic_regression_model,
    prepare_model_data,
)


TARGET_COLUMN = "future_leakage_24h"


def train_production_model(
    modeling_data: pd.DataFrame,
):
    """Train the production predictive model on labeled history."""

    X, y = prepare_model_data(modeling_data)

    model = build_logistic_regression_model()

    model.fit(X, y)

    return model


def generate_production_predictions(
    model,
    modeling_data: pd.DataFrame,
) -> pd.DataFrame:
    """Generate predictive leakage-risk scores."""

    X, y = prepare_model_data(modeling_data)

    probabilities = model.predict_proba(X)[:, 1]

    results = pd.DataFrame(
        {
            "event_time": pd.to_datetime(
                modeling_data["Timestamp"]
            ).to_numpy(),
            "predicted_probability": probabilities,
            "predicted_leakage": probabilities >= 0.50,
        }
    )

    results = (
        results
        .sort_values("event_time")
        .reset_index(drop=True)
    )

    if results["event_time"].duplicated().any():
        raise ValueError(
            "Production predictions contain duplicate timestamps."
        )

    if results["predicted_probability"].isna().any():
        raise ValueError(
            "Production predictions contain missing probabilities."
        )

    if not results["predicted_probability"].between(
        0.0,
        1.0,
    ).all():
        raise ValueError(
            "Predictive probabilities must be between 0 and 1."
        )

    return results


def build_production_predictions() -> pd.DataFrame:
    """Build the complete production predictive result."""

    modeling_data = build_modeling_dataset()

    model = train_production_model(
        modeling_data
    )

    predictions = generate_production_predictions(
        model,
        modeling_data,
    )

    return predictions


def summarize_production_predictions(
    predictions: pd.DataFrame,
) -> None:
    """Print production scoring validation information."""

    print(
        "Production predictive scoring completed."
    )

    print(
        "Rows:",
        len(predictions),
    )

    print(
        "Missing values:",
        int(predictions.isna().sum().sum()),
    )

    print(
        "Duplicate timestamps:",
        int(
            predictions["event_time"]
            .duplicated()
            .sum()
        ),
    )

    print(
        "\nTime range:"
    )

    print(
        predictions["event_time"].min(),
        "to",
        predictions["event_time"].max(),
    )

    print(
        "\nPredicted leakage:"
    )

    print(
        int(
            predictions["predicted_leakage"]
            .sum()
        )
    )

    print(
        "\nPredictive risk:"
    )

    print(
        predictions["predicted_probability"]
        .describe()
        .to_string()
    )


if __name__ == "__main__":
    production_predictions = (
        build_production_predictions()
    )

    summarize_production_predictions(
        production_predictions
    )
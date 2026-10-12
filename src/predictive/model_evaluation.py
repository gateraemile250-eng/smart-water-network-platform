"""Evaluate the predictive leakage model using temporal validation."""

from __future__ import annotations

import pandas as pd
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

from src.predictive.model_dataset import build_modeling_dataset
from src.predictive.predictive_model import (
    build_logistic_regression_model,
    prepare_model_data,
)


TARGET_COLUMN = "future_leakage_24h"
TRAIN_FRACTION = 0.80
CLASSIFICATION_THRESHOLD = 0.50

# Expanding-window validation.
# Each value represents the fraction of the full chronological
# dataset used as the end of the validation period.
VALIDATION_FRACTIONS = (
    (0.50, 0.65),
    (0.65, 0.80),
    (0.80, 1.00),
)


def chronological_split(
    modeling_data: pd.DataFrame,
    train_fraction: float = TRAIN_FRACTION,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split modeling data chronologically into train and test periods."""

    if not 0 < train_fraction < 1:
        raise ValueError(
            "train_fraction must be between 0 and 1."
        )

    data = modeling_data.copy()

    data["Timestamp"] = pd.to_datetime(
        data["Timestamp"]
    )

    data = (
        data
        .sort_values("Timestamp")
        .reset_index(drop=True)
    )

    split_index = int(
        len(data) * train_fraction
    )

    if split_index <= 0 or split_index >= len(data):
        raise ValueError(
            "Temporal split produced an empty "
            "training or test set."
        )

    train_data = data.iloc[
        :split_index
    ].copy()

    test_data = data.iloc[
        split_index:
    ].copy()

    if (
        train_data["Timestamp"].max()
        >= test_data["Timestamp"].min()
    ):
        raise ValueError(
            "Temporal leakage detected: "
            "training and test periods overlap."
        )

    return train_data, test_data


def evaluate_temporal_model(
    modeling_data: pd.DataFrame,
) -> tuple[dict[str, float | int], pd.DataFrame]:
    """
    Train on earlier observations and evaluate on later observations.

    The test period is never used during model fitting.
    """

    train_data, test_data = chronological_split(
        modeling_data
    )

    X_train, y_train = prepare_model_data(
        train_data
    )

    X_test, y_test = prepare_model_data(
        test_data
    )

    model = build_logistic_regression_model()

    model.fit(
        X_train,
        y_train,
    )

    test_probabilities = model.predict_proba(
        X_test
    )[:, 1]

    test_predictions = (
        test_probabilities
        >= CLASSIFICATION_THRESHOLD
    )

    precision = precision_score(
        y_test,
        test_predictions,
        zero_division=0,
    )

    recall = recall_score(
        y_test,
        test_predictions,
        zero_division=0,
    )

    f1 = f1_score(
        y_test,
        test_predictions,
        zero_division=0,
    )

    roc_auc = roc_auc_score(
        y_test,
        test_probabilities,
    )

    pr_auc = average_precision_score(
        y_test,
        test_probabilities,
    )

    tn, fp, fn, tp = confusion_matrix(
        y_test,
        test_predictions,
        labels=[0, 1],
    ).ravel()

    results = pd.DataFrame(
        {
            "Timestamp": test_data[
                "Timestamp"
            ].to_numpy(),
            "predicted_probability": (
                test_probabilities
            ),
            "predicted_leakage": (
                test_predictions
            ),
            "actual_leakage": (
                y_test.to_numpy()
            ),
        }
    )

    metrics = {
        "train_rows": len(train_data),
        "test_rows": len(test_data),
        "train_positive": int(y_train.sum()),
        "test_positive": int(y_test.sum()),
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "roc_auc": roc_auc,
        "pr_auc": pr_auc,
        "true_negatives": int(tn),
        "false_positives": int(fp),
        "false_negatives": int(fn),
        "true_positives": int(tp),
        "predicted_positive": int(
            test_predictions.sum()
        ),
    }

    return metrics, results


def _validate_temporal_boundaries(
    train_data: pd.DataFrame,
    validation_data: pd.DataFrame,
) -> None:
    """Ensure training data strictly precedes validation data."""

    train_end = train_data["Timestamp"].max()
    validation_start = validation_data["Timestamp"].min()

    if train_end >= validation_start:
        raise ValueError(
            "Temporal leakage detected: "
            "training data overlaps validation data."
        )


def _evaluate_validation_window(
    train_data: pd.DataFrame,
    validation_data: pd.DataFrame,
    fold_name: str,
) -> dict[str, float | int | str]:
    """Train and evaluate one chronological validation window."""

    _validate_temporal_boundaries(
        train_data,
        validation_data,
    )

    X_train, y_train = prepare_model_data(
        train_data
    )

    X_validation, y_validation = prepare_model_data(
        validation_data
    )

    model = build_logistic_regression_model()

    model.fit(
        X_train,
        y_train,
    )

    probabilities = model.predict_proba(
        X_validation
    )[:, 1]

    predictions = (
        probabilities
        >= CLASSIFICATION_THRESHOLD
    )

    precision = precision_score(
        y_validation,
        predictions,
        zero_division=0,
    )

    recall = recall_score(
        y_validation,
        predictions,
        zero_division=0,
    )

    f1 = f1_score(
        y_validation,
        predictions,
        zero_division=0,
    )

    roc_auc = roc_auc_score(
        y_validation,
        probabilities,
    )

    pr_auc = average_precision_score(
        y_validation,
        probabilities,
    )

    tn, fp, fn, tp = confusion_matrix(
        y_validation,
        predictions,
        labels=[0, 1],
    ).ravel()

    return {
        "fold": fold_name,
        "train_rows": len(train_data),
        "validation_rows": len(validation_data),
        "train_positive": int(y_train.sum()),
        "validation_positive": int(
            y_validation.sum()
        ),
        "train_start": train_data[
            "Timestamp"
        ].min(),
        "train_end": train_data[
            "Timestamp"
        ].max(),
        "validation_start": validation_data[
            "Timestamp"
        ].min(),
        "validation_end": validation_data[
            "Timestamp"
        ].max(),
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "roc_auc": roc_auc,
        "pr_auc": pr_auc,
        "true_negatives": int(tn),
        "false_positives": int(fp),
        "false_negatives": int(fn),
        "true_positives": int(tp),
        "predicted_positive": int(
            predictions.sum()
        ),
    }


def evaluate_expanding_windows(
    modeling_data: pd.DataFrame,
) -> pd.DataFrame:
    """
    Evaluate the model using multiple chronological expanding windows.

    Each validation period occurs strictly after its corresponding
    training period.
    """

    data = modeling_data.copy()

    data["Timestamp"] = pd.to_datetime(
        data["Timestamp"]
    )

    data = (
        data
        .sort_values("Timestamp")
        .reset_index(drop=True)
    )

    results = []

    for index, (
        train_end_fraction,
        validation_end_fraction,
    ) in enumerate(
        VALIDATION_FRACTIONS,
        start=1,
    ):
        train_end = int(
            len(data)
            * train_end_fraction
        )

        validation_end = int(
            len(data)
            * validation_end_fraction
        )

        train_data = data.iloc[
            :train_end
        ].copy()

        validation_data = data.iloc[
            train_end:validation_end
        ].copy()

        if train_data.empty or validation_data.empty:
            raise ValueError(
                "Validation window contains "
                "an empty partition."
            )

        results.append(
            _evaluate_validation_window(
                train_data,
                validation_data,
                f"fold_{index}",
            )
        )

    return pd.DataFrame(results)


def print_expanding_window_results(
    results: pd.DataFrame,
) -> None:
    """Print results from multi-period temporal validation."""

    print(
        "Expanding-window temporal validation completed."
    )

    print("\nValidation folds:")

    display_columns = [
        "fold",
        "train_rows",
        "validation_rows",
        "validation_positive",
        "precision",
        "recall",
        "f1",
        "roc_auc",
        "pr_auc",
    ]

    print(
        results[display_columns].to_string(
            index=False
        )
    )

    print("\nValidation periods:")

    for _, row in results.iterrows():
        print(
            f"\n{row['fold']}:"
        )
        print(
            "  Training:",
            row["train_start"],
            "to",
            row["train_end"],
        )
        print(
            "  Validation:",
            row["validation_start"],
            "to",
            row["validation_end"],
        )
        print(
            "  Validation positives:",
            row["validation_positive"],
        )

    print("\nMean metrics across folds:")

    metric_columns = [
        "precision",
        "recall",
        "f1",
        "roc_auc",
        "pr_auc",
    ]

    print(
        results[metric_columns]
        .mean()
        .round(6)
        .to_string()
    )


def print_evaluation_results(
    metrics: dict[str, float | int],
) -> None:
    """Print single temporal holdout results."""

    print(
        "Temporal model evaluation completed."
    )

    print("\nDataset split:")
    print(
        "Training rows:",
        metrics["train_rows"],
    )
    print(
        "Test rows:",
        metrics["test_rows"],
    )

    print("\nPositive observations:")
    print(
        "Training positives:",
        metrics["train_positive"],
    )
    print(
        "Test positives:",
        metrics["test_positive"],
    )

    print("\nClassification metrics:")
    print(
        "Precision:",
        round(
            float(metrics["precision"]),
            6,
        ),
    )
    print(
        "Recall:",
        round(
            float(metrics["recall"]),
            6,
        ),
    )
    print(
        "F1:",
        round(
            float(metrics["f1"]),
            6,
        ),
    )

    print("\nRanking metrics:")
    print(
        "ROC-AUC:",
        round(
            float(metrics["roc_auc"]),
            6,
        ),
    )
    print(
        "PR-AUC:",
        round(
            float(metrics["pr_auc"]),
            6,
        ),
    )

    print("\nConfusion matrix:")
    print(
        "True negatives:",
        metrics["true_negatives"],
    )
    print(
        "False positives:",
        metrics["false_positives"],
    )
    print(
        "False negatives:",
        metrics["false_negatives"],
    )
    print(
        "True positives:",
        metrics["true_positives"],
    )

    print(
        "\nPredicted positives:",
        metrics["predicted_positive"],
    )


if __name__ == "__main__":
    modeling_data = (
        build_modeling_dataset()
    )

    print("=" * 70)
    print("8.4.4 — ROBUST TEMPORAL VALIDATION")
    print("=" * 70)

    expanding_results = (
        evaluate_expanding_windows(
            modeling_data
        )
    )

    print_expanding_window_results(
        expanding_results
    )

    print("\n" + "=" * 70)
    print("ORIGINAL 80/20 TEMPORAL HOLDOUT")
    print("=" * 70)

    metrics, _ = (
        evaluate_temporal_model(
            modeling_data
        )
    )

    print_evaluation_results(
        metrics
    )
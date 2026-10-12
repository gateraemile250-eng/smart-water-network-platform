"""Combine production predictive risk with anomaly and localization intelligence."""

from __future__ import annotations

import pandas as pd

from src.predictive.model_dataset import build_modeling_dataset
from src.predictive.production_scoring import build_production_predictions


PREDICTIVE_THRESHOLD = 0.50

ANOMALY_RESULTS_PATH = (
    "data/publish/curated/network_anomaly_results.parquet"
)

LOCALIZATION_RESULTS_PATH = (
    "data/publish/curated/network_localization_results.parquet"
)

FUSION_OUTPUT_PATH = (
    "data/publish/curated/predictive_anomaly_fusion.parquet"
)


def load_anomaly_results() -> pd.DataFrame:
    """Load and validate network anomaly results."""

    anomaly_results = pd.read_parquet(
        ANOMALY_RESULTS_PATH
    ).copy()

    if "event_time" not in anomaly_results.columns:
        if "timestamp" in anomaly_results.columns:
            anomaly_results = anomaly_results.rename(
                columns={"timestamp": "event_time"}
            )
        else:
            raise ValueError(
                "Anomaly results must contain "
                "'event_time' or 'timestamp'."
            )

    anomaly_results["event_time"] = pd.to_datetime(
        anomaly_results["event_time"]
    )

    required_columns = {
        "event_time",
        "is_baseline_anomaly",
        "baseline_sensor_count",
        "baseline_sensor_fraction",
        "ml_anomaly_score",
        "is_ml_anomaly",
    }

    missing_columns = (
        required_columns
        - set(anomaly_results.columns)
    )

    if missing_columns:
        raise ValueError(
            f"Missing anomaly columns: {sorted(missing_columns)}"
        )

    if anomaly_results["event_time"].isna().any():
        raise ValueError(
            "Anomaly results contain missing event times."
        )

    if anomaly_results["event_time"].duplicated().any():
        raise ValueError(
            "Anomaly results contain duplicate event timestamps."
        )

    return anomaly_results


def load_localization_results() -> pd.DataFrame:
    """Load and validate localization candidates."""

    localization = pd.read_parquet(
        LOCALIZATION_RESULTS_PATH
    ).copy()

    required_columns = {
        "event_time",
        "pipe_id",
        "localization_score",
        "candidate_rank",
    }

    missing_columns = (
        required_columns
        - set(localization.columns)
    )

    if missing_columns:
        raise ValueError(
            f"Missing localization columns: {sorted(missing_columns)}"
        )

    localization["event_time"] = pd.to_datetime(
        localization["event_time"]
    )

    localization["candidate_rank"] = (
        localization["candidate_rank"].astype(int)
    )

    localization["localization_score"] = (
        localization["localization_score"].astype(float)
    )

    if localization["event_time"].isna().any():
        raise ValueError(
            "Localization results contain missing event times."
        )

    if localization["pipe_id"].isna().any():
        raise ValueError(
            "Localization results contain missing pipe IDs."
        )

    if localization.duplicated(
        subset=["event_time", "pipe_id"]
    ).any():
        raise ValueError(
            "Localization results contain duplicate "
            "(event_time, pipe_id) pairs."
        )

    return localization


def classify_intelligence_state(
    is_ml_anomaly: bool,
    predicted_probability: float,
) -> str:
    """Assign an operational intelligence state."""

    if (
        is_ml_anomaly
        and predicted_probability >= PREDICTIVE_THRESHOLD
    ):
        return "HIGH_PRIORITY"

    if is_ml_anomaly:
        return "CURRENT_ANOMALY"

    if predicted_probability >= PREDICTIVE_THRESHOLD:
        return "EARLY_WARNING"

    return "NORMAL"


def build_localization_summary(
    localization: pd.DataFrame,
) -> pd.DataFrame:
    """Build one localization summary per event timestamp."""

    columns = [
        "event_time",
        "localization_available",
        "localization_candidate_count",
        "top_candidate_pipe",
        "top_candidate_score",
        "top_candidate_rank",
    ]

    if localization.empty:
        return pd.DataFrame(columns=columns)

    localization = (
        localization
        .sort_values(
            ["event_time", "candidate_rank"]
        )
        .reset_index(drop=True)
    )

    summary = (
        localization
        .groupby("event_time", as_index=False)
        .agg(
            localization_candidate_count=(
                "pipe_id",
                "count",
            ),
            top_candidate_pipe=(
                "pipe_id",
                "first",
            ),
            top_candidate_score=(
                "localization_score",
                "first",
            ),
            top_candidate_rank=(
                "candidate_rank",
                "first",
            ),
        )
    )

    summary["localization_available"] = True

    return summary[columns]


def build_predictive_results() -> pd.DataFrame:
    """Build production predictive leakage-risk results."""

    modeling_data = build_modeling_dataset()
    predictions = build_production_predictions()

    valid_timestamps = set(
        pd.to_datetime(
            modeling_data["Timestamp"]
        )
    )

    predictions = predictions[
        predictions["event_time"].isin(
            valid_timestamps
        )
    ].copy()

    if predictions.empty:
        raise ValueError(
            "Production predictive scoring returned no "
            "valid modeling timestamps."
        )

    if predictions["event_time"].duplicated().any():
        raise ValueError(
            "Production predictions contain duplicate timestamps."
        )

    if predictions["predicted_probability"].isna().any():
        raise ValueError(
            "Production predictions contain missing probabilities."
        )

    if not predictions["predicted_probability"].between(
        0.0,
        1.0,
    ).all():
        raise ValueError(
            "Predictive probabilities must be between 0 and 1."
        )

    return predictions


def build_predictive_anomaly_fusion() -> pd.DataFrame:
    """Combine predictive risk, anomaly evidence and localization."""

    predictive = build_predictive_results()
    anomaly = load_anomaly_results()
    localization = load_localization_results()

    fused = predictive.merge(
        anomaly,
        on="event_time",
        how="inner",
        validate="one_to_one",
    )

    if fused.empty:
        raise ValueError(
            "No timestamps overlap between predictive "
            "and anomaly results."
        )

    localization_summary = build_localization_summary(
        localization
    )

    fused = fused.merge(
        localization_summary,
        on="event_time",
        how="left",
        validate="one_to_one",
    )

    fused["localization_available"] = (
        fused["localization_available"]
        .fillna(False)
        .astype(bool)
    )

    fused["localization_candidate_count"] = (
        fused["localization_candidate_count"]
        .fillna(0)
        .astype(int)
    )

    fused["top_candidate_rank"] = (
        fused["top_candidate_rank"]
        .fillna(0)
        .astype(int)
    )

    fused["intelligence_state"] = [
        classify_intelligence_state(
            bool(is_ml_anomaly),
            float(predicted_probability),
        )
        for is_ml_anomaly, predicted_probability
        in zip(
            fused["is_ml_anomaly"],
            fused["predicted_probability"],
        )
    ]

    fused = (
        fused
        .sort_values("event_time")
        .reset_index(drop=True)
    )

    required_columns = [
        "event_time",
        "predicted_probability",
        "predicted_leakage",
        "is_baseline_anomaly",
        "baseline_sensor_count",
        "baseline_sensor_fraction",
        "ml_anomaly_score",
        "is_ml_anomaly",
        "intelligence_state",
        "localization_available",
        "localization_candidate_count",
    ]

    missing_required = int(
        fused[required_columns]
        .isna()
        .sum()
        .sum()
    )

    if missing_required:
        raise ValueError(
            "Required fusion fields contain missing values: "
            f"{missing_required}"
        )

    if fused["event_time"].duplicated().any():
        raise ValueError(
            "Fusion results contain duplicate timestamps."
        )

    return fused


def save_fusion_results(
    fused: pd.DataFrame,
) -> None:
    """Persist validated production fusion results."""

    fused.to_parquet(
        FUSION_OUTPUT_PATH,
        index=False,
    )

    print(
        "Fusion results written to:",
        FUSION_OUTPUT_PATH,
    )


def summarize_fusion_results(
    fused: pd.DataFrame,
) -> None:
    """Print production fusion validation information."""

    required_columns = [
        "event_time",
        "predicted_probability",
        "predicted_leakage",
        "is_baseline_anomaly",
        "baseline_sensor_count",
        "baseline_sensor_fraction",
        "ml_anomaly_score",
        "is_ml_anomaly",
        "intelligence_state",
        "localization_available",
        "localization_candidate_count",
    ]

    optional_localization_columns = [
        "top_candidate_pipe",
        "top_candidate_score",
    ]

    required_missing = int(
        fused[required_columns]
        .isna()
        .sum()
        .sum()
    )

    optional_missing = int(
        fused[optional_localization_columns]
        .isna()
        .sum()
        .sum()
    )

    print(
        "Production predictive + anomaly + localization "
        "fusion completed."
    )

    print("Rows:", len(fused))
    print(
        "Required missing values:",
        required_missing,
    )
    print(
        "Optional localization fields missing:",
        optional_missing,
    )
    print(
        "Duplicate timestamps:",
        int(
            fused["event_time"]
            .duplicated()
            .sum()
        ),
    )

    print("\nTime range:")
    print(
        fused["event_time"].min(),
        "to",
        fused["event_time"].max(),
    )

    print("\nIntelligence states:")
    print(
        fused["intelligence_state"]
        .value_counts()
        .sort_index()
        .to_string()
    )

    print(
        "\nCurrent ML anomalies:",
        int(fused["is_ml_anomaly"].sum()),
    )

    print(
        "Early warnings:",
        int(
            (
                fused["intelligence_state"]
                == "EARLY_WARNING"
            ).sum()
        ),
    )

    print(
        "High-priority events:",
        int(
            (
                fused["intelligence_state"]
                == "HIGH_PRIORITY"
            ).sum()
        ),
    )

    print("\nLocalization:")
    print(
        "Rows with localization:",
        int(fused["localization_available"].sum()),
    )
    print(
        "Rows without localization:",
        int(
            (~fused["localization_available"]).sum()
        ),
    )
    print(
        "Localization coverage:",
        round(
            fused["localization_available"].mean(),
            6,
        ),
    )

    print("\nPredictive risk:")
    print(
        fused["predicted_probability"]
        .describe()
        .to_string()
    )


if __name__ == "__main__":
    fused_results = build_predictive_anomaly_fusion()

    summarize_fusion_results(
        fused_results
    )

    save_fusion_results(
        fused_results
    )
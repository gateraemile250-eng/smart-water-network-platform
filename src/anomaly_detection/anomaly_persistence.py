"""Persist network-level anomaly detection results to PostgreSQL."""

import pandas as pd

from src.anomaly_detection.baseline_detector import (
    build_detector_dataset,
    calculate_baseline_scores,
    calculate_network_evidence,
    flag_network_anomalies,
)
from src.anomaly_detection.isolation_forest_detector import (
    TRAINING_PERIOD,
    calculate_anomaly_scores,
    calculate_anomaly_threshold,
    fit_isolation_forest,
    flag_ml_anomalies,
    select_ml_features,
    split_training_and_scoring_data,
)
from src.data.database import get_database_connection


UPSERT_SQL = """
INSERT INTO network_anomaly_results (
    timestamp,
    baseline_sensor_count,
    baseline_sensor_fraction,
    is_baseline_anomaly,
    ml_anomaly_score,
    is_ml_anomaly
)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (timestamp)
DO UPDATE SET
    baseline_sensor_count = EXCLUDED.baseline_sensor_count,
    baseline_sensor_fraction = EXCLUDED.baseline_sensor_fraction,
    is_baseline_anomaly = EXCLUDED.is_baseline_anomaly,
    ml_anomaly_score = EXCLUDED.ml_anomaly_score,
    is_ml_anomaly = EXCLUDED.is_ml_anomaly;
"""


def build_anomaly_results():
    """Build aligned baseline and Isolation Forest anomaly results."""

    # Build the common detector-ready dataset once.
    detector_data = build_detector_dataset()

    timestamp_column = detector_data.columns[0]
    timestamps = pd.to_datetime(detector_data[timestamp_column])

    # ------------------------------------------------------------------
    # Baseline detector
    # ------------------------------------------------------------------

    baseline_scored = calculate_baseline_scores(detector_data)
    baseline_evidence = calculate_network_evidence(baseline_scored)
    baseline_flagged = flag_network_anomalies(baseline_evidence)

    baseline_results = pd.DataFrame(
        {
            "timestamp": timestamps,
            "baseline_sensor_count": baseline_flagged[
                "anomalous_sensor_count"
            ],
            "baseline_sensor_fraction": baseline_flagged[
                "anomalous_sensor_fraction"
            ],
            "is_baseline_anomaly": baseline_flagged[
                "is_network_anomaly"
            ],
        }
    )

    # ------------------------------------------------------------------
    # Isolation Forest
    # ------------------------------------------------------------------

    ml_features, _ = select_ml_features(detector_data)

    training_features, scoring_features = (
        split_training_and_scoring_data(ml_features)
    )

    model = fit_isolation_forest(training_features)

    training_scores = calculate_anomaly_scores(
        model,
        training_features,
    )

    anomaly_threshold = calculate_anomaly_threshold(
        training_scores
    )

    ml_scores = calculate_anomaly_scores(
        model,
        scoring_features,
    )

    ml_flags = flag_ml_anomalies(
        ml_scores,
        anomaly_threshold,
    )

    ml_results = pd.DataFrame(
        {
            "timestamp": timestamps.iloc[
                TRAINING_PERIOD:
            ].to_numpy(),
            "ml_anomaly_score": ml_scores,
            "is_ml_anomaly": ml_flags,
        }
    )

    # ------------------------------------------------------------------
    # Align detector outputs
    # ------------------------------------------------------------------

    results = baseline_results.merge(
        ml_results,
        on="timestamp",
        how="inner",
        validate="one_to_one",
    )

    results = results.dropna(
        subset=[
            "baseline_sensor_count",
            "baseline_sensor_fraction",
            "is_baseline_anomaly",
            "ml_anomaly_score",
            "is_ml_anomaly",
        ]
    ).copy()

    results["baseline_sensor_count"] = (
        results["baseline_sensor_count"].astype(int)
    )

    results["baseline_sensor_fraction"] = (
        results["baseline_sensor_fraction"].astype(float)
    )

    results["is_baseline_anomaly"] = (
        results["is_baseline_anomaly"].astype(bool)
    )

    results["ml_anomaly_score"] = (
        results["ml_anomaly_score"].astype(float)
    )

    results["is_ml_anomaly"] = (
        results["is_ml_anomaly"].astype(bool)
    )

    return results, anomaly_threshold


def upsert_anomaly_results(results):
    """Insert or update network anomaly results in PostgreSQL."""

    connection = get_database_connection()

    try:
        cursor = connection.cursor()

        rows = [
            (
                row.timestamp.to_pydatetime(),
                int(row.baseline_sensor_count),
                float(row.baseline_sensor_fraction),
                bool(row.is_baseline_anomaly),
                float(row.ml_anomaly_score),
                bool(row.is_ml_anomaly),
            )
            for row in results.itertuples(index=False)
        ]

        cursor.executemany(UPSERT_SQL, rows)

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


if __name__ == "__main__":
    anomaly_results, threshold = build_anomaly_results()

    print("Anomaly results prepared successfully.")
    print("Rows:", len(anomaly_results))
    print(
        "First timestamp:",
        anomaly_results["timestamp"].min(),
    )
    print(
        "Last timestamp:",
        anomaly_results["timestamp"].max(),
    )
    print(
        "Baseline anomalies:",
        int(anomaly_results["is_baseline_anomaly"].sum()),
    )
    print(
        "ML anomalies:",
        int(anomaly_results["is_ml_anomaly"].sum()),
    )
    print(
        "ML threshold:",
        round(threshold, 6),
    )

    print("\nWriting anomaly results to PostgreSQL...")

    upsert_anomaly_results(anomaly_results)

    print("Anomaly results persisted successfully.")
import numpy as np
from sklearn.ensemble import IsolationForest

from src.anomaly_detection.feature_engineering import (
    add_change_features,
    add_hourly_change_features,
    add_temporal_features,
    add_weekly_deviation_features,
    build_scada_feature_table,
    prepare_detector_features,
)


# ---------------------------------------------------------------------------
# Model configuration
# ---------------------------------------------------------------------------

# 12 five-minute observations/hour × 24 hours/day × 7 days
TRAINING_PERIOD = 12 * 24 * 7

N_ESTIMATORS = 200
RANDOM_STATE = 42

# The alert threshold is calibrated from historical training scores only.
# Leakage ground truth and future scoring observations are not used.
ANOMALY_QUANTILE = 0.99


# ---------------------------------------------------------------------------
# Dataset preparation
# ---------------------------------------------------------------------------

def build_ml_dataset():
    """Build the detector-ready dataset for Isolation Forest."""

    features = build_scada_feature_table()
    features = add_temporal_features(features)
    features = add_change_features(features)
    features = add_hourly_change_features(features)
    features = add_weekly_deviation_features(features)

    return prepare_detector_features(features)


def select_ml_features(df):
    """
    Select behavioural features used by the ML anomaly detector.

    The model uses:
    - 5-minute sensor changes
    - 1-hour sensor changes
    - weekly sensor deviations

    Raw sensor measurements and leakage ground truth are excluded.
    """

    change_columns = [
        column
        for column in df.columns
        if column.endswith("__diff")
        and not column.endswith("__hourly_diff")
    ]

    hourly_change_columns = [
        column
        for column in df.columns
        if column.endswith("__hourly_diff")
    ]

    weekly_deviation_columns = [
        column
        for column in df.columns
        if column.endswith("__weekly_dev")
    ]

    ml_columns = (
        change_columns
        + hourly_change_columns
        + weekly_deviation_columns
    )

    return df[ml_columns], ml_columns


def split_training_and_scoring_data(ml_features):
    """
    Split ML features into historical training and future scoring periods.

    The model is fitted only on observations that occur before the
    scoring period.
    """

    training_features = ml_features.iloc[:TRAINING_PERIOD].copy()
    scoring_features = ml_features.iloc[TRAINING_PERIOD:].copy()

    return training_features, scoring_features


# ---------------------------------------------------------------------------
# Isolation Forest
# ---------------------------------------------------------------------------

def fit_isolation_forest(training_features):
    """Fit an unsupervised Isolation Forest on the training period."""

    model = IsolationForest(
        n_estimators=N_ESTIMATORS,
        contamination="auto",
        random_state=RANDOM_STATE,
        n_jobs=-1,
    )

    model.fit(training_features)

    return model


def calculate_anomaly_scores(model, features):
    """
    Calculate continuous Isolation Forest anomaly scores.

    Isolation Forest's score_samples() returns lower values for more
    abnormal observations. Negating the values makes higher scores
    represent stronger anomaly evidence.
    """

    return -model.score_samples(features)


def calculate_anomaly_threshold(training_scores):
    """
    Calculate a fixed anomaly threshold from historical training scores.

    Future scoring observations and leakage ground truth are not used
    when determining the threshold.
    """

    return float(
        np.quantile(
            training_scores,
            ANOMALY_QUANTILE,
        )
    )


def flag_ml_anomalies(anomaly_scores, threshold):
    """Flag observations whose anomaly score reaches the fixed threshold."""

    return anomaly_scores >= threshold


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    detector_data = build_ml_dataset()

    ml_features, ml_columns = select_ml_features(detector_data)

    training_features, scoring_features = (
        split_training_and_scoring_data(
            ml_features
        )
    )

    change_columns = [
        column
        for column in ml_columns
        if column.endswith("__diff")
        and not column.endswith("__hourly_diff")
    ]

    hourly_change_columns = [
        column
        for column in ml_columns
        if column.endswith("__hourly_diff")
    ]

    weekly_deviation_columns = [
        column
        for column in ml_columns
        if column.endswith("__weekly_dev")
    ]

    training_end_index = TRAINING_PERIOD - 1
    scoring_start_index = TRAINING_PERIOD

    print("Isolation Forest dataset prepared successfully.")
    print("Detector dataset shape:", detector_data.shape)

    print("\nML feature selection:")
    print("5-minute change features:", len(change_columns))
    print("1-hour change features:", len(hourly_change_columns))
    print(
        "Weekly deviation features:",
        len(weekly_deviation_columns),
    )
    print("Total ML features:", len(ml_columns))

    print("\nML feature matrix:")
    print("Shape:", ml_features.shape)
    print("Missing values:", ml_features.isna().sum().sum())
    print(
        "Duplicate feature names:",
        ml_features.columns.duplicated().sum(),
    )

    print("\nDetector period:")
    print("First timestamp:", detector_data.iloc[0, 0])
    print("Last timestamp:", detector_data.iloc[-1, 0])

    print("\nInitial ML training configuration:")
    print("Training observations:", TRAINING_PERIOD)
    print("Training duration: 7 days")

    print("\nCausal train/scoring split:")
    print("Training shape:", training_features.shape)
    print(
        "Training start timestamp:",
        detector_data.iloc[0, 0],
    )
    print(
        "Training end timestamp:",
        detector_data.iloc[training_end_index, 0],
    )

    print("Scoring shape:", scoring_features.shape)
    print(
        "Scoring start timestamp:",
        detector_data.iloc[scoring_start_index, 0],
    )
    print(
        "Scoring end timestamp:",
        detector_data.iloc[-1, 0],
    )

    print(
        "Total split observations:",
        len(training_features) + len(scoring_features),
    )

    print("\nFitting Isolation Forest...")

    model = fit_isolation_forest(training_features)

    print("Isolation Forest fitted successfully.")
    print("Number of trees:", len(model.estimators_))
    print("Contamination:", model.contamination)
    print("Random state:", RANDOM_STATE)

    # Historical scores are used only to calibrate the fixed threshold.
    training_scores = calculate_anomaly_scores(
        model,
        training_features,
    )

    anomaly_threshold = calculate_anomaly_threshold(
        training_scores
    )

    # Future observations are then scored against that fixed threshold.
    anomaly_scores = calculate_anomaly_scores(
        model,
        scoring_features,
    )

    print("\nHistorical threshold calibration:")
    print(
        "Calibration observations:",
        len(training_scores),
    )
    print("Anomaly quantile:", ANOMALY_QUANTILE)
    print(
        "Fixed anomaly-score threshold:",
        round(anomaly_threshold, 6),
    )

    print("\nML anomaly scoring:")
    print("Scored observations:", len(anomaly_scores))
    print(
        "Missing anomaly scores:",
        int(np.isnan(anomaly_scores).sum()),
    )

    print("\nFuture anomaly-score distribution:")

    for quantile in [0.50, 0.90, 0.95, 0.99, 0.999]:
        value = np.quantile(
            anomaly_scores,
            quantile,
        )

        print(
            f"{quantile:.3f}:",
            round(float(value), 6),
        )

    maximum_score_position = int(np.argmax(anomaly_scores))
    maximum_score = anomaly_scores[maximum_score_position]

    maximum_score_index = (
        scoring_start_index + maximum_score_position
    )

    print(
        "Maximum anomaly score:",
        round(float(maximum_score), 6),
    )
    print(
        "Timestamp of maximum anomaly score:",
        detector_data.iloc[maximum_score_index, 0],
    )

    anomaly_flags = flag_ml_anomalies(
        anomaly_scores,
        anomaly_threshold,
    )

    anomaly_count = int(anomaly_flags.sum())
    anomaly_rate = anomaly_count / len(anomaly_flags)

    anomaly_positions = np.flatnonzero(anomaly_flags)

    print("\nFinal ML anomaly rule:")
    print(
        "Fixed historical threshold:",
        round(anomaly_threshold, 6),
    )
    print(
        "Valid timestamps evaluated:",
        len(anomaly_flags),
    )
    print(
        "Anomalous timestamps:",
        anomaly_count,
    )
    print(
        "Anomaly rate:",
        f"{anomaly_rate:.4%}",
    )

    if anomaly_positions.size > 0:
        first_anomaly_index = (
            scoring_start_index + anomaly_positions[0]
        )

        last_anomaly_index = (
            scoring_start_index + anomaly_positions[-1]
        )

        print(
            "First ML anomaly timestamp:",
            detector_data.iloc[first_anomaly_index, 0],
        )
        print(
            "Last ML anomaly timestamp:",
            detector_data.iloc[last_anomaly_index, 0],
        )
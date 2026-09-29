"""Evaluate anomaly detectors against BattLeDIM leakage ground truth."""

import pandas as pd

from src.data.battledim_loader import load_leakage_events
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


def select_evaluation_leakages(events, evaluation_start, evaluation_end):
    """Return leakage events overlapping the evaluation period."""

    events = events.loc[
        (events["start_time"] <= evaluation_end)
        & (events["end_time"] >= evaluation_start)
    ].copy()

    events["evaluation_start"] = events["start_time"].clip(
        lower=evaluation_start
    )
    events["evaluation_end"] = events["end_time"].clip(
        upper=evaluation_end
    )

    return events.reset_index(drop=True)


def build_baseline_evaluation_results(detector_data):
    """Run the finalized baseline detector."""

    scored = calculate_baseline_scores(detector_data)
    evidence = calculate_network_evidence(scored)

    return flag_network_anomalies(evidence)


def build_ml_evaluation_results(detector_data):
    """Run Isolation Forest using historical-only threshold calibration."""

    ml_features, _ = select_ml_features(detector_data)

    training_features, scoring_features = (
        split_training_and_scoring_data(ml_features)
    )

    model = fit_isolation_forest(training_features)

    training_scores = calculate_anomaly_scores(
        model,
        training_features,
    )
    threshold = calculate_anomaly_threshold(training_scores)

    scoring_scores = calculate_anomaly_scores(
        model,
        scoring_features,
    )
    anomaly_flags = flag_ml_anomalies(
        scoring_scores,
        threshold,
    )

    timestamps = (
        detector_data.iloc[TRAINING_PERIOD:, 0]
        .reset_index(drop=True)
    )

    results = pd.DataFrame(
        {
            "timestamp": timestamps,
            "ml_anomaly_score": scoring_scores,
            "is_ml_anomaly": anomaly_flags,
        }
    )

    return results, threshold


def evaluate_detector_by_event(
    detector_results,
    leakage_events,
    anomaly_column,
    detector_name,
):
    """Evaluate the first detector alert within each leakage window."""

    event_results = []

    for _, event in leakage_events.iterrows():
        start = event["evaluation_start"]
        end = event["evaluation_end"]

        window = detector_results.loc[
            (detector_results["timestamp"] >= start)
            & (detector_results["timestamp"] <= end)
        ]

        anomalies = window.loc[
            window[anomaly_column].fillna(False)
        ]

        detected = not anomalies.empty

        if detected:
            first_detection = anomalies.iloc[0]["timestamp"]
            delay_hours = (
                first_detection - start
            ).total_seconds() / 3600
        else:
            first_detection = pd.NaT
            delay_hours = None

        event_results.append(
            {
                "link_id": event["link_id"],
                "leak_type": event["leak_type"],
                "detector": detector_name,
                "detected": detected,
                "first_detection": first_detection,
                "detection_delay_hours": delay_hours,
            }
        )

    return pd.DataFrame(event_results)


def evaluate_onset_windows(event_results):
    """Evaluate detection within 24 and 72 hours of leakage onset."""

    results = event_results.copy()

    results["detected_within_24h"] = (
        results["detected"]
        & (results["detection_delay_hours"] <= 24)
    )

    results["detected_within_72h"] = (
        results["detected"]
        & (results["detection_delay_hours"] <= 72)
    )

    return results


def summarize_detection_delays(event_results):
    """Summarize first-alert delay for detected leakage events."""

    delays = event_results.loc[
        event_results["detected"],
        "detection_delay_hours",
    ]

    return {
        "mean_hours": delays.mean(),
        "median_hours": delays.median(),
        "min_hours": delays.min(),
        "max_hours": delays.max(),
    }


def compare_detector_alerts(baseline_results, ml_results):
    """Compare timestamp-level alerts produced by both detectors."""

    comparison = baseline_results[
        ["timestamp", "is_baseline_anomaly"]
    ].merge(
        ml_results[
            ["timestamp", "is_ml_anomaly"]
        ],
        on="timestamp",
        how="inner",
        validate="one_to_one",
    )

    baseline = comparison["is_baseline_anomaly"].astype(bool)
    ml = comparison["is_ml_anomaly"].astype(bool)

    summary = {
        "both": int((baseline & ml).sum()),
        "baseline_only": int((baseline & ~ml).sum()),
        "ml_only": int((~baseline & ml).sum()),
        "neither": int((~baseline & ~ml).sum()),
    }

    return comparison, summary


if __name__ == "__main__":
    detector_data = build_detector_dataset()

    evaluation_start = detector_data.iloc[TRAINING_PERIOD, 0]
    evaluation_end = detector_data.iloc[-1, 0]

    leakage_events = select_evaluation_leakages(
        load_leakage_events(),
        evaluation_start,
        evaluation_end,
    )

    # ------------------------------------------------------------------
    # Run detectors
    # ------------------------------------------------------------------

    ml_results, ml_threshold = build_ml_evaluation_results(
        detector_data
    )

    baseline_results = build_baseline_evaluation_results(
        detector_data
    )

    baseline_evaluation = (
        baseline_results.iloc[TRAINING_PERIOD:][
            [
                baseline_results.columns[0],
                "anomalous_sensor_count",
                "anomalous_sensor_fraction",
                "is_network_anomaly",
            ]
        ]
        .copy()
        .reset_index(drop=True)
        .rename(
            columns={
                baseline_results.columns[0]: "timestamp",
                "is_network_anomaly": "is_baseline_anomaly",
            }
        )
    )

    # ------------------------------------------------------------------
    # Detector summary
    # ------------------------------------------------------------------

    print("Ground-truth evaluation period:")
    print("Start:", evaluation_start)
    print("End:", evaluation_end)
    print("Leakage events:", len(leakage_events))

    print("\nBaseline:")
    print("Observations:", len(baseline_evaluation))
    print(
        "Anomalies:",
        int(baseline_evaluation["is_baseline_anomaly"].sum()),
    )
    print(
        "Anomaly rate:",
        f"{baseline_evaluation['is_baseline_anomaly'].mean():.4%}",
    )

    print("\nIsolation Forest:")
    print("Observations:", len(ml_results))
    print("Threshold:", round(ml_threshold, 6))
    print(
        "Anomalies:",
        int(ml_results["is_ml_anomaly"].sum()),
    )
    print(
        "Anomaly rate:",
        f"{ml_results['is_ml_anomaly'].mean():.4%}",
    )

    # ------------------------------------------------------------------
    # Event-level evaluation
    # ------------------------------------------------------------------

    baseline_events = evaluate_detector_by_event(
        baseline_evaluation,
        leakage_events,
        "is_baseline_anomaly",
        "baseline",
    )

    ml_events = evaluate_detector_by_event(
        ml_results,
        leakage_events,
        "is_ml_anomaly",
        "isolation_forest",
    )

    baseline_events = evaluate_onset_windows(baseline_events)
    ml_events = evaluate_onset_windows(ml_events)

    event_columns = [
        "link_id",
        "leak_type",
        "detection_delay_hours",
        "detected_within_24h",
        "detected_within_72h",
    ]

    print("\nBaseline onset-window evaluation:")
    print(
        baseline_events[event_columns].to_string(index=False)
    )

    print("\nIsolation Forest onset-window evaluation:")
    print(
        ml_events[event_columns].to_string(index=False)
    )

    # ------------------------------------------------------------------
    # Onset-window summary
    # ------------------------------------------------------------------

    total_events = len(leakage_events)

    print("\nOnset-window detection summary:")
    print(
        "Baseline within 24h:",
        int(baseline_events["detected_within_24h"].sum()),
        "/",
        total_events,
    )
    print(
        "Baseline within 72h:",
        int(baseline_events["detected_within_72h"].sum()),
        "/",
        total_events,
    )
    print(
        "Isolation Forest within 24h:",
        int(ml_events["detected_within_24h"].sum()),
        "/",
        total_events,
    )
    print(
        "Isolation Forest within 72h:",
        int(ml_events["detected_within_72h"].sum()),
        "/",
        total_events,
    )

    # ------------------------------------------------------------------
    # Detection by leakage type
    # ------------------------------------------------------------------

    onset_columns = [
        "detected_within_24h",
        "detected_within_72h",
    ]

    print("\nBaseline by leakage type:")
    print(
        baseline_events.groupby("leak_type")[
            onset_columns
        ].sum()
    )

    print("\nIsolation Forest by leakage type:")
    print(
        ml_events.groupby("leak_type")[
            onset_columns
        ].sum()
    )

    # ------------------------------------------------------------------
    # Detection-delay summary
    # ------------------------------------------------------------------

    baseline_delays = summarize_detection_delays(
        baseline_events
    )
    ml_delays = summarize_detection_delays(
        ml_events
    )

    print("\nDetection-delay summary (hours):")
    print(
        "Baseline:",
        {
            key: round(value, 2)
            for key, value in baseline_delays.items()
        },
    )
    print(
        "Isolation Forest:",
        {
            key: round(value, 2)
            for key, value in ml_delays.items()
        },
    )

    # ------------------------------------------------------------------
    # Detector alert overlap
    # ------------------------------------------------------------------

    comparison, overlap = compare_detector_alerts(
        baseline_evaluation,
        ml_results,
    )

    print("\nDetector alert overlap:")
    print("Aligned timestamps:", len(comparison))
    print("Both detectors:", overlap["both"])
    print("Baseline only:", overlap["baseline_only"])
    print("Isolation Forest only:", overlap["ml_only"])
    print("Neither detector:", overlap["neither"])

    baseline_alerts = int(
        baseline_evaluation["is_baseline_anomaly"].sum()
    )
    ml_alerts = int(
        ml_results["is_ml_anomaly"].sum()
    )

    print("\nAlert overlap percentages:")
    print(
        "Baseline alerts also detected by Isolation Forest:",
        f"{overlap['both'] / baseline_alerts:.2%}",
    )
    print(
        "Isolation Forest alerts also detected by baseline:",
        f"{overlap['both'] / ml_alerts:.2%}",
    )
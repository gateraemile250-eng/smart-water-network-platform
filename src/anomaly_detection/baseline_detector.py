import pandas as pd

from src.anomaly_detection.feature_engineering import (
    add_change_features,
    add_hourly_change_features,
    add_temporal_features,
    add_weekly_deviation_features,
    build_scada_feature_table,
    prepare_detector_features,
)


CALIBRATION_PERIOD = 12 * 24 * 7  # 7 days of 5-minute observations
SENSOR_Z_THRESHOLD = 3.0
TOTAL_SENSORS = 119
NETWORK_SENSOR_THRESHOLD = 15


def build_detector_dataset():
    """Build the detector-ready feature dataset."""

    features = build_scada_feature_table()
    features = add_temporal_features(features)
    features = add_change_features(features)
    features = add_hourly_change_features(features)
    features = add_weekly_deviation_features(features)

    return prepare_detector_features(features)


def calculate_baseline_scores(df):
    """Calculate causal z-scores for weekly sensor deviations."""

    weekly_columns = [
        column
        for column in df.columns
        if column.endswith("__weekly_dev")
    ]

    weekly_data = df[weekly_columns]

    # Use only past observations and require 7 days of calibration.
    historical_mean = (
        weekly_data
        .expanding(min_periods=CALIBRATION_PERIOD)
        .mean()
        .shift(1)
    )

    historical_std = (
        weekly_data
        .expanding(min_periods=CALIBRATION_PERIOD)
        .std()
        .shift(1)
    )

    z_scores = (weekly_data - historical_mean) / historical_std

    z_scores.columns = [
        column.replace("__weekly_dev", "__weekly_z")
        for column in weekly_columns
    ]

    return pd.concat([df, z_scores], axis=1)


def calculate_network_evidence(df):
    """Count sensors whose absolute z-score exceeds the sensor threshold."""

    z_columns = [
        column
        for column in df.columns
        if column.endswith("__weekly_z")
    ]

    valid_rows = df[z_columns].notna().all(axis=1)

    sensor_flags = (
        df.loc[valid_rows, z_columns].abs()
        >= SENSOR_Z_THRESHOLD
    )

    anomalous_counts = sensor_flags.sum(axis=1)

    evidence = pd.DataFrame(index=df.index, dtype=float)

    evidence.loc[
        valid_rows,
        "anomalous_sensor_count",
    ] = anomalous_counts

    evidence.loc[
        valid_rows,
        "anomalous_sensor_fraction",
    ] = anomalous_counts / TOTAL_SENSORS

    return pd.concat([df, evidence], axis=1)


def flag_network_anomalies(df):
    """Flag timestamps with coordinated abnormal sensor behaviour."""

    result = df.copy()

    # Nullable Boolean allows True, False, and <NA>.
    result["is_network_anomaly"] = (
        result["anomalous_sensor_count"]
        >= NETWORK_SENSOR_THRESHOLD
    ).astype("boolean")

    result.loc[
        result["anomalous_sensor_count"].isna(),
        "is_network_anomaly",
    ] = pd.NA

    return result


if __name__ == "__main__":
    detector_data = build_detector_dataset()
    scored_data = calculate_baseline_scores(detector_data)
    evidence_data = calculate_network_evidence(scored_data)
    flagged_data = flag_network_anomalies(evidence_data)

    z_columns = [
        column
        for column in scored_data.columns
        if column.endswith("__weekly_z")
    ]

    fully_scored_rows = scored_data[z_columns].notna().all(axis=1)

    print("Baseline detector dataset loaded successfully.")
    print("Shape:", detector_data.shape)
    print("Missing values:", detector_data.isna().sum().sum())
    print("First timestamp:", detector_data.iloc[0, 0])
    print("Last timestamp:", detector_data.iloc[-1, 0])

    print("\nCalibration:")
    print("Required historical observations:", CALIBRATION_PERIOD)
    print("Calibration duration: 7 days")

    print("\nBaseline scoring:")
    print("Scored shape:", scored_data.shape)
    print("Weekly z-score columns:", len(z_columns))
    print(
        "Z-score missing values:",
        scored_data[z_columns].isna().sum().sum(),
    )
    print(
        "Rows with all z-scores available:",
        fully_scored_rows.sum(),
    )

    if fully_scored_rows.any():
        valid_scores = scored_data.loc[
            fully_scored_rows,
            z_columns,
        ]
        absolute_scores = valid_scores.abs()

        print(
            "First fully scored timestamp:",
            scored_data.loc[fully_scored_rows].iloc[0, 0],
        )

        print("\nAbsolute z-score distribution:")
        print(
            absolute_scores.stack()
            .quantile([0.50, 0.90, 0.95, 0.99, 0.999])
            .round(3)
        )

        print(
            "Maximum absolute z-score:",
            round(absolute_scores.max().max(), 3),
        )

        max_score_index, max_score_sensor = (
            absolute_scores.stack().idxmax()
        )

        print("\nMaximum-score inspection:")
        print(
            "Timestamp:",
            scored_data.loc[max_score_index].iloc[0],
        )
        print(
            "Sensor score column:",
            max_score_sensor,
        )
        print(
            "Absolute z-score:",
            round(
                absolute_scores.loc[
                    max_score_index,
                    max_score_sensor,
                ],
                3,
            ),
        )

        valid_evidence = evidence_data.loc[
            fully_scored_rows,
            "anomalous_sensor_count",
        ]

        print("\nNetwork-level evidence:")
        print("Sensor z-score threshold:", SENSOR_Z_THRESHOLD)
        print("Anomalous-sensor count distribution:")
        print(
            valid_evidence
            .quantile([0.50, 0.90, 0.95, 0.99, 0.999])
            .round(2)
        )

        max_evidence_index = valid_evidence.idxmax()

        print(
            "Maximum simultaneous anomalous sensors:",
            int(valid_evidence.max()),
        )
        print(
            "Timestamp of maximum network evidence:",
            evidence_data.loc[max_evidence_index].iloc[0],
        )
        print(
            "Maximum anomalous-sensor fraction:",
            round(
                evidence_data.loc[
                    max_evidence_index,
                    "anomalous_sensor_fraction",
                ],
                4,
            ),
        )

        valid_flags = flagged_data.loc[
            fully_scored_rows,
            "is_network_anomaly",
        ]

        anomaly_count = int(valid_flags.sum())
        evaluated_count = valid_flags.notna().sum()
        anomaly_rate = anomaly_count / evaluated_count

        print("\nFinal baseline anomaly rule:")
        print(
            "Network sensor threshold:",
            NETWORK_SENSOR_THRESHOLD,
        )
        print(
            "Valid timestamps evaluated:",
            evaluated_count,
        )
        print(
            "Anomalous timestamps:",
            anomaly_count,
        )
        print(
            "Anomaly rate:",
            f"{anomaly_rate:.4%}",
        )

        anomaly_rows = flagged_data.loc[
            flagged_data["is_network_anomaly"].fillna(False)
        ]

        if not anomaly_rows.empty:
            print(
                "First anomaly timestamp:",
                anomaly_rows.iloc[0, 0],
            )
            print(
                "Last anomaly timestamp:",
                anomaly_rows.iloc[-1, 0],
            )
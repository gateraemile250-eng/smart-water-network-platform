import pandas as pd

from src.anomaly_detection.baseline_detector import (
    SENSOR_Z_THRESHOLD,
    build_detector_dataset,
    calculate_baseline_scores,
)


# ---------------------------------------------------------------------------
# Sensor metadata
# ---------------------------------------------------------------------------

def extract_sensor_metadata(z_score_column):
    """
    Extract sensor type and sensor ID from a weekly z-score column.

    Expected formats:
        pressure__n54__weekly_z
        flow__p227__weekly_z
        demand__n1__weekly_z
        level__T1__weekly_z
    """

    suffix = "__weekly_z"

    if not z_score_column.endswith(suffix):
        raise ValueError(
            f"Invalid sensor z-score column: {z_score_column}"
        )

    sensor_reference = z_score_column.removesuffix(suffix)

    sensor_type, sensor_id = sensor_reference.split(
        "__",
        maxsplit=1,
    )

    return sensor_type, sensor_id


# ---------------------------------------------------------------------------
# Sensor evidence construction
# ---------------------------------------------------------------------------

def build_sensor_evidence_table(scored_data):
    """
    Convert wide sensor z-scores into a long-form evidence table.

    Each row represents one sensor at one timestamp.

    Leakage ground truth is intentionally excluded from the evidence
    calculation so that it can later be used independently for evaluation.
    """

    timestamp_column = scored_data.columns[0]

    z_columns = [
        column
        for column in scored_data.columns
        if column.endswith("__weekly_z")
    ]

    evidence = scored_data[
        [timestamp_column] + z_columns
    ].melt(
        id_vars=timestamp_column,
        value_vars=z_columns,
        var_name="z_score_column",
        value_name="z_score",
    )

    metadata = evidence["z_score_column"].apply(
        extract_sensor_metadata
    )

    evidence["sensor_type"] = metadata.str[0]
    evidence["sensor_id"] = metadata.str[1]

    evidence["absolute_z_score"] = evidence["z_score"].abs()

    evidence["is_sensor_anomaly"] = (
        evidence["absolute_z_score"] >= SENSOR_Z_THRESHOLD
    ).astype("boolean")

    evidence = evidence[
        [
            timestamp_column,
            "sensor_type",
            "sensor_id",
            "z_score",
            "absolute_z_score",
            "is_sensor_anomaly",
        ]
    ]

    return evidence


def select_valid_sensor_evidence(evidence):
    """
    Remove calibration rows where causal sensor z-scores
    are not yet available.
    """

    return (
        evidence.dropna(
            subset=[
                "z_score",
                "absolute_z_score",
                "is_sensor_anomaly",
            ]
        )
        .reset_index(drop=True)
    )


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    detector_data = build_detector_dataset()

    scored_data = calculate_baseline_scores(
        detector_data
    )

    sensor_evidence = build_sensor_evidence_table(
        scored_data
    )

    valid_evidence = select_valid_sensor_evidence(
        sensor_evidence
    )

    timestamp_column = valid_evidence.columns[0]

    print("Sensor evidence table built successfully.")

    # -----------------------------------------------------------------------
    # Full evidence table
    # -----------------------------------------------------------------------

    print("\nFull evidence table:")
    print("Shape:", sensor_evidence.shape)

    print(
        "Missing z-scores:",
        sensor_evidence["z_score"].isna().sum(),
    )

    # -----------------------------------------------------------------------
    # Valid evidence table
    # -----------------------------------------------------------------------

    print("\nValid evidence table:")
    print("Shape:", valid_evidence.shape)

    print(
        "Unique timestamps:",
        valid_evidence[timestamp_column].nunique(),
    )

    print(
        "Unique sensor references:",
        valid_evidence[
            ["sensor_type", "sensor_id"]
        ].drop_duplicates().shape[0],
    )

    print(
        "First valid timestamp:",
        valid_evidence[timestamp_column].min(),
    )

    print(
        "Last valid timestamp:",
        valid_evidence[timestamp_column].max(),
    )

    # -----------------------------------------------------------------------
    # Sensor-type coverage
    # -----------------------------------------------------------------------

    print("\nSensor types:")

    print(
        valid_evidence["sensor_type"]
        .value_counts()
        .sort_index()
    )

    # -----------------------------------------------------------------------
    # Sensor anomaly evidence
    # -----------------------------------------------------------------------

    print("\nSensor anomaly evidence:")
    print(
        "Sensor z-score threshold:",
        SENSOR_Z_THRESHOLD,
    )

    print(
        "Anomalous sensor observations:",
        int(
            valid_evidence[
                "is_sensor_anomaly"
            ].sum()
        ),
    )

    # -----------------------------------------------------------------------
    # Evidence-strength distribution
    # -----------------------------------------------------------------------

    print("\nAbsolute z-score distribution:")

    print(
        valid_evidence["absolute_z_score"]
        .quantile(
            [0.50, 0.90, 0.95, 0.99, 0.999]
        )
        .round(3)
    )

    # -----------------------------------------------------------------------
    # Strongest evidence across the evaluation period
    # -----------------------------------------------------------------------

    strongest_evidence = (
        valid_evidence
        .sort_values(
            "absolute_z_score",
            ascending=False,
        )
        .head(10)
    )

    print("\nStrongest sensor evidence:")

    print(
        strongest_evidence.to_string(
            index=False
        )
    )

    # -----------------------------------------------------------------------
    # Diagnostic inspection at a known evaluation timestamp
    #
    # This timestamp corresponds to the configured onset of the p673
    # leakage event. It is used only to inspect/evaluate the already
    # calculated sensor evidence. Leakage information is not used to
    # calculate the evidence itself.
    # -----------------------------------------------------------------------

    inspection_timestamp = pd.Timestamp(
        "2018-03-05 15:45:00"
    )

    all_onset_evidence = valid_evidence.loc[
        valid_evidence[timestamp_column]
        == inspection_timestamp
    ].copy()

    onset_evidence = (
        all_onset_evidence
        .sort_values(
            "absolute_z_score",
            ascending=False,
        )
        .head(20)
    )

    print(
        "\nTop sensor evidence at "
        f"{inspection_timestamp}:"
    )

    print(
        onset_evidence.to_string(
            index=False
        )
    )

    print(
        "\nTotal sensors evaluated at inspection timestamp:",
        len(all_onset_evidence),
    )

    print(
        "Total anomalous sensors at inspection timestamp:",
        int(
            all_onset_evidence[
                "is_sensor_anomaly"
            ].sum()
        ),
    )
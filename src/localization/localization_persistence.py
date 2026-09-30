"""Persist ranked network-localization candidates to PostgreSQL."""

import pandas as pd

from src.anomaly_detection.baseline_detector import (
    build_detector_dataset,
    calculate_baseline_scores,
)
from src.data.database import get_database_connection
from src.localization.localization_engine import (
    build_anchor_distance_lookup,
    build_localization_context,
    localize_timestamp,
)
from src.localization.sensor_evidence import (
    build_sensor_evidence_table,
    select_valid_sensor_evidence,
)


TOP_CANDIDATES = 25


UPSERT_SQL = """
INSERT INTO network_localization_results (
    timestamp,
    pipe_id,
    localization_score,
    candidate_rank
)
VALUES (%s, %s, %s, %s)
ON CONFLICT (timestamp, pipe_id)
DO UPDATE SET
    localization_score = EXCLUDED.localization_score,
    candidate_rank = EXCLUDED.candidate_rank;
"""


DELETE_TIMESTAMP_SQL = """
DELETE FROM network_localization_results
WHERE timestamp = %s;
"""


ML_ANOMALY_TIMESTAMPS_SQL = """
SELECT timestamp
FROM network_anomaly_results
WHERE is_ml_anomaly = TRUE
ORDER BY timestamp;
"""


def load_ml_anomaly_timestamps(connection):
    """Load timestamps flagged by the finalized Isolation Forest."""

    cursor = connection.cursor()

    try:
        cursor.execute(ML_ANOMALY_TIMESTAMPS_SQL)
        rows = cursor.fetchall()
    finally:
        cursor.close()

    return pd.DatetimeIndex(row[0] for row in rows)


def build_localization_evidence(anomaly_timestamps):
    """
    Build causal sensor evidence only for ML-anomaly timestamps.

    Ground-truth leakage information is not used.
    """

    detector_data = build_detector_dataset()
    timestamp_column = detector_data.columns[0]

    if not detector_data[timestamp_column].isin(
        anomaly_timestamps
    ).any():
        return pd.DataFrame()

    scored_data = calculate_baseline_scores(detector_data)

    selected_scored_data = scored_data.loc[
        scored_data[timestamp_column].isin(
            anomaly_timestamps
        )
    ].copy()

    evidence = build_sensor_evidence_table(
        selected_scored_data
    )

    return select_valid_sensor_evidence(evidence)


def build_localization_results(
    evidence,
    anomaly_timestamps,
    graph,
    candidate_pipes,
    sensor_anchors,
    distance_lookup,
    top_candidates=TOP_CANDIDATES,
):
    """
    Localize ML anomalies and retain the ranked candidate shortlist.

    Timestamps without qualifying sensor evidence produce no rows.
    """

    result_frames = []

    if evidence.empty:
        return pd.DataFrame(
            columns=[
                "timestamp",
                "pipe_id",
                "localization_score",
                "candidate_rank",
            ]
        )

    timestamp_column = evidence.columns[0]

    for timestamp in anomaly_timestamps:
        timestamp = pd.Timestamp(timestamp)

        timestamp_mask = (
            evidence[timestamp_column] == timestamp
        )

        if not timestamp_mask.any():
            continue

        ranked_pipes = localize_timestamp(
            evidence=evidence,
            timestamp=timestamp,
            graph=graph,
            candidate_pipes=candidate_pipes,
            sensor_anchors=sensor_anchors,
            distance_lookup=distance_lookup,
        )

        if ranked_pipes.empty:
            continue

        shortlist = ranked_pipes.head(
            top_candidates
        ).copy()

        shortlist.insert(
            0,
            "timestamp",
            timestamp,
        )

        shortlist = shortlist[
            [
                "timestamp",
                "pipe_id",
                "localization_score",
                "rank",
            ]
        ].rename(
            columns={
                "rank": "candidate_rank",
            }
        )

        result_frames.append(shortlist)

    if not result_frames:
        return pd.DataFrame(
            columns=[
                "timestamp",
                "pipe_id",
                "localization_score",
                "candidate_rank",
            ]
        )

    return pd.concat(
        result_frames,
        ignore_index=True,
    )


def replace_localization_results(
    connection,
    results,
    processed_timestamps,
):
    """
    Replace localization results for every timestamp processed.

    Existing rows are deleted even when a timestamp currently produces
    no candidates, preventing stale localization results from surviving.
    """

    timestamps = (
        pd.DatetimeIndex(processed_timestamps)
        .drop_duplicates()
    )

    cursor = connection.cursor()

    try:
        if len(timestamps):
            cursor.executemany(
                DELETE_TIMESTAMP_SQL,
                [
                    (
                        pd.Timestamp(
                            timestamp
                        ).to_pydatetime(),
                    )
                    for timestamp in timestamps
                ],
            )

        if not results.empty:
            rows = [
                (
                    pd.Timestamp(
                        row.timestamp
                    ).to_pydatetime(),
                    str(row.pipe_id),
                    float(row.localization_score),
                    int(row.candidate_rank),
                )
                for row in results.itertuples(
                    index=False
                )
            ]

            cursor.executemany(
                UPSERT_SQL,
                rows,
            )

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        cursor.close()


def validate_localization_results(
    results,
    top_candidates=TOP_CANDIDATES,
):
    """Validate localization candidates before persistence."""

    if results.empty:
        return

    required_columns = [
        "timestamp",
        "pipe_id",
        "localization_score",
        "candidate_rank",
    ]

    if results[required_columns].isna().any().any():
        raise ValueError(
            "Localization results contain missing values."
        )

    if results.duplicated(
        subset=["timestamp", "pipe_id"]
    ).any():
        raise ValueError(
            "Localization results contain duplicate "
            "timestamp/pipe combinations."
        )

    if (results["localization_score"] < 0).any():
        raise ValueError(
            "Localization scores must be non-negative."
        )

    if (results["candidate_rank"] < 1).any():
        raise ValueError(
            "Candidate ranks must be positive."
        )

    if (
        results["candidate_rank"] > top_candidates
    ).any():
        raise ValueError(
            "Candidate rank exceeds configured shortlist size."
        )

    rank_counts = results.groupby(
        "timestamp"
    )["candidate_rank"].nunique()

    row_counts = results.groupby(
        "timestamp"
    ).size()

    if not rank_counts.equals(row_counts):
        raise ValueError(
            "Candidate ranks are not unique within timestamps."
        )


if __name__ == "__main__":
    connection = get_database_connection()

    try:
        anomaly_timestamps = (
            load_ml_anomaly_timestamps(
                connection
            )
        )

        print(
            "ML anomaly timestamps:",
            len(anomaly_timestamps),
        )

        (
            graph,
            candidate_pipes,
            sensor_anchors,
        ) = build_localization_context(
            connection
        )

        distance_lookup = (
            build_anchor_distance_lookup(
                graph,
                sensor_anchors,
            )
        )

        evidence = build_localization_evidence(
            anomaly_timestamps
        )

        print(
            "Sensor evidence rows:",
            len(evidence),
        )

        print(
            "Evidence timestamps:",
            (
                evidence[
                    evidence.columns[0]
                ].nunique()
                if not evidence.empty
                else 0
            ),
        )

        localization_results = (
            build_localization_results(
                evidence=evidence,
                anomaly_timestamps=anomaly_timestamps,
                graph=graph,
                candidate_pipes=candidate_pipes,
                sensor_anchors=sensor_anchors,
                distance_lookup=distance_lookup,
            )
        )

        validate_localization_results(
            localization_results
        )

        localized_timestamps = (
            localization_results[
                "timestamp"
            ].nunique()
            if not localization_results.empty
            else 0
        )

        print(
            "Localized timestamps:",
            localized_timestamps,
        )
        print(
            "Candidate rows prepared:",
            len(localization_results),
        )
        print(
            "Shortlist size:",
            TOP_CANDIDATES,
        )

        print(
            "\nWriting localization results "
            "to PostgreSQL..."
        )

        replace_localization_results(
            connection,
            localization_results,
            anomaly_timestamps,
        )

        print(
            "Localization results "
            "persisted successfully."
        )

    finally:
        connection.close()
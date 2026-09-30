"""Evaluate network localization against BattLeDIM leakage ground truth."""

import pandas as pd

from src.anomaly_detection.baseline_detector import (
    build_detector_dataset,
    calculate_baseline_scores,
)
from src.anomaly_detection.evaluation import (
    build_ml_evaluation_results,
    evaluate_detector_by_event,
    select_evaluation_leakages,
)
from src.anomaly_detection.isolation_forest_detector import (
    TRAINING_PERIOD,
)
from src.data.battledim_loader import (
    load_leakage_events,
)
from src.data.database import (
    get_database_connection,
)
from src.localization.localization_engine import (
    build_anchor_distance_lookup,
    build_localization_context,
    localize_timestamp,
)
from src.localization.network_topology import (
    shortest_hop_distance,
)
from src.localization.sensor_evidence import (
    build_sensor_evidence_table,
    select_valid_sensor_evidence,
)


TOP_K_VALUES = (
    1,
    5,
    10,
    25,
    50,
)

NEIGHBORHOOD_HOPS = (
    0,
    1,
    2,
    5,
)


def select_event_alert_timestamps(
    ml_results,
    leakage_events,
):
    """
    Select the first Isolation Forest alert inside each
    leakage event's observable evaluation window.

    Ground truth is used here only to define evaluation windows.
    It is not used by the detector or localization method.
    """

    return evaluate_detector_by_event(
        detector_results=ml_results,
        leakage_events=leakage_events,
        anomaly_column="is_ml_anomaly",
        detector_name="isolation_forest",
    )


def build_selected_sensor_evidence(
    scored_data,
    event_alerts,
):
    """
    Build sensor evidence only for timestamps selected for
    localization evaluation.

    This avoids constructing the full multi-million-row
    long-form evidence table.
    """

    selected_timestamps = (
        event_alerts.loc[
            event_alerts["detected"],
            "first_detection",
        ]
        .dropna()
        .drop_duplicates()
    )

    timestamp_column = scored_data.columns[0]

    selected_scored_data = scored_data.loc[
        scored_data[timestamp_column].isin(
            selected_timestamps
        )
    ].copy()

    evidence = build_sensor_evidence_table(
        selected_scored_data
    )

    return select_valid_sensor_evidence(
        evidence
    )


def calculate_pipe_to_pipe_distance(
    graph,
    predicted_pipe,
    true_pipe,
):
    """
    Calculate the minimum topology distance between two pipes.

    Distance is defined as the minimum node-hop distance between
    either endpoint of the predicted pipe and either endpoint of
    the true pipe.

    Pipes sharing an endpoint therefore have distance 0.

    This metric is used only for evaluation. It does not influence
    localization scoring or candidate ranking.
    """

    predicted_nodes = (
        predicted_pipe["from_node_id"],
        predicted_pipe["to_node_id"],
    )

    true_nodes = (
        true_pipe["from_node_id"],
        true_pipe["to_node_id"],
    )

    distances = []

    for predicted_node in predicted_nodes:
        for true_node in true_nodes:
            distance = shortest_hop_distance(
                graph,
                predicted_node,
                true_node,
            )

            if distance is not None:
                distances.append(distance)

    if not distances:
        return None

    return min(distances)


def evaluate_localization_by_event(
    event_alerts,
    sensor_evidence,
    graph,
    candidate_pipes,
    sensor_anchors,
    distance_lookup,
):
    """
    Localize each detected event at its first Isolation Forest
    alert and compare the frozen ranking with BattLeDIM ground truth.

    Ground truth is used only after localization ranking has already
    been produced.
    """

    evaluation_rows = []

    for _, event in event_alerts.iterrows():
        true_pipe_id = event["link_id"]
        alert_timestamp = event["first_detection"]

        result = {
            "link_id": true_pipe_id,
            "leak_type": event["leak_type"],
            "first_detection": alert_timestamp,
            "detection_delay_hours": event[
                "detection_delay_hours"
            ],
            "localization_available": False,
            "true_pipe_rank": pd.NA,
            "true_pipe_score": pd.NA,
            "top_candidate_pipe": pd.NA,
            "top_candidate_score": pd.NA,
            "top_candidate_distance_hops": pd.NA,
        }

        for k in TOP_K_VALUES:
            result[f"top_{k}"] = False

        for hops in NEIGHBORHOOD_HOPS:
            result[
                f"top_candidate_within_{hops}_hops"
            ] = False

        if (
            not event["detected"]
            or pd.isna(alert_timestamp)
        ):
            evaluation_rows.append(result)
            continue

        ranked_pipes = localize_timestamp(
            evidence=sensor_evidence,
            timestamp=alert_timestamp,
            graph=graph,
            candidate_pipes=candidate_pipes,
            sensor_anchors=sensor_anchors,
            distance_lookup=distance_lookup,
        )

        if ranked_pipes.empty:
            evaluation_rows.append(result)
            continue

        true_pipe = candidate_pipes.loc[
            candidate_pipes["pipe_id"]
            == true_pipe_id
        ]

        if true_pipe.empty:
            evaluation_rows.append(result)
            continue

        true_pipe = true_pipe.iloc[0]

        ranked_true_pipe = ranked_pipes.loc[
            ranked_pipes["pipe_id"]
            == true_pipe_id
        ]

        if ranked_true_pipe.empty:
            evaluation_rows.append(result)
            continue

        ranked_true_pipe = ranked_true_pipe.iloc[0]

        top_candidate = ranked_pipes.iloc[0]

        true_rank = int(
            ranked_true_pipe["rank"]
        )

        pipe_distance = calculate_pipe_to_pipe_distance(
            graph=graph,
            predicted_pipe=top_candidate,
            true_pipe=true_pipe,
        )

        result[
            "localization_available"
        ] = True

        result[
            "true_pipe_rank"
        ] = true_rank

        result[
            "true_pipe_score"
        ] = float(
            ranked_true_pipe[
                "localization_score"
            ]
        )

        result[
            "top_candidate_pipe"
        ] = top_candidate["pipe_id"]

        result[
            "top_candidate_score"
        ] = float(
            top_candidate[
                "localization_score"
            ]
        )

        result[
            "top_candidate_distance_hops"
        ] = pipe_distance

        for k in TOP_K_VALUES:
            result[f"top_{k}"] = (
                true_rank <= k
            )

        if pipe_distance is not None:
            for hops in NEIGHBORHOOD_HOPS:
                result[
                    f"top_candidate_within_{hops}_hops"
                ] = (
                    pipe_distance <= hops
                )

        evaluation_rows.append(result)

    return pd.DataFrame(
        evaluation_rows
    )


def summarize_localization_results(
    evaluation_results,
):
    """
    Summarize exact-pipe ranking and network-neighborhood
    localization performance.

    Localization scores are not interpreted as probabilities.
    """

    total_events = len(
        evaluation_results
    )

    available = evaluation_results.loc[
        evaluation_results[
            "localization_available"
        ]
    ].copy()

    summary = {
        "total_events": total_events,
        "localization_available": len(
            available
        ),
    }

    for k in TOP_K_VALUES:
        summary[f"top_{k}_events"] = int(
            available[f"top_{k}"].sum()
        )

    for hops in NEIGHBORHOOD_HOPS:
        column = (
            f"top_candidate_within_{hops}_hops"
        )

        summary[
            f"top_candidate_within_{hops}_hops_events"
        ] = int(
            available[column].sum()
        )

    if available.empty:
        summary[
            "median_true_pipe_rank"
        ] = None

        summary[
            "mean_true_pipe_rank"
        ] = None

        summary[
            "median_top_candidate_distance_hops"
        ] = None

        summary[
            "mean_top_candidate_distance_hops"
        ] = None

    else:
        summary[
            "median_true_pipe_rank"
        ] = float(
            available[
                "true_pipe_rank"
            ].median()
        )

        summary[
            "mean_true_pipe_rank"
        ] = float(
            available[
                "true_pipe_rank"
            ].mean()
        )

        distances = (
            available[
                "top_candidate_distance_hops"
            ]
            .dropna()
            .astype(float)
        )

        if distances.empty:
            summary[
                "median_top_candidate_distance_hops"
            ] = None

            summary[
                "mean_top_candidate_distance_hops"
            ] = None

        else:
            summary[
                "median_top_candidate_distance_hops"
            ] = float(
                distances.median()
            )

            summary[
                "mean_top_candidate_distance_hops"
            ] = float(
                distances.mean()
            )

    return summary


if __name__ == "__main__":
    # ------------------------------------------------------------------
    # Build the same detector dataset used in Phase 8
    # ------------------------------------------------------------------

    detector_data = build_detector_dataset()

    evaluation_start = detector_data.iloc[
        TRAINING_PERIOD,
        0,
    ]

    evaluation_end = detector_data.iloc[
        -1,
        0,
    ]

    # ------------------------------------------------------------------
    # Load leakage ground truth for evaluation only
    # ------------------------------------------------------------------

    leakage_events = select_evaluation_leakages(
        load_leakage_events(),
        evaluation_start,
        evaluation_end,
    )

    # ------------------------------------------------------------------
    # Reproduce finalized Phase 8 Isolation Forest alerts
    # ------------------------------------------------------------------

    ml_results, ml_threshold = (
        build_ml_evaluation_results(
            detector_data
        )
    )

    event_alerts = (
        select_event_alert_timestamps(
            ml_results,
            leakage_events,
        )
    )

    # ------------------------------------------------------------------
    # Calculate causal sensor evidence
    # ------------------------------------------------------------------

    scored_data = calculate_baseline_scores(
        detector_data
    )

    sensor_evidence = (
        build_selected_sensor_evidence(
            scored_data,
            event_alerts,
        )
    )

    # ------------------------------------------------------------------
    # Build static localization context
    # ------------------------------------------------------------------

    connection = get_database_connection()

    try:
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

    finally:
        connection.close()

    # ------------------------------------------------------------------
    # Evaluate frozen localization method
    # ------------------------------------------------------------------

    localization_results = (
        evaluate_localization_by_event(
            event_alerts=event_alerts,
            sensor_evidence=sensor_evidence,
            graph=graph,
            candidate_pipes=candidate_pipes,
            sensor_anchors=sensor_anchors,
            distance_lookup=distance_lookup,
        )
    )

    summary = summarize_localization_results(
        localization_results
    )

    # ------------------------------------------------------------------
    # Report
    # ------------------------------------------------------------------

    print(
        "Localization ground-truth evaluation"
    )

    print(
        "\nEvaluation period:"
    )
    print(
        "Start:",
        evaluation_start,
    )
    print(
        "End:",
        evaluation_end,
    )

    print(
        "\nIsolation Forest:"
    )
    print(
        "Threshold:",
        round(
            ml_threshold,
            6,
        ),
    )
    print(
        "Leakage events:",
        len(
            leakage_events
        ),
    )
    print(
        "Detected events:",
        int(
            event_alerts[
                "detected"
            ].sum()
        ),
    )

    print(
        "\nSelected sensor evidence:"
    )
    print(
        "Rows:",
        len(
            sensor_evidence
        ),
    )
    print(
        "Unique timestamps:",
        sensor_evidence[
            sensor_evidence.columns[0]
        ].nunique(),
    )

    display_columns = [
        "link_id",
        "leak_type",
        "first_detection",
        "detection_delay_hours",
        "localization_available",
        "true_pipe_rank",
        "top_candidate_pipe",
        "top_candidate_distance_hops",
        "top_1",
        "top_5",
        "top_10",
        "top_25",
        "top_50",
    ]

    print(
        "\nEvent-level localization results:"
    )

    print(
        localization_results[
            display_columns
        ].to_string(
            index=False
        )
    )

    print(
        "\nLocalization summary:"
    )

    for key, value in summary.items():
        print(
            f"{key}: {value}"
        )
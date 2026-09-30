import pandas as pd

from src.data.database import get_database_connection
from src.localization.network_topology import (
    build_network_graph,
    calculate_all_hop_distances,
    load_network_links,
    load_sensor_assets,
)
from src.localization.pipe_localization import (
    build_candidate_pipes,
    calculate_sensor_contribution,
)
from src.localization.sensor_attribution import (
    build_sensor_anchor_table,
)


def build_localization_context(connection):
    """
    Load and prepare the static network information required
    for anomaly localization.
    """

    links = load_network_links(connection)
    sensors = load_sensor_assets(connection)

    graph = build_network_graph(links)
    candidate_pipes = build_candidate_pipes(links)
    sensor_anchors = build_sensor_anchor_table(
        sensors,
        links,
    )

    return (
        graph,
        candidate_pipes,
        sensor_anchors,
    )


def select_abnormal_sensor_evidence(
    evidence,
    z_threshold=3.0,
):
    """
    Retain sensor observations whose absolute causal z-score
    meets the established anomaly threshold.

    Ground-truth leakage information is not used.
    """

    return (
        evidence.loc[
            evidence["absolute_z_score"]
            >= z_threshold
        ]
        .copy()
        .sort_values(
            "absolute_z_score",
            ascending=False,
        )
        .reset_index(drop=True)
    )


def attach_sensor_anchors(
    abnormal_evidence,
    sensor_anchors,
):
    """
    Attach network topology anchors to abnormal sensor evidence.

    Node-mounted sensors have one anchor.
    Link-mounted sensors can have two anchors.
    """

    anchored_evidence = abnormal_evidence.merge(
        sensor_anchors[
            [
                "sensor_id",
                "sensor_type",
                "anchor_node_id",
            ]
        ],
        on=[
            "sensor_id",
            "sensor_type",
        ],
        how="left",
        validate="many_to_many",
    )

    missing_mask = anchored_evidence[
        "anchor_node_id"
    ].isna()

    if missing_mask.any():
        missing = (
            anchored_evidence.loc[
                missing_mask,
                [
                    "sensor_id",
                    "sensor_type",
                ],
            ]
            .drop_duplicates()
        )

        raise ValueError(
            "Some abnormal sensors have no topology anchor:\n"
            + missing.to_string(index=False)
        )

    return anchored_evidence


def build_anchor_distance_lookup(
    graph,
    sensor_anchors,
):
    """
    Precompute shortest-hop distances from every unique
    sensor anchor to all reachable network nodes.

    Each anchor requires only one breadth-first search.
    """

    anchor_nodes = (
        sensor_anchors["anchor_node_id"]
        .dropna()
        .drop_duplicates()
        .tolist()
    )

    return {
        anchor_node: calculate_all_hop_distances(
            graph,
            anchor_node,
        )
        for anchor_node in anchor_nodes
    }


def sensor_to_pipe_distance_from_lookup(
    distance_lookup,
    anchor_nodes,
    pipe_from_node,
    pipe_to_node,
):
    """
    Return the minimum topology distance from any sensor anchor
    to either endpoint of a candidate pipe.
    """

    distances = []

    for anchor_node in anchor_nodes:
        anchor_distances = distance_lookup.get(
            anchor_node,
            {},
        )

        for pipe_node in (
            pipe_from_node,
            pipe_to_node,
        ):
            distance = anchor_distances.get(
                pipe_node
            )

            if distance is not None:
                distances.append(distance)

    if not distances:
        return None

    return min(distances)


def score_candidate_pipes(
    graph,
    candidate_pipes,
    anchored_evidence,
    distance_lookup=None,
):
    """
    Score candidate pipes using abnormal sensor evidence.

    Each physical sensor contributes once to each candidate pipe.

    Link-mounted sensors use the minimum topology distance from
    either of their anchor nodes.

    Pipe score:
        sum(|z| / (1 + topology distance))

    The score is an interpretable localization heuristic,
    not a leakage probability.
    """

    if distance_lookup is None:
        unique_anchors = (
            anchored_evidence["anchor_node_id"]
            .dropna()
            .drop_duplicates()
            .tolist()
        )

        distance_lookup = {
            anchor_node: calculate_all_hop_distances(
                graph,
                anchor_node,
            )
            for anchor_node in unique_anchors
        }

    sensor_groups = []

    for (
        sensor_id,
        sensor_type,
    ), group in anchored_evidence.groupby(
        [
            "sensor_id",
            "sensor_type",
        ],
        sort=False,
    ):
        sensor_groups.append(
            {
                "sensor_id": sensor_id,
                "sensor_type": sensor_type,
                "absolute_z_score": group[
                    "absolute_z_score"
                ].iloc[0],
                "anchor_nodes": (
                    group["anchor_node_id"]
                    .drop_duplicates()
                    .tolist()
                ),
            }
        )

    scored_pipes = []

    for pipe in candidate_pipes.itertuples(
        index=False
    ):
        pipe_score = 0.0

        for sensor in sensor_groups:
            distance = (
                sensor_to_pipe_distance_from_lookup(
                    distance_lookup=distance_lookup,
                    anchor_nodes=sensor[
                        "anchor_nodes"
                    ],
                    pipe_from_node=pipe.from_node_id,
                    pipe_to_node=pipe.to_node_id,
                )
            )

            contribution = calculate_sensor_contribution(
                absolute_z_score=sensor[
                    "absolute_z_score"
                ],
                distance=distance,
            )

            pipe_score += contribution

        scored_pipes.append(
            {
                "pipe_id": pipe.pipe_id,
                "from_node_id": pipe.from_node_id,
                "to_node_id": pipe.to_node_id,
                "localization_score": pipe_score,
            }
        )

    scored_pipes = (
        pd.DataFrame(scored_pipes)
        .sort_values(
            [
                "localization_score",
                "pipe_id",
            ],
            ascending=[
                False,
                True,
            ],
        )
        .reset_index(drop=True)
    )

    scored_pipes["rank"] = (
        scored_pipes.index + 1
    )

    return scored_pipes


def localize_timestamp(
    evidence,
    timestamp,
    graph,
    candidate_pipes,
    sensor_anchors,
    z_threshold=3.0,
    distance_lookup=None,
):
    """
    Localize abnormal network behaviour at one timestamp.

    Ground-truth leakage information is not used.
    """

    timestamp_column = evidence.columns[0]
    target_timestamp = pd.Timestamp(timestamp)

    timestamp_evidence = evidence.loc[
        evidence[timestamp_column]
        == target_timestamp
    ].copy()

    if timestamp_evidence.empty:
        raise ValueError(
            f"No sensor evidence found for {target_timestamp}."
        )

    abnormal_evidence = select_abnormal_sensor_evidence(
        timestamp_evidence,
        z_threshold=z_threshold,
    )

    if abnormal_evidence.empty:
        return pd.DataFrame(
            columns=[
                "pipe_id",
                "from_node_id",
                "to_node_id",
                "localization_score",
                "rank",
            ]
        )

    anchored_evidence = attach_sensor_anchors(
        abnormal_evidence,
        sensor_anchors,
    )

    return score_candidate_pipes(
        graph=graph,
        candidate_pipes=candidate_pipes,
        anchored_evidence=anchored_evidence,
        distance_lookup=distance_lookup,
    )


if __name__ == "__main__":
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

        print(
            "Localization context built successfully."
        )

        print("\nNetwork:")
        print(
            "Graph nodes:",
            len(graph),
        )
        print(
            "Candidate pipes:",
            len(candidate_pipes),
        )

        print("\nSensor attribution:")
        print(
            "Anchor rows:",
            len(sensor_anchors),
        )
        print(
            "Unique sensor references:",
            sensor_anchors[
                [
                    "sensor_id",
                    "sensor_type",
                ]
            ]
            .drop_duplicates()
            .shape[0],
        )
        print(
            "Unique topology anchors:",
            sensor_anchors[
                "anchor_node_id"
            ].nunique(),
        )
        print(
            "Missing anchor nodes:",
            sensor_anchors[
                "anchor_node_id"
            ].isna().sum(),
        )

        print("\nDistance cache:")
        print(
            "Cached anchor nodes:",
            len(distance_lookup),
        )

    finally:
        connection.close()
from src.data.database import get_database_connection
from src.localization.network_topology import (
    load_network_links,
    shortest_hop_distance,
)


def build_candidate_pipes(links):
    """
    Build the candidate set used for leak localization.

    Only physical pipes are included as localization candidates.
    Pumps and valves remain part of the network topology but are
    not considered candidate leaking pipes in this method.
    """

    candidate_pipes = (
        links.loc[
            links["link_type"] == "pipe",
            [
                "link_id",
                "from_node_id",
                "to_node_id",
            ],
        ]
        .copy()
        .rename(
            columns={
                "link_id": "pipe_id",
            }
        )
        .sort_values("pipe_id")
        .reset_index(drop=True)
    )

    return candidate_pipes


def sensor_to_pipe_distance(
    graph,
    anchor_nodes,
    pipe_from_node,
    pipe_to_node,
):
    """
    Calculate the minimum topology distance from a sensor to a pipe.

    A node-mounted sensor has one topology anchor node.
    A link-mounted sensor can have two topology anchor nodes.

    The distance to a candidate pipe is the minimum hop distance
    between any sensor anchor and either endpoint of the pipe.

    Returns None when no candidate-pipe endpoint is reachable from
    any of the supplied sensor anchors.
    """

    distances = []

    for anchor_node in anchor_nodes:
        for pipe_node in (
            pipe_from_node,
            pipe_to_node,
        ):
            distance = shortest_hop_distance(
                graph,
                anchor_node,
                pipe_node,
            )

            if distance is not None:
                distances.append(distance)

    if not distances:
        return None

    return min(distances)


def calculate_sensor_contribution(
    absolute_z_score,
    distance,
):
    """
    Calculate one sensor's contribution to a candidate pipe.

    Sensor evidence strength is discounted by topology distance.

    Formula:
        contribution = |z| / (1 + distance)

    This is an interpretable localization heuristic. The resulting
    contribution is not a probability that the candidate pipe is
    leaking.
    """

    if distance is None:
        return 0.0

    if distance < 0:
        raise ValueError(
            "Topology distance cannot be negative."
        )

    return absolute_z_score / (1.0 + distance)


if __name__ == "__main__":
    connection = get_database_connection()

    try:
        links = load_network_links(connection)

        candidate_pipes = build_candidate_pipes(
            links
        )

        print(
            "Candidate pipe table built successfully."
        )

        # -------------------------------------------------------------------
        # Candidate-pipe validation
        # -------------------------------------------------------------------

        print("\nCandidate pipes:")

        print(
            "Rows:",
            len(candidate_pipes),
        )

        print(
            "Unique pipe IDs:",
            candidate_pipes["pipe_id"].nunique(),
        )

        print(
            "Missing pipe IDs:",
            candidate_pipes["pipe_id"].isna().sum(),
        )

        print(
            "Missing from-node IDs:",
            candidate_pipes["from_node_id"].isna().sum(),
        )

        print(
            "Missing to-node IDs:",
            candidate_pipes["to_node_id"].isna().sum(),
        )

        duplicate_pipes = (
            candidate_pipes["pipe_id"]
            .duplicated()
            .sum()
        )

        print(
            "Duplicate pipe IDs:",
            duplicate_pipes,
        )

        print("\nFirst candidate pipes:")

        print(
            candidate_pipes.head(10).to_string(
                index=False
            )
        )

        # -------------------------------------------------------------------
        # Contribution-function validation
        # -------------------------------------------------------------------

        print("\nSensor contribution examples:")

        example_z_score = 6.0

        for distance in [0, 1, 2, 5, 10]:
            contribution = calculate_sensor_contribution(
                example_z_score,
                distance,
            )

            print(
                f"|z|={example_z_score:.1f}, "
                f"distance={distance}: "
                f"contribution={contribution:.3f}"
            )

    finally:
        connection.close()
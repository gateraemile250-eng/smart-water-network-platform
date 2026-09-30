from collections import defaultdict, deque

import pandas as pd

from src.data.database import get_database_connection


def query_to_dataframe(connection, query):
    """Execute a query and return the result as a pandas DataFrame."""

    cursor = connection.cursor()

    try:
        cursor.execute(query)
        rows = cursor.fetchall()

        columns = [
            description[0]
            for description in cursor.description
        ]

        return pd.DataFrame(
            rows,
            columns=columns,
        )

    finally:
        cursor.close()


def load_network_nodes(connection):
    """Load network nodes required for topology analysis."""

    query = """
        SELECT
            node_id,
            node_type
        FROM network_nodes
        ORDER BY node_id;
    """

    return query_to_dataframe(
        connection,
        query,
    )


def load_network_links(connection):
    """Load network links and their endpoint nodes."""

    query = """
        SELECT
            link_id,
            link_type,
            from_node_id,
            to_node_id
        FROM network_links
        ORDER BY link_id;
    """

    return query_to_dataframe(
        connection,
        query,
    )


def load_sensor_assets(connection):
    """Load mappings between sensors and physical network assets."""

    query = """
        SELECT
            sensor_id,
            sensor_type,
            asset_id,
            asset_type
        FROM sensors
        ORDER BY sensor_type, sensor_id;
    """

    return query_to_dataframe(
        connection,
        query,
    )


def build_network_graph(links):
    """
    Build an undirected adjacency graph from network links.

    Each node stores the neighboring node, link ID, and link type.
    """

    graph = defaultdict(list)

    for row in links.itertuples(index=False):
        graph[row.from_node_id].append(
            (
                row.to_node_id,
                row.link_id,
                row.link_type,
            )
        )

        graph[row.to_node_id].append(
            (
                row.from_node_id,
                row.link_id,
                row.link_type,
            )
        )

    return dict(graph)


def shortest_hop_distance(
    graph,
    start_node,
    target_node,
):
    """
    Return the minimum number of network-link hops between two nodes.

    Returns None when either node is absent or the target
    cannot be reached.
    """

    if start_node == target_node:
        return 0

    if (
        start_node not in graph
        or target_node not in graph
    ):
        return None

    queue = deque(
        [(start_node, 0)]
    )

    visited = {
        start_node
    }

    while queue:
        current_node, distance = queue.popleft()

        for neighbor, _, _ in graph[current_node]:
            if neighbor in visited:
                continue

            if neighbor == target_node:
                return distance + 1

            visited.add(neighbor)

            queue.append(
                (
                    neighbor,
                    distance + 1,
                )
            )

    return None


def calculate_all_hop_distances(
    graph,
    start_node,
):
    """
    Return shortest topology distances from one node
    to every reachable node.

    One breadth-first search is performed and the resulting
    distances can be reused during localization.
    """

    if start_node not in graph:
        return {}

    distances = {
        start_node: 0,
    }

    queue = deque(
        [start_node]
    )

    while queue:
        current_node = queue.popleft()

        current_distance = distances[
            current_node
        ]

        for neighbor, _, _ in graph[current_node]:
            if neighbor in distances:
                continue

            distances[neighbor] = (
                current_distance + 1
            )

            queue.append(
                neighbor
            )

    return distances


if __name__ == "__main__":
    connection = get_database_connection()

    try:
        nodes = load_network_nodes(connection)
        links = load_network_links(connection)
        sensors = load_sensor_assets(connection)

        graph = build_network_graph(
            links
        )

        print(
            "Network topology data loaded successfully."
        )

        print("\nNetwork nodes:")
        print("Rows:", len(nodes))
        print(
            "Unique node IDs:",
            nodes["node_id"].nunique(),
        )
        print(
            "Missing node IDs:",
            nodes["node_id"].isna().sum(),
        )

        print("\nNetwork links:")
        print("Rows:", len(links))
        print(
            "Unique link IDs:",
            links["link_id"].nunique(),
        )
        print(
            "Missing from-node IDs:",
            links["from_node_id"].isna().sum(),
        )
        print(
            "Missing to-node IDs:",
            links["to_node_id"].isna().sum(),
        )

        print("\nLink types:")
        print(
            links["link_type"]
            .value_counts()
            .sort_index()
        )

        print("\nSensor mappings:")
        print("Rows:", len(sensors))
        print(
            "Unique sensor references:",
            sensors[
                [
                    "sensor_type",
                    "sensor_id",
                ]
            ]
            .drop_duplicates()
            .shape[0],
        )

        print("\nSensor-to-asset mappings:")
        print(
            sensors.groupby(
                [
                    "sensor_type",
                    "asset_type",
                ]
            )
            .size()
            .to_string()
        )

        print("\nNetwork graph:")
        print(
            "Nodes represented in graph:",
            len(graph),
        )

        isolated_nodes = (
            set(nodes["node_id"])
            - set(graph)
        )

        print(
            "Isolated nodes:",
            len(isolated_nodes),
        )

        edge_references = sum(
            len(neighbors)
            for neighbors in graph.values()
        )

        print(
            "Undirected links represented:",
            edge_references // 2,
        )

        test_cases = [
            ("n229", "n206"),
            ("n229", "n623"),
            ("n215", "n206"),
            ("n215", "n623"),
            ("n389", "n206"),
            ("n389", "n623"),
        ]

        print("\nShortest-hop validation:")

        for start_node, target_node in test_cases:
            distance = shortest_hop_distance(
                graph,
                start_node,
                target_node,
            )

            print(
                f"{start_node} -> {target_node}: "
                f"{distance} hops"
            )

    finally:
        connection.close()
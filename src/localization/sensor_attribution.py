import pandas as pd

from src.data.database import get_database_connection
from src.localization.network_topology import (
    load_network_links,
    load_sensor_assets,
)


def build_sensor_anchor_table(sensors, links):
    """
    Map every sensor to one or more topology anchor nodes.

    Node-mounted sensors receive one anchor node.

    Link-mounted sensors receive both endpoint nodes of the
    physical link on which the sensor is installed.
    """

    node_sensors = sensors.loc[
        sensors["asset_type"] == "node"
    ].copy()

    node_anchors = node_sensors[
        [
            "sensor_id",
            "sensor_type",
            "asset_id",
            "asset_type",
        ]
    ].copy()

    node_anchors["anchor_node_id"] = (
        node_anchors["asset_id"]
    )

    link_sensors = sensors.loc[
        sensors["asset_type"] == "link"
    ].copy()

    link_anchors = link_sensors.merge(
        links[
            [
                "link_id",
                "from_node_id",
                "to_node_id",
            ]
        ],
        left_on="asset_id",
        right_on="link_id",
        how="left",
        validate="many_to_one",
    )

    from_anchors = link_anchors[
        [
            "sensor_id",
            "sensor_type",
            "asset_id",
            "asset_type",
            "from_node_id",
        ]
    ].copy()

    from_anchors = from_anchors.rename(
        columns={
            "from_node_id": "anchor_node_id",
        }
    )

    to_anchors = link_anchors[
        [
            "sensor_id",
            "sensor_type",
            "asset_id",
            "asset_type",
            "to_node_id",
        ]
    ].copy()

    to_anchors = to_anchors.rename(
        columns={
            "to_node_id": "anchor_node_id",
        }
    )

    sensor_anchors = pd.concat(
        [
            node_anchors,
            from_anchors,
            to_anchors,
        ],
        ignore_index=True,
    )

    sensor_anchors = (
        sensor_anchors
        .sort_values(
            [
                "sensor_type",
                "sensor_id",
                "anchor_node_id",
            ]
        )
        .reset_index(drop=True)
    )

    return sensor_anchors


if __name__ == "__main__":
    connection = get_database_connection()

    try:
        sensors = load_sensor_assets(connection)
        links = load_network_links(connection)

        sensor_anchors = build_sensor_anchor_table(
            sensors,
            links,
        )

        print(
            "Sensor topology anchors built successfully."
        )

        print("\nSource sensors:")
        print("Sensors:", len(sensors))

        print("\nTopology anchors:")
        print("Rows:", len(sensor_anchors))

        print(
            "Unique sensor references:",
            sensor_anchors[
                ["sensor_type", "sensor_id"]
            ].drop_duplicates().shape[0],
        )

        print(
            "Missing anchor nodes:",
            sensor_anchors[
                "anchor_node_id"
            ].isna().sum(),
        )

        print("\nAnchors by sensor type:")

        print(
            sensor_anchors.groupby(
                "sensor_type"
            )
            .size()
            .sort_index()
        )

        print("\nLink-mounted sensor anchors:")

        print(
            sensor_anchors.loc[
                sensor_anchors["asset_type"] == "link"
            ].to_string(index=False)
        )

    finally:
        connection.close()
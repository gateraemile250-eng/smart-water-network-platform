"""Load BattLeDIM reference data into PostgreSQL/PostGIS."""

from src.data.battledim_loader import load_scada_dataset
from src.data.database import get_database_connection
from src.data.network_loader import (
    parse_links,
    parse_nodes,
    read_epanet_sections,
)


SENSOR_SOURCES = {
    "pressure": ("2018_SCADA_Pressures.csv", "node", "m"),
    "flow": ("2018_SCADA_Flows.csv", "link", "m3/h"),
    "level": ("2018_SCADA_Levels.csv", "node", "m"),
    "demand": ("2018_SCADA_Demands.csv", "node", "L/h"),
}


def load_network_nodes(connection, nodes: list) -> None:
    """Insert or update network nodes."""

    sql = """
        INSERT INTO network_nodes (node_id, node_type, geometry)
        VALUES (%s, %s, ST_GeomFromText(%s))
        ON CONFLICT (node_id) DO UPDATE SET
            node_type = EXCLUDED.node_type,
            geometry = EXCLUDED.geometry;
    """

    records = [
        (node["node_id"], node["node_type"], node["geometry_wkt"])
        for node in nodes
    ]

    cursor = connection.cursor()
    cursor.executemany(sql, records)
    cursor.close()


def load_network_links(connection, links: list) -> None:
    """Insert or update network links."""

    sql = """
        INSERT INTO network_links (
            link_id,
            link_type,
            from_node_id,
            to_node_id,
            geometry
        )
        VALUES (%s, %s, %s, %s, ST_GeomFromText(%s))
        ON CONFLICT (link_id) DO UPDATE SET
            link_type = EXCLUDED.link_type,
            from_node_id = EXCLUDED.from_node_id,
            to_node_id = EXCLUDED.to_node_id,
            geometry = EXCLUDED.geometry;
    """

    records = [
        (
            link["link_id"],
            link["link_type"],
            link["from_node_id"],
            link["to_node_id"],
            link["geometry_wkt"],
        )
        for link in links
    ]

    cursor = connection.cursor()
    cursor.executemany(sql, records)
    cursor.close()


def prepare_sensors(nodes: list, links: list) -> list:
    """Build sensor metadata and validate asset mappings."""

    assets = {
        "node": {node["node_id"] for node in nodes},
        "link": {link["link_id"] for link in links},
    }

    sensors = []

    for sensor_type, (filename, asset_type, unit) in SENSOR_SOURCES.items():
        dataset = load_scada_dataset(filename)

        for sensor_id in dataset.columns.drop("Timestamp"):
            if sensor_id not in assets[asset_type]:
                raise ValueError(
                    f"{sensor_type} sensor '{sensor_id}' "
                    f"does not map to a network {asset_type}."
                )

            sensors.append(
                {
                    "sensor_id": sensor_id,
                    "sensor_type": sensor_type,
                    "asset_id": sensor_id,
                    "asset_type": asset_type,
                    "unit": unit,
                }
            )

    return sensors


def load_sensors(connection, sensors: list) -> None:
    """Insert or update sensor metadata."""

    sql = """
        INSERT INTO sensors (
            sensor_id,
            sensor_type,
            asset_id,
            asset_type,
            unit
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (sensor_id, sensor_type) DO UPDATE SET
            asset_id = EXCLUDED.asset_id,
            asset_type = EXCLUDED.asset_type,
            unit = EXCLUDED.unit;
    """

    records = [
        (
            sensor["sensor_id"],
            sensor["sensor_type"],
            sensor["asset_id"],
            sensor["asset_type"],
            sensor["unit"],
        )
        for sensor in sensors
    ]

    cursor = connection.cursor()
    cursor.executemany(sql, records)
    cursor.close()


def main() -> None:
    """Load BattLeDIM reference data into PostgreSQL/PostGIS."""

    sections = read_epanet_sections()
    nodes = parse_nodes(sections)
    links = parse_links(sections, nodes)
    sensors = prepare_sensors(nodes, links)

    connection = get_database_connection()

    try:
        load_network_nodes(connection, nodes)
        load_network_links(connection, links)
        load_sensors(connection, sensors)
        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()

    print("\nREFERENCE DATA LOAD")
    print("-" * 60)
    print(f"Network nodes loaded: {len(nodes)}")
    print(f"Network links loaded: {len(links)}")
    print(f"Sensors loaded: {len(sensors)}")


if __name__ == "__main__":
    main()
"""Parse BattLeDIM L-Town network data for spatial storage."""

from pathlib import Path


NETWORK_FILE = Path("data/raw/battledim/L-TOWN.inp")

NODE_SECTIONS = {
    "JUNCTIONS": "junction",
    "RESERVOIRS": "reservoir",
    "TANKS": "tank",
}

LINK_SECTIONS = {
    "PIPES": "pipe",
    "PUMPS": "pump",
    "VALVES": "valve",
}


def read_epanet_sections() -> dict:
    """Read the EPANET network file into named sections."""

    if not NETWORK_FILE.exists():
        raise FileNotFoundError(
            f"Network file not found: {NETWORK_FILE}"
        )

    sections = {}
    current_section = None

    with NETWORK_FILE.open("r", encoding="utf-8") as file:
        for raw_line in file:
            line = raw_line.strip()

            if not line or line.startswith(";"):
                continue

            if line.startswith("[") and line.endswith("]"):
                current_section = line[1:-1].upper()
                sections.setdefault(current_section, [])
                continue

            if current_section:
                sections[current_section].append(line)

    return sections


def parse_coordinates(sections: dict) -> dict:
    """Return node coordinates keyed by node ID."""

    coordinates = {}

    for line in sections.get("COORDINATES", []):
        node_id, x, y, *_ = line.split()

        coordinates[node_id] = (
            float(x),
            float(y),
        )

    return coordinates


def parse_nodes(sections: dict) -> list:
    """Prepare network nodes with coordinates."""

    coordinates = parse_coordinates(sections)
    nodes = []

    for section, node_type in NODE_SECTIONS.items():
        for line in sections.get(section, []):
            node_id = line.split()[0]

            if node_id not in coordinates:
                raise ValueError(
                    f"Coordinates missing for node: {node_id}"
                )

            x, y = coordinates[node_id]

            nodes.append(
                {
                    "node_id": node_id,
                    "node_type": node_type,
                    "x": x,
                    "y": y,
                    "geometry_wkt": f"POINT({x} {y})",
                }
            )

    return nodes


def parse_links(sections: dict, nodes: list) -> list:
    """Prepare network links with endpoint geometry."""

    node_coordinates = {
        node["node_id"]: (node["x"], node["y"])
        for node in nodes
    }

    links = []

    for section, link_type in LINK_SECTIONS.items():
        for line in sections.get(section, []):
            parts = line.split()

            link_id = parts[0]
            from_node = parts[1]
            to_node = parts[2]

            if (
                from_node not in node_coordinates
                or to_node not in node_coordinates
            ):
                raise ValueError(
                    f"Invalid endpoints for link: {link_id}"
                )

            x1, y1 = node_coordinates[from_node]
            x2, y2 = node_coordinates[to_node]

            links.append(
                {
                    "link_id": link_id,
                    "link_type": link_type,
                    "from_node_id": from_node,
                    "to_node_id": to_node,
                    "geometry_wkt": (
                        f"LINESTRING({x1} {y1}, {x2} {y2})"
                    ),
                }
            )

    return links


def main() -> None:
    """Parse and validate network data for spatial storage."""

    sections = read_epanet_sections()
    nodes = parse_nodes(sections)
    links = parse_links(sections, nodes)

    print("\nBATTLEDIM NETWORK STORAGE PREPARATION")
    print("-" * 60)
    print(f"Nodes prepared: {len(nodes)}")
    print(f"Links prepared: {len(links)}")

    print("\nExample node:")
    print(nodes[0])

    print("\nExample link:")
    print(links[0])


if __name__ == "__main__":
    main()
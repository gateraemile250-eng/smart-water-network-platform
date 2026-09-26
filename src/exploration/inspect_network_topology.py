"""Inspect BattLeDIM L-Town network topology and asset relationships."""

from pathlib import Path

from src.data.battledim_loader import load_scada_dataset


NETWORK_FILE = Path("data/raw/battledim/L-TOWN.inp")

SECTIONS = {
    "JUNCTIONS",
    "RESERVOIRS",
    "TANKS",
    "PIPES",
    "PUMPS",
    "VALVES",
    "COORDINATES",
    "VERTICES",
}


def parse_network_sections() -> dict:
    """Extract asset IDs from relevant EPANET sections."""

    if not NETWORK_FILE.exists():
        raise FileNotFoundError(
            f"Network file not found: {NETWORK_FILE}"
        )

    records = {section: [] for section in SECTIONS}
    current_section = None

    with NETWORK_FILE.open("r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()

            if not line or line.startswith(";"):
                continue

            if line.startswith("[") and line.endswith("]"):
                current_section = line[1:-1].upper()
                continue

            if current_section in records:
                asset_id = line.split()[0]
                records[current_section].append(asset_id)

    return records


def validate_asset_relationships(records: dict) -> None:
    """Check whether sensor and leakage IDs exist in the network model."""

    pressure_df = load_scada_dataset(
        "2018_SCADA_Pressures.csv"
    )
    flow_df = load_scada_dataset(
        "2018_SCADA_Flows.csv"
    )
    level_df = load_scada_dataset(
        "2018_SCADA_Levels.csv"
    )
    demand_df = load_scada_dataset(
        "2018_SCADA_Demands.csv"
    )
    leakage_df = load_scada_dataset(
        "2018_Leakages.csv"
    )

    pressure_sensors = set(
        pressure_df.columns.drop("Timestamp")
    )
    flow_sensors = set(
        flow_df.columns.drop("Timestamp")
    )
    level_sensors = set(
        level_df.columns.drop("Timestamp")
    )
    demand_sensors = set(
        demand_df.columns.drop("Timestamp")
    )
    leakage_links = set(
        leakage_df.columns.drop("Timestamp")
    )

    network_nodes = (
        set(records["JUNCTIONS"])
        | set(records["RESERVOIRS"])
        | set(records["TANKS"])
    )

    network_links = (
        set(records["PIPES"])
        | set(records["PUMPS"])
        | set(records["VALVES"])
    )

    print("\nASSET RELATIONSHIP VALIDATION")
    print("-" * 70)

    print(
        f"Pressure sensors mapped to nodes: "
        f"{len(pressure_sensors & network_nodes)}/"
        f"{len(pressure_sensors)}"
    )

    print(
        f"Flow sensors mapped to links: "
        f"{len(flow_sensors & network_links)}/"
        f"{len(flow_sensors)}"
    )

    print(
        f"Level sensors mapped to nodes: "
        f"{len(level_sensors & network_nodes)}/"
        f"{len(level_sensors)}"
    )

    print(
        f"Demand sensors mapped to nodes: "
        f"{len(demand_sensors & network_nodes)}/"
        f"{len(demand_sensors)}"
    )

    print(
        f"Leakage IDs mapped to links: "
        f"{len(leakage_links & network_links)}/"
        f"{len(leakage_links)}"
    )

    unmapped_pressure = pressure_sensors - network_nodes
    unmapped_flow = flow_sensors - network_links
    unmapped_level = level_sensors - network_nodes
    unmapped_demand = demand_sensors - network_nodes
    unmapped_leakage = leakage_links - network_links

    if any(
        [
            unmapped_pressure,
            unmapped_flow,
            unmapped_level,
            unmapped_demand,
            unmapped_leakage,
        ]
    ):
        print("\nUnmapped IDs:")

        if unmapped_pressure:
            print(
                f"Pressure: {sorted(unmapped_pressure)}"
            )

        if unmapped_flow:
            print(
                f"Flow: {sorted(unmapped_flow)}"
            )

        if unmapped_level:
            print(
                f"Level: {sorted(unmapped_level)}"
            )

        if unmapped_demand:
            print(
                f"Demand: {sorted(unmapped_demand)}"
            )

        if unmapped_leakage:
            print(
                f"Leakage: {sorted(unmapped_leakage)}"
            )


def main() -> None:
    """Display topology and validate sensor/network relationships."""

    records = parse_network_sections()

    print("\n" + "=" * 70)
    print("BATTLEDIM L-TOWN NETWORK TOPOLOGY")
    print("=" * 70)

    print(
        f"Junctions:   {len(records['JUNCTIONS'])}"
    )
    print(
        f"Reservoirs:  {len(records['RESERVOIRS'])}"
    )
    print(
        f"Tanks:       {len(records['TANKS'])}"
    )
    print(
        f"Pipes:       {len(records['PIPES'])}"
    )
    print(
        f"Pumps:       {len(records['PUMPS'])}"
    )
    print(
        f"Valves:      {len(records['VALVES'])}"
    )
    print(
        f"Coordinates: {len(records['COORDINATES'])}"
    )
    print(
        f"Vertices:    {len(records['VERTICES'])}"
    )

    validate_asset_relationships(records)


if __name__ == "__main__":
    main()
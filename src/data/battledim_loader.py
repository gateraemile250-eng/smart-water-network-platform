"""Utilities for loading BattLeDIM source datasets."""

from pathlib import Path

import pandas as pd
import yaml


DATA_DIR = Path("data/raw/battledim")
CONFIG_FILENAME = "dataset_configuration.yaml"


def load_scada_dataset(filename: str) -> pd.DataFrame:
    """Load a BattLeDIM SCADA CSV using the source file format."""

    file_path = DATA_DIR / filename

    if not file_path.exists():
        raise FileNotFoundError(f"Dataset not found: {file_path}")

    return pd.read_csv(
        file_path,
        sep=";",
        decimal=",",
        parse_dates=["Timestamp"],
    )


def load_dataset_configuration() -> dict:
    """Load the BattLeDIM dataset configuration."""

    file_path = DATA_DIR / CONFIG_FILENAME

    if not file_path.exists():
        raise FileNotFoundError(
            f"Dataset configuration not found: {file_path}"
        )

    with file_path.open(
        "r",
        encoding="utf-8",
    ) as file:
        return yaml.safe_load(file)


def load_leakage_events() -> pd.DataFrame:
    """
    Load leakage-event metadata from the BattLeDIM configuration.

    BattLeDIM stores each leakage entry as one comma-separated
    string inside the YAML leakage list. This function parses those
    strings into a structured event table.
    """

    configuration = load_dataset_configuration()
    leakage_records = configuration.get("leakages", [])

    columns = [
        "link_id",
        "start_time",
        "end_time",
        "leak_diameter_m",
        "leak_type",
        "peak_time",
    ]

    parsed_records = []

    for record in leakage_records:
        # Ignore empty or non-data entries.
        if not isinstance(record, str):
            continue

        parts = [
            value.strip()
            for value in record.split(",")
        ]

        if len(parts) != len(columns):
            continue

        parsed_records.append(parts)

    leakage_events = pd.DataFrame(
        parsed_records,
        columns=columns,
    )

    datetime_columns = [
        "start_time",
        "end_time",
        "peak_time",
    ]

    for column in datetime_columns:
        leakage_events[column] = pd.to_datetime(
            leakage_events[column]
        )

    leakage_events["leak_diameter_m"] = pd.to_numeric(
        leakage_events["leak_diameter_m"]
    )

    return leakage_events
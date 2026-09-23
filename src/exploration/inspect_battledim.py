"""Inspect the structure and temporal coverage of BattLeDIM 2018 SCADA data."""

from pathlib import Path

import pandas as pd


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

DATA_DIR = Path("data/raw/battledim")

DATASETS = {
    "flows": "2018_SCADA_Flows.csv",
    "levels": "2018_SCADA_Levels.csv",
    "pressures": "2018_SCADA_Pressures.csv",
    "demands": "2018_SCADA_Demands.csv",
}

EXPECTED_INTERVAL = pd.Timedelta(minutes=5)


# ---------------------------------------------------------------------
# Dataset inspection
# ---------------------------------------------------------------------

def inspect_dataset(name: str, filename: str) -> None:
    """Inspect the structure and timestamp continuity of one SCADA dataset."""

    file_path = DATA_DIR / filename

    if not file_path.exists():
        raise FileNotFoundError(f"Dataset not found: {file_path}")

    df = pd.read_csv(
        file_path,
        sep=";",
        decimal=",",
        parse_dates=["Timestamp"],
    )

    # Calculate time intervals between consecutive observations.
    time_differences = df["Timestamp"].diff().dropna()

    unexpected_intervals = time_differences[
        time_differences != EXPECTED_INTERVAL
    ]

    # Structural summary.
    print(f"\n{'=' * 60}")
    print(f"DATASET: {name.upper()}")
    print(f"FILE: {filename}")
    print(f"{'=' * 60}")

    print(f"Shape: {df.shape}")
    print(f"Rows: {len(df):,}")
    print(f"Columns: {len(df.columns)}")
    print(f"Measurement columns: {len(df.columns) - 1}")

    # Temporal summary.
    print("\nTemporal coverage:")
    print(f"Start timestamp: {df['Timestamp'].min()}")
    print(f"End timestamp: {df['Timestamp'].max()}")
    print(f"Expected interval: {EXPECTED_INTERVAL}")
    print(f"Unexpected time intervals: {len(unexpected_intervals):,}")

    # Schema summary.
    print("\nColumn names:")
    print(df.columns.tolist())

    print("\nData types:")
    print(df.dtypes)

    # Small sample for visual inspection.
    print("\nFirst 3 rows:")
    print(df.head(3))

    print("\nLast 3 rows:")
    print(df.tail(3))


# ---------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------

def main() -> None:
    """Inspect all configured BattLeDIM SCADA datasets."""

    for dataset_name, dataset_file in DATASETS.items():
        inspect_dataset(dataset_name, dataset_file)


if __name__ == "__main__":
    main()
"""Utilities for loading BattLeDIM source datasets."""

from pathlib import Path

import pandas as pd


DATA_DIR = Path("data/raw/battledim")


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
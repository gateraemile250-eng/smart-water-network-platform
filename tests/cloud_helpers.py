"""Small synthetic datasets shared by the cloud tests.

The data spans a month boundary (two timestamps in January, three in
February) so partitioning can be tested with a handful of rows.
"""

from pathlib import Path

import numpy as np
import pandas as pd

from src.cloud.quality_gate import Expectations

SMALL = Expectations(
    sensor_counts={"pressure": 2, "flow": 1, "level": 1, "demand": 2},
    first=pd.Timestamp("2018-01-31 23:50"),
    last=pd.Timestamp("2018-02-01 00:10"),
    step=pd.Timedelta(minutes=5),
)

CONFIG = {
    "pressure": ["p1", "p2"],
    "flow": ["f1"],
    "level": ["t1"],
    "demand": ["d1", "d2"],
}


def make_frames(expect=SMALL, config=CONFIG):
    """Build a healthy set of four wide SCADA tables."""

    grid = expect.grid
    frames = {}

    for sensor_type, sensors in config.items():
        data = {"Timestamp": grid}

        for offset, sensor in enumerate(sensors):
            data[sensor] = np.arange(len(grid), dtype=float) * 0.1 + 1 + offset

        frames[sensor_type] = pd.DataFrame(data)

    return frames


def failed_errors(results):
    """Names of ERROR checks that did not pass."""

    return {
        r["name"]
        for r in results
        if not r["passed"] and r["severity"] == "ERROR"
    }


def failed_warnings(results):
    """Names of WARNING checks that did not pass."""

    return {
        r["name"]
        for r in results
        if not r["passed"] and r["severity"] == "WARNING"
    }


def detail_of(results, name):
    """Return the detail text of one check."""

    return next(r["detail"] for r in results if r["name"] == name)


class FakeParquet:
    """Stand-in writer and describer so tests need no pyarrow."""

    def __init__(self, rows_override=None):
        self.written = {}
        self.rows_override = rows_override

    def write(self, frame, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"fake-parquet-" + str(len(frame)).encode())
        self.written[str(path)] = len(frame)

    def describe(self, path: Path):
        from src.cloud.parquet_io import PROCESSED_TYPES

        rows = self.written[str(path)]

        return {
            "rows": self.rows_override if self.rows_override is not None else rows,
            "columns": dict(PROCESSED_TYPES),
            "compression": ["SNAPPY"],
        }

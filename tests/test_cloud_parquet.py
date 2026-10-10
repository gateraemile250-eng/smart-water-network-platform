"""Tests that need pyarrow (skipped automatically if it is missing)."""

import hashlib
import tempfile
from pathlib import Path

import pytest

pytest.importorskip("pyarrow")

from src.cloud import parquet_io  # noqa: E402
from src.cloud.build import run_build, to_long_format  # noqa: E402
from cloud_helpers import CONFIG, SMALL, make_frames  # noqa: E402


def write_small(path):
    frame = to_long_format(make_frames(), 2018, 2)
    parquet_io.write_parquet(frame, path, schema=parquet_io.processed_schema())

    return frame


def test_parquet_round_trip_keeps_rows_columns_and_types():
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "part-0000.parquet"
        frame = write_small(path)
        info = parquet_io.describe_parquet(path)

        assert info["rows"] == len(frame)
        assert info["columns"] == parquet_io.PROCESSED_TYPES


def test_parquet_uses_snappy_compression():
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "part-0000.parquet"
        write_small(path)

        assert parquet_io.describe_parquet(path)["compression"] == ["SNAPPY"]


def test_parquet_can_be_read_with_pandas():
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "part-0000.parquet"
        frame = write_small(path)
        sample = parquet_io.read_sample(path, rows=4)

        assert len(sample) == 4
        assert list(sample.columns) == list(frame.columns)
        assert sample["value"].iloc[0] == frame["value"].iloc[0]


def test_partition_columns_are_not_stored_inside_the_file():
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "part-0000.parquet"
        write_small(path)
        columns = parquet_io.describe_parquet(path)["columns"]

        assert "year" not in columns and "month" not in columns


def test_writing_the_same_data_twice_gives_identical_bytes():
    with tempfile.TemporaryDirectory() as tmp:
        first, second = Path(tmp) / "a.parquet", Path(tmp) / "b.parquet"
        write_small(first)
        write_small(second)

        digest = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()

        assert digest(first) == digest(second)


def test_full_build_with_the_real_writer_creates_readable_partitions():
    with tempfile.TemporaryDirectory() as tmp:
        status = run_build(make_frames(), CONFIG, SMALL, publish_dir=Path(tmp))

        assert status["passed"] is True
        assert sorted(p.parent.name for p in Path(tmp).rglob("part-0000.parquet")) == [
            "month=01", "month=02",
        ]

        for item in status["files"]:
            info = parquet_io.describe_parquet(Path(tmp) / item["key"])

            assert info["rows"] == item["rows"]
            assert info["compression"] == ["SNAPPY"]

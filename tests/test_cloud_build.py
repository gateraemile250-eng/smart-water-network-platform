"""Tests for conversion, partitioning and the build report.

No AWS, no PostgreSQL and no pyarrow: a stand-in writer replaces the
Parquet writer, which has its own tests in test_cloud_parquet.py.
"""

import json
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

from src.cloud.build import format_summary, run_build, to_long_format
from src.cloud.layout import PROCESSED_COLUMNS, processed_key
from cloud_helpers import (
    CONFIG,
    SMALL,
    FakeParquet,
    failed_errors,
    make_frames,
)

JAN = "processed/sensor_readings/year=2018/month=01/part-0000.parquet"
FEB = "processed/sensor_readings/year=2018/month=02/part-0000.parquet"


def build(frames=None, fake=None, publish_dir=None):
    fake = fake or FakeParquet()

    return run_build(
        frames or make_frames(),
        CONFIG,
        SMALL,
        publish_dir=publish_dir,
        writer=fake.write,
        describer=fake.describe,
    ), fake


# ---------------------------------------------------------- long format


def test_long_format_has_the_designed_structure():
    long = to_long_format(make_frames(), 2018, 2)

    assert list(long.columns) == PROCESSED_COLUMNS
    # 3 February timestamps x 6 sensors
    assert len(long) == 18
    assert str(long["event_time"].dtype) == "datetime64[ms]"
    assert long["value"].dtype == np.float64


def test_long_format_units_follow_the_sensor_type():
    long = to_long_format(make_frames(), 2018, 2)
    units = long.groupby("sensor_type")["unit"].first().to_dict()

    assert units == {"demand": "L/h", "flow": "m3/h", "level": "m", "pressure": "m"}


def test_long_format_is_sorted_by_type_sensor_and_time():
    long = to_long_format(make_frames(), 2018, 2)
    expected = long.sort_values(["sensor_type", "sensor_id", "event_time"]).reset_index(drop=True)

    assert long.equals(expected)


def test_long_format_keeps_only_the_requested_month():
    january = to_long_format(make_frames(), 2018, 1)

    assert len(january) == 2 * 6
    assert january["event_time"].dt.month.eq(1).all()


def test_long_format_does_not_contain_partition_columns():
    long = to_long_format(make_frames(), 2018, 2)

    assert "year" not in long.columns and "month" not in long.columns


# ------------------------------------------------------------ the build


def test_valid_build_writes_one_file_per_month_in_the_planned_layout():
    with tempfile.TemporaryDirectory() as tmp:
        status, fake = build(publish_dir=Path(tmp))

        assert status["passed"] is True
        assert [f["key"] for f in status["files"]] == [JAN, FEB]
        assert (Path(tmp) / JAN).exists() and (Path(tmp) / FEB).exists()
        assert [f["rows"] for f in status["files"]] == [12, 18]


def test_build_reports_input_duplicates_removed_and_final_rows():
    with tempfile.TemporaryDirectory() as tmp:
        status, _ = build(publish_dir=Path(tmp))

        assert status["input_rows"] == 30
        assert status["duplicate_rows_detected"] == 0
        assert status["rows_removed"] == 0
        assert status["final_rows"] == 30
        assert status["errors"] == 0 and status["warnings"] == []


def test_build_records_a_checksum_and_size_for_every_file():
    with tempfile.TemporaryDirectory() as tmp:
        status, _ = build(publish_dir=Path(tmp))

        for item in status["files"]:
            assert len(item["sha256"]) == 64
            assert item["bytes"] == (Path(tmp) / item["key"]).stat().st_size

        assert status["output_bytes"] == sum(f["bytes"] for f in status["files"])


def test_build_saves_a_report_and_a_status_file():
    with tempfile.TemporaryDirectory() as tmp:
        build(publish_dir=Path(tmp))

        status = json.loads((Path(tmp) / "build_status.json").read_text())
        report = json.loads((Path(tmp) / "reports" / "build_report.json").read_text())

        assert status["passed"] is True
        assert report["checks"] and report["status"]["final_rows"] == 30


def test_a_failing_source_gate_writes_nothing():
    frames = make_frames()
    frames["pressure"].loc[1, "p1"] = np.nan

    with tempfile.TemporaryDirectory() as tmp:
        status, fake = build(frames, publish_dir=Path(tmp))

        assert status["passed"] is False
        assert fake.written == {}
        assert not (Path(tmp) / "processed").exists()
        assert "values_finite" in failed_errors(status["checks"])
        # The report is still saved so the failure can be read.
        assert (Path(tmp) / "reports" / "build_report.json").exists()


def test_a_failing_build_is_marked_failed_in_the_saved_status():
    frames = make_frames()
    frames["level"].loc[0, "t1"] = -1.0

    with tempfile.TemporaryDirectory() as tmp:
        build(frames, publish_dir=Path(tmp))

        status = json.loads((Path(tmp) / "build_status.json").read_text())

        assert status["passed"] is False and status["files"] == []


def test_old_output_is_removed_before_a_new_build():
    with tempfile.TemporaryDirectory() as tmp:
        stale = Path(tmp) / "processed/sensor_readings/year=2018/month=03/old.parquet"
        stale.parent.mkdir(parents=True)
        stale.write_bytes(b"old")

        build(publish_dir=Path(tmp))

        assert not stale.exists()


def test_a_readback_mismatch_fails_the_build():
    with tempfile.TemporaryDirectory() as tmp:
        status, _ = build(fake=FakeParquet(rows_override=1), publish_dir=Path(tmp))

        assert status["passed"] is False
        assert "parquet_rows_match" in failed_errors(status["checks"])


def test_summary_text_contains_the_headline_numbers():
    with tempfile.TemporaryDirectory() as tmp:
        status, _ = build(publish_dir=Path(tmp))
        text = format_summary(status)

        assert "Input rows:" in text and "30" in text
        assert "Duplicate rows detected:" in text
        assert "Rows removed:" in text
        assert "Final rows:" in text
        assert "PASSED THE GATE:          YES" in text


def test_processed_key_matches_the_approved_layout():
    assert processed_key(2018, 1) == JAN
    assert processed_key(2018, 12).endswith("year=2018/month=12/part-0000.parquet")


def test_a_month_that_fails_its_checks_stops_the_build_before_writing():
    from src.cloud import build as build_module

    forced = [{
        "name": "month_row_count",
        "severity": "ERROR",
        "passed": False,
        "detail": "forced failure",
    }]
    original = build_module.check_month
    build_module.check_month = lambda *args, **kwargs: forced

    try:
        with tempfile.TemporaryDirectory() as tmp:
            status, fake = build(publish_dir=Path(tmp))

            assert status["passed"] is False
            assert fake.written == {}
            assert status["files"] == []
            assert "month_row_count" in failed_errors(status["checks"])
    finally:
        build_module.check_month = original

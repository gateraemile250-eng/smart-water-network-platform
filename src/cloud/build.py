"""Build the processed readings dataset locally (no AWS, no PostgreSQL).

    python -m src.cloud.build

Steps: load the four SCADA CSV files, run the source quality gate,
convert month by month to the long format, de-duplicate, check every
month, write Snappy Parquet files under data/publish/, read each file
back, and save a report. Nothing is uploaded anywhere.
"""

from __future__ import annotations

import json
import logging
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from src.cloud import parquet_io
from src.cloud.layout import (
    CONFIG_SENSOR_KEYS,
    DATA_DIR,
    PROCESSED_COLUMNS,
    PROCESSED_PREFIX,
    PUBLISH_DIR,
    SENSOR_FILES,
    processed_key,
)
from src.cloud.quality_gate import (
    Expectations,
    Result,
    check_dedup,
    check_month,
    check_source,
    deduplicate,
    format_report,
    has_errors,
    merge_results,
)
from src.data.battledim_loader import (
    load_dataset_configuration,
    load_scada_dataset,
)
from src.data.quality_checks import EXPECTED_UNITS

logger = logging.getLogger("smart_water.build")

Writer = Callable[[pd.DataFrame, Path], None]
Describer = Callable[[Path], dict[str, Any]]


def to_long_format(
    frames: dict[str, pd.DataFrame], year: int, month: int
) -> pd.DataFrame:
    """Convert one month of the wide SCADA tables to the long format.

    Wide: one column per sensor. Long: one row per reading with the
    columns event_time, sensor_id, sensor_type, unit and value, sorted
    by sensor type, sensor and time.
    """

    parts = []

    for sensor_type in sorted(frames):
        wide = frames[sensor_type]
        stamps = wide["Timestamp"]
        selected = wide.loc[
            (stamps.dt.year == year) & (stamps.dt.month == month)
        ]

        long = selected.melt(
            id_vars="Timestamp",
            var_name="sensor_id",
            value_name="value",
        ).rename(columns={"Timestamp": "event_time"})

        long["sensor_type"] = sensor_type
        long["unit"] = EXPECTED_UNITS[sensor_type]
        parts.append(long)

    combined = pd.concat(parts, ignore_index=True)
    combined["event_time"] = combined["event_time"].astype("datetime64[ms]")
    combined["sensor_id"] = combined["sensor_id"].astype(str)
    combined["value"] = combined["value"].astype("float64")

    return (
        combined[PROCESSED_COLUMNS]
        .sort_values(
            ["sensor_type", "sensor_id", "event_time"], kind="stable"
        )
        .reset_index(drop=True)
    )


def file_sha256(path: Path) -> str:
    """Return the SHA-256 digest of a file."""

    import hashlib

    digest = hashlib.sha256()

    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)

    return digest.hexdigest()


def _check_readback(
    frame: pd.DataFrame, info: dict[str, Any], label: str
) -> list[Result]:
    """Compare a written Parquet file with the frame it came from."""

    wanted = parquet_io.PROCESSED_TYPES
    types_ok = info["columns"] == wanted
    rows_ok = info["rows"] == len(frame)
    compression_ok = info["compression"] == ["SNAPPY"]

    return [
        {
            "name": "parquet_rows_match",
            "severity": "ERROR",
            "passed": rows_ok,
            "detail": f"{label}: file has {info['rows']} rows, "
            f"expected {len(frame)}"
            if not rows_ok
            else "rows read back match",
        },
        {
            "name": "parquet_schema",
            "severity": "ERROR",
            "passed": types_ok,
            "detail": f"{label}: types {info['columns']}"
            if not types_ok
            else "column names and types match the design",
        },
        {
            "name": "parquet_snappy",
            "severity": "ERROR",
            "passed": compression_ok,
            "detail": f"{label}: compression {info['compression']}"
            if not compression_ok
            else "Snappy compression",
        },
    ]


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=str)


def run_build(
    frames: dict[str, pd.DataFrame],
    config_sensors: dict[str, list],
    expect: Expectations | None = None,
    publish_dir: Path = PUBLISH_DIR,
    source_bytes: int = 0,
    writer: Writer | None = None,
    describer: Describer | None = None,
) -> dict[str, Any]:
    """Run the gate, convert and write Parquet. Returns the status dict.

    If any ERROR check fails, nothing is written and the status says
    passed = False.
    """

    expect = expect or Expectations()
    publish_dir = Path(publish_dir)
    writer = writer or (
        lambda frame, path: parquet_io.write_parquet(
            frame, path, schema=parquet_io.processed_schema()
        )
    )
    describer = describer or parquet_io.describe_parquet

    source_results = check_source(frames, config_sensors, expect)

    status: dict[str, Any] = {
        "stage": "build",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "passed": False,
        "input_rows": 0,
        "duplicate_rows_detected": 0,
        "rows_removed": 0,
        "final_rows": 0,
        "source_bytes": source_bytes,
        "output_bytes": 0,
        "files": [],
    }

    all_results: list[list[Result]] = [source_results]

    if has_errors(source_results):
        logger.error("Source quality gate FAILED. Nothing was written.")
        return _finish(status, all_results, publish_dir)

    processed_dir = publish_dir / PROCESSED_PREFIX

    if processed_dir.exists():
        shutil.rmtree(processed_dir)

    reference = frames[next(iter(expect.sensor_counts))]["Timestamp"]
    months = sorted(
        {(int(t.year), int(t.month)) for t in reference.drop_duplicates()}
    )
    expected_per_month = expect.timestamps_per_month()

    for year, month in months:
        label = f"{year}-{month:02d}"
        long = to_long_format(frames, year, month)
        cleaned, stats = deduplicate(long)

        status["input_rows"] += stats.input_rows
        status["duplicate_rows_detected"] += stats.duplicate_rows_detected
        status["rows_removed"] += stats.rows_removed

        month_results = check_dedup(stats) + check_month(
            cleaned,
            year,
            month,
            expected_per_month.get((year, month), 0),
            expect.sensors_per_timestamp,
        )
        all_results.append(month_results)

        if has_errors(month_results):
            logger.error("Month %s FAILED the gate. Stopping.", label)
            break

        key = processed_key(year, month)
        path = publish_dir / key
        writer(cleaned, path)
        info = describer(path)
        readback = _check_readback(cleaned, info, label)
        all_results.append(readback)

        if has_errors(readback):
            logger.error("Read-back of %s FAILED. Stopping.", label)
            break

        status["final_rows"] += stats.final_rows
        status["files"].append(
            {
                "key": key,
                "path": str(path),
                "rows": info["rows"],
                "bytes": path.stat().st_size,
                "sha256": file_sha256(path),
            }
        )
        logger.info(
            "Wrote %s: %d rows, %.1f MB",
            key,
            info["rows"],
            path.stat().st_size / 1e6,
        )

    status["output_bytes"] = sum(item["bytes"] for item in status["files"])

    return _finish(status, all_results, publish_dir)


def _finish(
    status: dict[str, Any],
    all_results: list[list[Result]],
    publish_dir: Path,
) -> dict[str, Any]:
    """Merge results, decide pass or fail and save the report files."""

    merged = merge_results(all_results)
    status["passed"] = not has_errors(merged) and bool(status["files"])
    status["errors"] = sum(
        1 for r in merged if not r["passed"] and r["severity"] == "ERROR"
    )
    status["warnings"] = [
        r["name"]
        for r in merged
        if not r["passed"] and r["severity"] == "WARNING"
    ]

    _write_json(publish_dir / "reports" / "build_report.json", {
        "status": status,
        "checks": merged,
    })
    _write_json(publish_dir / "build_status.json", status)

    status["checks"] = merged

    return status


def load_frames() -> tuple[dict[str, pd.DataFrame], dict[str, list], int]:
    """Load the four SCADA tables, the configured sensor lists and sizes."""

    frames = {
        sensor_type: load_scada_dataset(filename)
        for sensor_type, filename in SENSOR_FILES.items()
    }
    configuration = load_dataset_configuration()
    config_sensors = {
        sensor_type: list(configuration[key])
        for sensor_type, key in CONFIG_SENSOR_KEYS.items()
    }
    source_bytes = sum(
        (DATA_DIR / filename).stat().st_size
        for filename in SENSOR_FILES.values()
    )

    return frames, config_sensors, source_bytes


def format_summary(status: dict[str, Any]) -> str:
    """Return the headline numbers of a build as readable text."""

    ratio = (
        status["source_bytes"] / status["output_bytes"]
        if status["output_bytes"]
        else 0
    )

    return "\n".join(
        [
            f"Input rows:               {status['input_rows']:,}",
            f"Duplicate rows detected:  {status['duplicate_rows_detected']:,}",
            f"Rows removed:             {status['rows_removed']:,}",
            f"Final rows:               {status['final_rows']:,}",
            f"Output files:             {len(status['files'])}",
            f"Source CSV size:          {status['source_bytes'] / 1e6:,.1f} MB",
            f"Parquet size:             {status['output_bytes'] / 1e6:,.1f} MB",
            f"CSV / Parquet ratio:      {ratio:.2f}x",
            f"Errors:                   {status['errors']}",
            f"Warnings:                 {status['warnings'] or 'none'}",
            f"PASSED THE GATE:          {'YES' if status['passed'] else 'NO'}",
        ]
    )


def main() -> int:
    """Run the build from the command line."""

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    frames, config_sensors, source_bytes = load_frames()
    status = run_build(frames, config_sensors, source_bytes=source_bytes)

    print("\nQuality gate report")
    print(format_report(status["checks"]))
    print("\nSummary")
    print(format_summary(status))
    print(f"\nReport: {PUBLISH_DIR / 'reports' / 'build_report.json'}")

    return 0 if status["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())

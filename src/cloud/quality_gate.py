"""Quality gate for the BattLeDIM publishing pipeline.

Pure pandas, no file or network access. Each check records a result
with a severity:

* ERROR   - the dataset must not be published.
* WARNING - reported, but does not block publishing.

The same result format and severities as src/data/quality_checks.py are
used, so reports look alike across the project.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from src.cloud.layout import (
    EXPECTED_SENSOR_COUNTS,
    KEY_COLUMNS,
    PROCESSED_COLUMNS,
)
from src.data.quality_checks import (
    ERROR,
    EXPECTED_UNITS,
    PLAUSIBLE_MAX,
    WARNING,
    format_report,
    has_errors,
)

Result = dict[str, Any]

__all__ = [
    "ERROR",
    "WARNING",
    "DedupStats",
    "Expectations",
    "check_dedup",
    "check_month",
    "check_source",
    "deduplicate",
    "format_report",
    "has_errors",
    "merge_results",
]


@dataclass(frozen=True)
class Expectations:
    """What a complete, healthy dataset looks like.

    The defaults describe the BattLeDIM 2018 SCADA data. Tests pass
    smaller values.
    """

    sensor_counts: dict[str, int] = field(
        default_factory=lambda: dict(EXPECTED_SENSOR_COUNTS)
    )
    first: pd.Timestamp = pd.Timestamp("2018-01-01 00:00")
    last: pd.Timestamp = pd.Timestamp("2018-12-31 23:55")
    step: pd.Timedelta = pd.Timedelta(minutes=5)

    @property
    def grid(self) -> pd.DatetimeIndex:
        """Every timestamp that must exist."""

        return pd.date_range(self.first, self.last, freq=self.step)

    @property
    def sensors_per_timestamp(self) -> int:
        """Readings expected at every timestamp."""

        return sum(self.sensor_counts.values())

    def timestamps_per_month(self) -> dict[tuple[int, int], int]:
        """Return the expected timestamp count for each (year, month)."""

        grid = self.grid
        counts = pd.Series(1, index=grid).groupby(
            [grid.year, grid.month]
        ).sum()

        return {
            (int(year), int(month)): int(count)
            for (year, month), count in counts.items()
        }


class _Checks:
    """Collects check results."""

    def __init__(self) -> None:
        self.items: list[Result] = []

    def add(
        self,
        name: str,
        passed: bool,
        detail: str,
        severity: str = ERROR,
    ) -> None:
        self.items.append(
            {
                "name": name,
                "severity": severity,
                "passed": bool(passed),
                "detail": detail,
            }
        )


def _minutes(step: pd.Timedelta) -> str:
    """Format a time step as, for example, '5 minutes'."""

    minutes = step.total_seconds() / 60

    return f"{minutes:g} minutes"


def _examples(values: Any, limit: int = 3) -> str:
    """Return the first few offending values as short text."""

    items = [str(value) for value in list(values)[:limit]]

    return ", ".join(items) if items else "none"


def check_source(
    frames: dict[str, pd.DataFrame],
    config_sensors: dict[str, list],
    expect: Expectations | None = None,
) -> list[Result]:
    """Check the raw SCADA tables before any conversion."""

    expect = expect or Expectations()
    checks = _Checks()
    types = list(expect.sensor_counts)

    missing = [name for name in types if name not in frames]
    checks.add(
        "scada_tables_loaded",
        not missing,
        f"missing tables: {missing}" if missing else f"{len(types)} tables",
    )

    if missing:
        return checks.items

    no_timestamp = [
        name for name in types if "Timestamp" not in frames[name].columns
    ]
    checks.add(
        "timestamp_column_present",
        not no_timestamp,
        f"no 'Timestamp' column in: {no_timestamp}"
        if no_timestamp
        else "present in all tables",
    )

    if no_timestamp:
        return checks.items

    columns = {
        name: [c for c in frames[name].columns if c != "Timestamp"]
        for name in types
    }

    wrong_counts = {
        name: f"{len(columns[name])} (expected {expect.sensor_counts[name]})"
        for name in types
        if len(columns[name]) != expect.sensor_counts[name]
    }
    checks.add(
        "sensor_counts",
        not wrong_counts,
        f"wrong sensor counts: {wrong_counts}"
        if wrong_counts
        else f"{expect.sensors_per_timestamp} sensors in total",
    )

    id_problems: dict[str, str] = {}

    for name in types:
        found = columns[name]
        wanted = [str(sensor) for sensor in config_sensors.get(name, [])]
        unexpected = sorted(set(found) - set(wanted))
        absent = sorted(set(wanted) - set(found))

        if len(set(found)) != len(found):
            id_problems[name] = "duplicate column names"
        elif unexpected or absent:
            id_problems[name] = (
                f"unexpected {_examples(unexpected)}; "
                f"missing {_examples(absent)}"
            )

    checks.add(
        "sensor_ids_match_configuration",
        not id_problems,
        f"sensor IDs differ from dataset_configuration.yaml: {id_problems}"
        if id_problems
        else "all sensor IDs match dataset_configuration.yaml",
    )

    expected_rows = len(expect.grid)
    wrong_rows = {
        name: len(frames[name])
        for name in types
        if len(frames[name]) != expected_rows
    }
    checks.add(
        "row_count",
        not wrong_rows,
        f"expected {expected_rows} rows per table, found {wrong_rows}"
        if wrong_rows
        else f"{expected_rows} rows in every table",
    )

    reference_name = types[0]
    reference = frames[reference_name]["Timestamp"].reset_index(drop=True)

    is_datetime = pd.api.types.is_datetime64_any_dtype(reference)
    checks.add(
        "timestamp_dtype",
        is_datetime,
        "Timestamp parsed as datetime"
        if is_datetime
        else f"Timestamp is {reference.dtype}, so some values did not parse",
    )

    if not is_datetime:
        return checks.items

    missing_ts = int(reference.isna().sum())
    checks.add(
        "timestamps_not_missing",
        missing_ts == 0,
        f"{missing_ts} missing timestamps" if missing_ts else "none missing",
    )

    duplicated = reference[reference.duplicated()]
    checks.add(
        "timestamps_unique",
        duplicated.empty,
        f"{len(duplicated)} duplicated timestamps, for example "
        f"{_examples(duplicated)}"
        if not duplicated.empty
        else "all unique",
    )

    checks.add(
        "timestamps_increasing",
        bool(reference.is_monotonic_increasing),
        "in chronological order"
        if reference.is_monotonic_increasing
        else "timestamps are not in chronological order",
    )

    steps = reference.diff().dropna()
    irregular = steps[steps != expect.step]
    checks.add(
        "timestamps_regular_step",
        irregular.empty,
        f"{len(irregular)} irregular steps (expected every "
        f"{_minutes(expect.step)}), "
        f"first at row {_examples(irregular.index)}"
        if not irregular.empty
        else f"every step is {_minutes(expect.step)}",
    )

    in_range = (
        len(reference) > 0
        and reference.iloc[0] == expect.first
        and reference.iloc[-1] == expect.last
    )
    checks.add(
        "timestamp_range",
        in_range,
        f"expected {expect.first} to {expect.last}"
        + ("" if in_range else f", found {_examples(reference.iloc[[0, -1]])}")
        if len(reference)
        else "no timestamps",
    )

    inconsistent = [
        name
        for name in types[1:]
        if not frames[name]["Timestamp"]
        .reset_index(drop=True)
        .equals(reference)
    ]
    checks.add(
        "timestamps_consistent_across_files",
        not inconsistent,
        f"timestamps differ from {reference_name} in: {inconsistent}"
        if inconsistent
        else "all four files share the same timestamps",
    )

    non_numeric: dict[str, list[str]] = {}
    non_finite: dict[str, int] = {}
    negative: dict[str, float] = {}
    too_high: dict[str, float] = {}

    for name in types:
        bad_columns = [
            column
            for column in columns[name]
            if not pd.api.types.is_numeric_dtype(frames[name][column])
        ]

        if bad_columns:
            non_numeric[name] = bad_columns[:3]
            continue

        values = frames[name][columns[name]].to_numpy(dtype=float)
        finite = np.isfinite(values)

        if not finite.all():
            non_finite[name] = int((~finite).sum())

        usable = values[finite]

        if usable.size and usable.min() < 0:
            negative[name] = float(usable.min())

        if usable.size and usable.max() > PLAUSIBLE_MAX.get(name, np.inf):
            too_high[name] = float(usable.max())

    checks.add(
        "values_numeric",
        not non_numeric,
        f"non-numeric columns (first few): {non_numeric}"
        if non_numeric
        else "all sensor columns are numeric",
    )
    checks.add(
        "values_finite",
        not non_finite,
        f"missing, NaN or infinite values: {non_finite}"
        if non_finite
        else "no missing, NaN or infinite values",
    )
    checks.add(
        "values_non_negative",
        not negative,
        f"negative minimums: {negative}"
        if negative
        else "no negative values",
    )
    checks.add(
        "values_within_plausible_range",
        not too_high,
        f"maximums above the plausible limit: {too_high}"
        if too_high
        else "all maximums within plausible limits",
        severity=WARNING,
    )

    return checks.items


@dataclass(frozen=True)
class DedupStats:
    """What de-duplication found and did."""

    input_rows: int
    duplicate_rows_detected: int
    rows_removed: int
    conflicting_keys: int
    final_rows: int


def deduplicate(frame: pd.DataFrame) -> tuple[pd.DataFrame, DedupStats]:
    """Remove exact duplicate readings.

    A duplicate is a row whose key (sensor_type, sensor_id, event_time)
    already appeared earlier. Rows with the same key and the same value
    are exact duplicates: the first is kept and the rest are removed.

    If rows share a key but have *different* values there is no safe
    way to choose, so nothing is removed and the conflict is reported.
    """

    input_rows = len(frame)
    repeated = frame.duplicated(subset=KEY_COLUMNS, keep="first")
    detected = int(repeated.sum())

    if detected == 0:
        return frame, DedupStats(input_rows, 0, 0, 0, input_rows)

    in_group = frame[frame.duplicated(subset=KEY_COLUMNS, keep=False)]
    distinct_values = in_group.groupby(
        KEY_COLUMNS, observed=True, sort=False
    )["value"].nunique(dropna=False)
    conflicts = int((distinct_values > 1).sum())

    if conflicts:
        return frame, DedupStats(input_rows, detected, 0, conflicts, input_rows)

    cleaned = frame.loc[~repeated].reset_index(drop=True)

    return cleaned, DedupStats(
        input_rows, detected, detected, 0, len(cleaned)
    )


def check_dedup(stats: DedupStats) -> list[Result]:
    """Turn de-duplication statistics into check results."""

    checks = _Checks()

    checks.add(
        "no_conflicting_duplicates",
        stats.conflicting_keys == 0,
        f"{stats.conflicting_keys} keys have the same sensor and time but "
        "different values"
        if stats.conflicting_keys
        else "no keys with conflicting values",
    )
    checks.add(
        "duplicate_rows",
        stats.duplicate_rows_detected == 0,
        f"{stats.duplicate_rows_detected} duplicate rows detected, "
        f"{stats.rows_removed} removed"
        if stats.duplicate_rows_detected
        else "no duplicate rows",
        severity=WARNING,
    )

    return checks.items


def check_month(
    frame: pd.DataFrame,
    year: int,
    month: int,
    expected_timestamps: int,
    sensors_per_timestamp: int,
) -> list[Result]:
    """Check one month of converted long-format readings."""

    checks = _Checks()
    label = f"{year}-{month:02d}"

    checks.add(
        "columns_exact",
        list(frame.columns) == PROCESSED_COLUMNS,
        f"{label}: columns {list(frame.columns)}"
        if list(frame.columns) != PROCESSED_COLUMNS
        else "columns are as designed",
    )

    if list(frame.columns) != PROCESSED_COLUMNS:
        return checks.items

    time_ok = pd.api.types.is_datetime64_any_dtype(frame["event_time"])
    value_ok = pd.api.types.is_float_dtype(frame["value"])
    text_ok = all(
        pd.api.types.is_string_dtype(frame[column])
        or pd.api.types.is_object_dtype(frame[column])
        for column in ("sensor_id", "sensor_type", "unit")
    )
    checks.add(
        "column_types",
        time_ok and value_ok and text_ok,
        f"{label}: event_time datetime={time_ok}, value float={value_ok}, "
        f"text columns={text_ok}"
        if not (time_ok and value_ok and text_ok)
        else "event_time datetime, value float, text columns as strings",
    )

    expected_rows = expected_timestamps * sensors_per_timestamp
    rows_ok = len(frame) == expected_rows
    checks.add(
        "month_row_count",
        rows_ok,
        f"{expected_timestamps} timestamps x {sensors_per_timestamp} sensors"
        if rows_ok
        else f"{label}: {len(frame)} rows, expected {expected_rows} "
        f"({expected_timestamps} timestamps x {sensors_per_timestamp} sensors)",
    )

    if not time_ok:
        return checks.items

    if len(frame):
        first, last = frame["event_time"].min(), frame["event_time"].max()
        inside = (
            (first.year, first.month) == (year, month)
            and (last.year, last.month) == (year, month)
        )
    else:
        inside = False

    checks.add(
        "month_time_bounds",
        inside,
        "all rows fall in the month"
        if inside
        else f"{label}: rows outside the month (first {first}, last {last})"
        if len(frame)
        else f"{label}: no rows",
    )

    key_duplicates = int(frame.duplicated(subset=KEY_COLUMNS).sum())
    checks.add(
        "month_key_unique",
        key_duplicates == 0,
        f"{label}: {key_duplicates} rows repeat a sensor and time"
        if key_duplicates
        else "every sensor and time appears once",
    )

    per_timestamp = frame.groupby("event_time").size()
    complete = (
        len(per_timestamp) > 0
        and int(per_timestamp.min()) == sensors_per_timestamp
        and int(per_timestamp.max()) == sensors_per_timestamp
    )
    checks.add(
        "month_timestamps_complete",
        complete,
        f"every timestamp has {sensors_per_timestamp} readings"
        if complete
        else f"{label}: readings per timestamp range from "
        f"{int(per_timestamp.min()) if len(per_timestamp) else 0} to "
        f"{int(per_timestamp.max()) if len(per_timestamp) else 0} "
        f"(expected {sensors_per_timestamp})",
    )

    unknown = ~frame["sensor_type"].isin(EXPECTED_UNITS)
    wrong_unit = ~unknown & (
        frame["sensor_type"].map(EXPECTED_UNITS) != frame["unit"]
    )
    checks.add(
        "month_units",
        not (unknown.any() or wrong_unit.any()),
        f"{label}: {int(unknown.sum())} unknown sensor types, "
        f"{int(wrong_unit.sum())} wrong units"
        if (unknown.any() or wrong_unit.any())
        else "units match sensor types",
    )

    finite = bool(np.isfinite(frame["value"].to_numpy(dtype=float)).all())
    checks.add(
        "month_values_finite",
        finite,
        f"{label}: contains missing, NaN or infinite values"
        if not finite
        else "all values finite",
    )

    return checks.items


def merge_results(result_lists: list[list[Result]]) -> list[Result]:
    """Combine per-month results into one result per check name.

    A check passes only if it passed in every list. The first failing
    detail is kept so the report points at a concrete problem.
    """

    merged: dict[str, Result] = {}
    counts: dict[str, int] = {}

    for results in result_lists:
        for result in results:
            name = result["name"]
            counts[name] = counts.get(name, 0) + 1

            if name not in merged:
                merged[name] = dict(result)
            elif merged[name]["passed"] and not result["passed"]:
                merged[name]["passed"] = False
                merged[name]["detail"] = result["detail"]

    for name, result in merged.items():
        if result["passed"] and counts[name] > 1:
            result["detail"] = f"{result['detail']} (all {counts[name]} months)"

    return list(merged.values())

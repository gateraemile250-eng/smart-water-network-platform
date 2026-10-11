"""Export the curated PostgreSQL tables to local Parquet files.

    python -m src.cloud.curated_export

The export is strictly read-only: one read-only, repeatable-read
transaction runs SELECT statements only, so every table comes from the
same snapshot, and the transaction is always rolled back. Nothing is
uploaded and the database is never modified.

Output files (Snappy-compressed Parquet, fixed column order):

    data/publish/curated/network_anomaly_results.parquet
    data/publish/curated/network_localization_results.parquet
    data/publish/curated/network_nodes.parquet
    data/publish/curated/network_links.parquet

Report and status files are written next to the Phase 4.3 reports, and
deliberately NOT inside curated/, so they can never be uploaded into a
table prefix by mistake:

    data/publish/reports/curated_report.json   every check and its detail
    data/publish/curated_status.json           counts, sizes, checksums

Geometry is exported as WKT text with ST_AsText. No SRID is added: the
BattLeDIM network uses local model coordinates (SRID 0). Node
coordinates are also stored as exact x and y doubles, and a check
confirms the WKT reproduces them exactly.

Columns renamed or dropped (as planned): "timestamp" is exported as
event_time, and created_at (a load-time artefact that would make the
output differ between runs) is not exported.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd

from src.anomaly_detection.baseline_detector import TOTAL_SENSORS
from src.cloud import parquet_io
from src.cloud.build import file_sha256
from src.cloud.layout import PUBLISH_DIR
from src.cloud.quality_gate import (
    ERROR,
    WARNING,
    Result,
    format_report,
    has_errors,
)

logger = logging.getLogger("smart_water.curated_export")

CURATED_DIR = PUBLISH_DIR / "curated"

TOP_N_CANDIDATES = 25

NODE_TYPES = {"junction", "reservoir", "tank"}
LINK_TYPES = {"pipe", "pump", "valve"}

# Statement that starts every export transaction.
SNAPSHOT_STATEMENT = (
    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
)

Writer = Callable[[pd.DataFrame, Path, Any], None]
Describer = Callable[[Path], dict[str, Any]]


@dataclass(frozen=True)
class TableSpec:
    """How one PostgreSQL table is exported."""

    table: str
    query: str
    types: dict[str, str]
    documented_rows: int

    @property
    def filename(self) -> str:
        return f"{self.table}.parquet"

    @property
    def columns(self) -> list[str]:
        return list(self.types)


TABLES: list[TableSpec] = [
    TableSpec(
        table="network_anomaly_results",
        query=(
            'SELECT "timestamp" AS event_time, baseline_sensor_count, '
            "baseline_sensor_fraction, is_baseline_anomaly, "
            "ml_anomaly_score, is_ml_anomaly "
            'FROM network_anomaly_results ORDER BY "timestamp"'
        ),
        types={
            "event_time": "timestamp[ms]",
            "baseline_sensor_count": "int64",
            "baseline_sensor_fraction": "double",
            "is_baseline_anomaly": "bool",
            "ml_anomaly_score": "double",
            "is_ml_anomaly": "bool",
        },
        documented_rows=101_088,
    ),
    TableSpec(
        table="network_localization_results",
        query=(
            'SELECT "timestamp" AS event_time, pipe_id, '
            "localization_score, candidate_rank "
            "FROM network_localization_results "
            'ORDER BY "timestamp", candidate_rank'
        ),
        types={
            "event_time": "timestamp[ms]",
            "pipe_id": "string",
            "localization_score": "double",
            "candidate_rank": "int64",
        },
        documented_rows=121_700,
    ),
    TableSpec(
        table="network_nodes",
        query=(
            "SELECT node_id, node_type, ST_X(geometry) AS x, "
            "ST_Y(geometry) AS y, ST_AsText(geometry) AS geometry_wkt "
            "FROM network_nodes ORDER BY node_id"
        ),
        types={
            "node_id": "string",
            "node_type": "string",
            "x": "double",
            "y": "double",
            "geometry_wkt": "string",
        },
        documented_rows=785,
    ),
    TableSpec(
        table="network_links",
        query=(
            "SELECT link_id, link_type, from_node_id, to_node_id, "
            "ST_AsText(geometry) AS geometry_wkt "
            "FROM network_links ORDER BY link_id"
        ),
        types={
            "link_id": "string",
            "link_type": "string",
            "from_node_id": "string",
            "to_node_id": "string",
            "geometry_wkt": "string",
        },
        documented_rows=909,
    ),
]

SPECS = {spec.table: spec for spec in TABLES}

# Figures recorded after the original validation run. A difference is
# reported as a WARNING, because the database may legitimately change.
DOCUMENTED = {
    "ml_anomalies": 5_897,
    "baseline_anomalies": 1_128,
    "node_types": {"junction": 782, "reservoir": 2, "tank": 1},
    "link_types": {"pipe": 905, "pump": 1, "valve": 3},
}

_POINT = re.compile(r"^POINT\(\s*(\S+)\s+(\S+)\s*\)$")


# --------------------------------------------------------------- database


def assert_read_only(sql: str) -> None:
    """Refuse any statement that is not a plain SELECT."""

    if not sql.lstrip().upper().startswith("SELECT"):
        raise ValueError(f"Only SELECT statements are allowed: {sql[:40]!r}")


def _execute(connection: Any, sql: str) -> tuple[list[str], list[tuple]]:
    """Run one SELECT and return its column names and rows."""

    assert_read_only(sql)
    cursor = connection.cursor()
    cursor.execute(sql)
    names = [
        item[0].decode() if isinstance(item[0], bytes) else item[0]
        for item in cursor.description
    ]

    return names, list(cursor.fetchall())


def begin_snapshot(connection: Any) -> None:
    """Make the current transaction read-only and consistent.

    PostgreSQL only warns if SET TRANSACTION runs outside a transaction,
    so the settings are read back and the export aborts unless the
    transaction really is read-only and repeatable-read.
    """

    connection.cursor().execute(SNAPSHOT_STATEMENT)

    _, rows = _execute(
        connection,
        "SELECT current_setting('transaction_read_only'), "
        "current_setting('transaction_isolation')",
    )
    read_only, isolation = rows[0]

    if read_only != "on" or isolation != "repeatable read":
        raise RuntimeError(
            "The export transaction is not read-only and repeatable-read "
            f"(read_only={read_only}, isolation={isolation}). Aborting "
            "before reading any data."
        )


def fetch_counts(connection: Any) -> dict[str, int]:
    """Count the rows of every exported table."""

    counts = {}

    for spec in TABLES:
        _, rows = _execute(connection, f"SELECT COUNT(*) FROM {spec.table}")
        counts[spec.table] = int(rows[0][0])

    return counts


def fetch_frame(connection: Any, spec: TableSpec) -> pd.DataFrame:
    """Read one table into a DataFrame."""

    names, rows = _execute(connection, spec.query)

    return pd.DataFrame(rows, columns=names)


# -------------------------------------------------------------- conversion


def _to_milliseconds(series: pd.Series) -> pd.Series:
    """Convert timestamps to millisecond precision without losing digits."""

    original = pd.to_datetime(series)
    converted = original.astype("datetime64[ms]")

    if (original != converted).any():
        raise ValueError(
            "Some timestamps have sub-millisecond precision, which Parquet "
            "milliseconds would silently truncate."
        )

    return converted


def normalise(frame: pd.DataFrame, spec: TableSpec) -> pd.DataFrame:
    """Give every column the exact dtype of its planned Parquet type.

    The driver's Python types are not relied on. Raises ValueError or
    TypeError if a value cannot be converted.
    """

    converters = {
        "timestamp[ms]": _to_milliseconds,
        "int64": lambda s: s.astype("int64"),
        "double": lambda s: s.astype("float64"),
        "bool": lambda s: s.astype(bool),
        "string": lambda s: s.astype(str),
    }

    return pd.DataFrame(
        {
            column: converters[arrow_type](frame[column])
            for column, arrow_type in spec.types.items()
        }
    )


def arrow_schema(spec: TableSpec) -> Any:
    """Build the Arrow schema for a table."""

    import pyarrow as pa

    mapping = {
        "timestamp[ms]": pa.timestamp("ms"),
        "int64": pa.int64(),
        "double": pa.float64(),
        "bool": pa.bool_(),
        "string": pa.string(),
    }

    return pa.schema(
        [(column, mapping[kind]) for column, kind in spec.types.items()]
    )


# -------------------------------------------------------------------- gate


def _add(
    results: list[Result],
    name: str,
    passed: bool,
    detail: str,
    severity: str = ERROR,
) -> None:
    results.append(
        {
            "name": name,
            "severity": severity,
            "passed": bool(passed),
            "detail": detail,
        }
    )


def _sample(values: Any, limit: int = 3) -> str:
    items = [str(value) for value in list(values)[:limit]]

    return ", ".join(items) if items else "none"


def check_structure(
    frames: dict[str, pd.DataFrame], counts: dict[str, int]
) -> list[Result]:
    """Columns, null values and row counts, before any conversion."""

    results: list[Result] = []

    for spec in TABLES:
        frame = frames[spec.table]
        label = spec.table

        columns_ok = list(frame.columns) == spec.columns
        _add(
            results,
            f"{label}_columns",
            columns_ok,
            f"columns {list(frame.columns)}, expected {spec.columns}"
            if not columns_ok
            else "columns as designed",
        )

        if not columns_ok:
            continue

        nulls = {
            column: int(frame[column].isna().sum())
            for column in spec.columns
            if frame[column].isna().any()
        }
        _add(
            results,
            f"{label}_no_nulls",
            not nulls,
            f"null values: {nulls}" if nulls else "no null values",
        )

        _add(
            results,
            f"{label}_rows_match_database",
            len(frame) == counts[spec.table],
            f"exported {len(frame)} rows, database counted "
            f"{counts[spec.table]}"
            if len(frame) != counts[spec.table]
            else f"{len(frame):,} rows, equal to the database count",
        )

        _add(
            results,
            f"{label}_rows_documented",
            len(frame) == spec.documented_rows,
            f"{len(frame):,} rows now, documented {spec.documented_rows:,}",
            severity=WARNING,
        )

    return results


def check_content(
    frames: dict[str, pd.DataFrame],
    top_n: int = TOP_N_CANDIDATES,
    documented: dict[str, Any] | None = DOCUMENTED,
) -> list[Result]:
    """Keys, ranges, geometry and cross-table consistency."""

    results: list[Result] = []
    anomalies = frames["network_anomaly_results"]
    localization = frames["network_localization_results"]
    nodes = frames["network_nodes"]
    links = frames["network_links"]

    # ---- anomaly results
    duplicates = int(anomalies["event_time"].duplicated().sum())
    _add(results, "anomaly_event_time_unique", duplicates == 0,
         f"{duplicates} repeated timestamps" if duplicates else "unique")

    numbers = ["baseline_sensor_fraction", "ml_anomaly_score"]
    bad = {
        c: int((~np.isfinite(anomalies[c].to_numpy())).sum())
        for c in numbers
        if not np.isfinite(anomalies[c].to_numpy()).all()
    }
    _add(results, "anomaly_scores_finite", not bad,
         f"non-finite values: {bad}" if bad else "all scores finite")

    fraction = anomalies["baseline_sensor_fraction"]
    count = anomalies["baseline_sensor_count"]
    _add(
        results,
        "anomaly_fraction_and_count_in_range",
        bool(fraction.between(0, 1).all() and count.between(0, TOTAL_SENSORS).all()),
        f"fraction must be 0 to 1 and count 0 to {TOTAL_SENSORS}; "
        f"found fraction {fraction.min()} to {fraction.max()}, "
        f"count {count.min()} to {count.max()}"
        if not (fraction.between(0, 1).all() and count.between(0, TOTAL_SENSORS).all())
        else "fraction and count within range",
    )

    consistent = bool(
        np.isclose(fraction, count / TOTAL_SENSORS, atol=1e-6).all()
    )
    _add(results, "anomaly_fraction_matches_count", consistent,
         f"fraction should equal count / {TOTAL_SENSORS}"
         if not consistent else f"fraction = count / {TOTAL_SENSORS}",
         severity=WARNING)

    outside = anomalies[anomalies["event_time"].dt.year != 2018]
    _add(results, "anomaly_timestamps_in_2018", outside.empty,
         f"{len(outside)} timestamps outside 2018, for example "
         f"{_sample(outside['event_time'])}" if not outside.empty
         else "all timestamps in 2018", severity=WARNING)

    if documented:
        found = int(anomalies["is_ml_anomaly"].sum())
        _add(results, "anomaly_ml_count_documented",
             found == documented["ml_anomalies"],
             f"{found:,} ML anomalies now, documented "
             f"{documented['ml_anomalies']:,}", severity=WARNING)

        found = int(anomalies["is_baseline_anomaly"].sum())
        _add(results, "anomaly_baseline_count_documented",
             found == documented["baseline_anomalies"],
             f"{found:,} baseline anomalies now, documented "
             f"{documented['baseline_anomalies']:,}", severity=WARNING)

    # ---- localization results
    repeated = int(localization.duplicated(["event_time", "pipe_id"]).sum())
    _add(results, "localization_key_unique", repeated == 0,
         f"{repeated} repeated (timestamp, pipe) pairs" if repeated
         else "each pipe appears once per timestamp")

    repeated_rank = int(
        localization.duplicated(["event_time", "candidate_rank"]).sum()
    )
    _add(results, "localization_rank_unique_per_event", repeated_rank == 0,
         f"{repeated_rank} repeated ranks within a timestamp" if repeated_rank
         else "ranks are unique within each timestamp")

    ranks = localization["candidate_rank"]
    ranks_ok = bool(ranks.between(1, top_n).all())
    _add(results, "localization_rank_range", ranks_ok,
         f"ranks must be 1 to {top_n}; found {ranks.min()} to {ranks.max()}"
         if not ranks_ok else f"ranks within 1 to {top_n}")

    scores = localization["localization_score"].to_numpy()
    scores_ok = bool(np.isfinite(scores).all() and (scores >= 0).all())
    _add(results, "localization_scores_valid", scores_ok,
         "scores must be finite and not negative" if not scores_ok
         else "scores finite and not negative")

    per_event = localization.groupby("event_time").size()
    _add(results, "localization_candidates_at_most_top_n",
         bool((per_event <= top_n).all()),
         f"a timestamp has {int(per_event.max())} candidates, limit {top_n}"
         if not (per_event <= top_n).all()
         else f"no timestamp has more than {top_n} candidates")
    _add(results, "localization_candidates_exactly_top_n",
         bool((per_event == top_n).all()),
         f"candidates per timestamp range from {int(per_event.min())} to "
         f"{int(per_event.max())}, documented {top_n}"
         if not (per_event == top_n).all()
         else f"every timestamp has exactly {top_n} candidates",
         severity=WARNING)

    link_ids = set(links["link_id"])
    unknown_pipes = sorted(set(localization["pipe_id"]) - link_ids)
    _add(results, "localization_pipes_exist_in_links", not unknown_pipes,
         f"{len(unknown_pipes)} pipe IDs are not in network_links, for "
         f"example {_sample(unknown_pipes)}" if unknown_pipes
         else "every candidate exists in network_links")

    pipe_ids = set(links.loc[links["link_type"] == "pipe", "link_id"])
    non_pipes = sorted((set(localization["pipe_id"]) & link_ids) - pipe_ids)
    _add(results, "localization_candidates_are_pipes", not non_pipes,
         f"{len(non_pipes)} candidates are not pipes, for example "
         f"{_sample(non_pipes)}" if non_pipes else "every candidate is a pipe")

    anomaly_times = set(anomalies["event_time"])
    orphan = ~localization["event_time"].isin(anomaly_times)
    _add(results, "localization_timestamps_exist_in_anomalies",
         not orphan.any(),
         f"{int(orphan.sum())} localization rows have no anomaly row"
         if orphan.any() else "every localized timestamp has an anomaly row")

    ml_times = set(anomalies.loc[anomalies["is_ml_anomaly"], "event_time"])
    not_ml = ~localization["event_time"].isin(ml_times)
    _add(results, "localization_only_for_ml_anomalies", not not_ml.any(),
         f"{int(not_ml.sum())} localization rows are for timestamps that are "
         "not ML anomalies" if not_ml.any()
         else "localization exists only for ML anomalies")

    # ---- network nodes
    repeated_nodes = int(nodes["node_id"].duplicated().sum())
    _add(results, "node_id_unique", repeated_nodes == 0,
         f"{repeated_nodes} repeated node IDs" if repeated_nodes
         else "node IDs are unique")

    point = nodes["geometry_wkt"].str.startswith("POINT(")
    _add(results, "node_geometry_is_point", bool(point.all()),
         f"{int((~point).sum())} nodes without POINT geometry, for example "
         f"{_sample(nodes.loc[~point, 'node_id'])}" if not point.all()
         else "every node has POINT geometry")

    coordinates = np.isfinite(nodes[["x", "y"]].to_numpy()).all()
    _add(results, "node_coordinates_finite", bool(coordinates),
         "node coordinates contain missing or infinite values"
         if not coordinates else "all node coordinates finite")

    parsed = nodes["geometry_wkt"].map(_wkt_point)
    exact = [
        point is not None and point == (x, y)
        for point, x, y in zip(parsed, nodes["x"], nodes["y"])
    ]
    _add(results, "node_wkt_matches_coordinates", all(exact),
         f"{exact.count(False)} node WKT values do not reproduce x and y "
         f"exactly, for example {_sample(nodes.loc[[not e for e in exact], 'node_id'])}"
         if not all(exact) else "node WKT reproduces x and y exactly")

    if documented:
        found = nodes["node_type"].value_counts().to_dict()
        _add(results, "node_type_counts_documented",
             found == documented["node_types"],
             f"node types now {found}, documented {documented['node_types']}",
             severity=WARNING)

    unknown_nodes = sorted(set(nodes["node_type"]) - NODE_TYPES)
    _add(results, "node_types_known", not unknown_nodes,
         f"unexpected node types: {unknown_nodes}" if unknown_nodes
         else "node types are junction, reservoir or tank",
         severity=WARNING)

    # ---- network links
    repeated_links = int(links["link_id"].duplicated().sum())
    _add(results, "link_id_unique", repeated_links == 0,
         f"{repeated_links} repeated link IDs" if repeated_links
         else "link IDs are unique")

    line = links["geometry_wkt"].str.startswith("LINESTRING(")
    _add(results, "link_geometry_is_linestring", bool(line.all()),
         f"{int((~line).sum())} links without LINESTRING geometry, for "
         f"example {_sample(links.loc[~line, 'link_id'])}" if not line.all()
         else "every link has LINESTRING geometry")

    node_ids = set(nodes["node_id"])
    dangling = ~(
        links["from_node_id"].isin(node_ids) & links["to_node_id"].isin(node_ids)
    )
    _add(results, "link_endpoints_exist_in_nodes", not dangling.any(),
         f"{int(dangling.sum())} links point to a node that does not exist, "
         f"for example {_sample(links.loc[dangling, 'link_id'])}"
         if dangling.any() else "every link joins two existing nodes")

    if documented:
        found = links["link_type"].value_counts().to_dict()
        _add(results, "link_type_counts_documented",
             found == documented["link_types"],
             f"link types now {found}, documented {documented['link_types']}",
             severity=WARNING)

    unknown_links = sorted(set(links["link_type"]) - LINK_TYPES)
    _add(results, "link_types_known", not unknown_links,
         f"unexpected link types: {unknown_links}" if unknown_links
         else "link types are pipe, pump or valve", severity=WARNING)

    # ---- geometry text
    with_srid = [
        spec_table
        for spec_table in ("network_nodes", "network_links")
        if frames[spec_table]["geometry_wkt"].str.upper().str.contains("SRID").any()
    ]
    _add(results, "geometry_has_no_srid_prefix", not with_srid,
         f"geometry text contains an SRID in: {with_srid}" if with_srid
         else "geometry is plain WKT; no SRID was added")

    return results


def _wkt_point(text: str) -> tuple[float, float] | None:
    """Parse 'POINT(x y)' into floats, or return None."""

    match = _POINT.match(text)

    if not match:
        return None

    try:
        return float(match.group(1)), float(match.group(2))
    except ValueError:
        return None


def column_ranges(frame: pd.DataFrame, spec: TableSpec) -> dict[str, list]:
    """Minimum and maximum of every numeric column (for the report)."""

    return {
        column: [float(frame[column].min()), float(frame[column].max())]
        for column, kind in spec.types.items()
        if kind in ("int64", "double") and len(frame)
    }


def check_readback(
    spec: TableSpec, frame: pd.DataFrame, info: dict[str, Any]
) -> list[Result]:
    """Compare a written Parquet file with the frame it came from."""

    results: list[Result] = []
    label = spec.table

    _add(results, f"{label}_parquet_rows", info["rows"] == len(frame),
         f"file has {info['rows']} rows, expected {len(frame)}"
         if info["rows"] != len(frame) else "rows read back match")
    _add(results, f"{label}_parquet_types", info["columns"] == spec.types,
         f"types {info['columns']}, expected {spec.types}"
         if info["columns"] != spec.types else "column types match the design")
    _add(results, f"{label}_parquet_snappy", info["compression"] == ["SNAPPY"],
         f"compression {info['compression']}"
         if info["compression"] != ["SNAPPY"] else "Snappy compression")

    return results


# ------------------------------------------------------------------ export


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=str)


def run_export(
    connection: Any,
    publish_dir: Path = PUBLISH_DIR,
    writer: Writer | None = None,
    describer: Describer | None = None,
    top_n: int = TOP_N_CANDIDATES,
    documented: dict[str, Any] | None = DOCUMENTED,
) -> dict[str, Any]:
    """Read the tables, run the gate, write Parquet and read it back.

    Nothing is written if any ERROR check fails.
    """

    publish_dir = Path(publish_dir)
    curated_dir = publish_dir / "curated"
    writer = writer or (
        lambda frame, path, schema: parquet_io.write_parquet(
            frame, path, schema=schema
        )
    )
    describer = describer or parquet_io.describe_parquet

    try:
        begin_snapshot(connection)
        counts = fetch_counts(connection)
        raw = {spec.table: fetch_frame(connection, spec) for spec in TABLES}
    finally:
        connection.rollback()

    results = check_structure(raw, counts)
    frames: dict[str, pd.DataFrame] = {}

    if not has_errors(results):
        try:
            frames = {
                spec.table: normalise(raw[spec.table], spec) for spec in TABLES
            }
        except (ValueError, TypeError) as error:
            _add(results, "dtype_conversion", False, str(error))

    if frames and not has_errors(results):
        results += check_content(frames, top_n, documented)

    status: dict[str, Any] = {
        "stage": "curated_export",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "passed": False,
        "tables": [],
        "total_rows": 0,
        "output_bytes": 0,
    }

    if frames and not has_errors(results):
        if curated_dir.exists():
            shutil.rmtree(curated_dir)

        for spec in TABLES:
            path = curated_dir / spec.filename
            writer(frames[spec.table], path, _schema_or_none(spec))
            info = describer(path)
            readback = check_readback(spec, frames[spec.table], info)
            results += readback

            status["tables"].append(
                {
                    "table": spec.table,
                    "path": str(path),
                    "rows": info["rows"],
                    "database_rows": counts[spec.table],
                    "bytes": path.stat().st_size,
                    "sha256": file_sha256(path),
                    "ranges": column_ranges(frames[spec.table], spec),
                }
            )

            if has_errors(readback):
                break

        status["total_rows"] = sum(t["rows"] for t in status["tables"])
        status["output_bytes"] = sum(t["bytes"] for t in status["tables"])
    else:
        logger.error("Curated export gate FAILED. Nothing was written.")

    status["passed"] = (
        not has_errors(results) and len(status["tables"]) == len(TABLES)
    )
    status["errors"] = sum(
        1 for r in results if not r["passed"] and r["severity"] == ERROR
    )
    status["warnings"] = [
        r["name"] for r in results if not r["passed"] and r["severity"] == WARNING
    ]

    _write_json(publish_dir / "reports" / "curated_report.json",
                {"status": status, "checks": results})
    _write_json(publish_dir / "curated_status.json", status)

    status["checks"] = results

    return status


def _schema_or_none(spec: TableSpec) -> Any:
    """Return the Arrow schema, or None where pyarrow is not installed."""

    try:
        return arrow_schema(spec)
    except ImportError:
        return None


def format_summary(status: dict[str, Any]) -> str:
    """Return the headline numbers of an export as readable text."""

    lines = []

    for table in status["tables"]:
        lines.append(
            f"{table['table']:<30} {table['rows']:>9,} rows "
            f"(database {table['database_rows']:,})  "
            f"{table['bytes'] / 1e6:6.2f} MB"
        )

    lines += [
        f"Total rows:               {status['total_rows']:,}",
        f"Parquet size:             {status['output_bytes'] / 1e6:,.2f} MB",
        f"Errors:                   {status['errors']}",
        f"Warnings:                 {status['warnings'] or 'none'}",
        f"PASSED THE GATE:          {'YES' if status['passed'] else 'NO'}",
    ]

    return "\n".join(lines)


def _safe_message(error: Exception) -> str:
    """Describe an error without ever revealing the database password."""

    message = f"{type(error).__name__}: {error}"
    password = os.environ.get("POSTGRES_PASSWORD", "")

    return message.replace(password, "***") if password else message


def main(connect: Callable[[], Any] | None = None) -> int:
    """Run the export from the command line.

    Credentials come from infrastructure/postgres/.env through
    src.data.database. They are never printed.
    """

    if connect is None:
        from src.data.database import get_database_connection as connect

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    try:
        connection = connect()

        try:
            status = run_export(connection)
        finally:
            connection.close()
    except Exception as error:  # noqa: BLE001 - report, never crash noisily
        print(f"Curated export failed: {_safe_message(error)}", file=sys.stderr)

        return 2

    print("\nCurated export report")
    print(format_report(status["checks"]))
    print("\nSummary")
    print(format_summary(status))
    print(f"\nReport: {PUBLISH_DIR / 'reports' / 'curated_report.json'}")

    return 0 if status["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())

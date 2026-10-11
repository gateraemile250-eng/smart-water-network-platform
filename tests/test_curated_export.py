"""Tests for the curated PostgreSQL export (src/cloud/curated_export.py).

A mocked connection replaces the database, and a stand-in writer
replaces the Parquet writer, so no PostgreSQL, pyarrow or AWS is needed.
"""

import json
import re
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

from cloud_helpers import failed_errors, failed_warnings
from src.anomaly_detection.baseline_detector import TOTAL_SENSORS
from src.cloud import curated_export as export
from src.cloud.curated_export import SPECS, TABLES

START = pd.Timestamp("2018-03-01 00:00")

# What the synthetic data below contains, in the same shape as
# export.DOCUMENTED, so a clean run produces no warnings.
SYNTH = {
    "ml_anomalies": 2,
    "baseline_anomalies": 2,
    "node_types": {"junction": 2, "tank": 1, "reservoir": 1},
    "link_types": {"pipe": 2, "pump": 1},
}


# ------------------------------------------------------ mocked database


def make_tables():
    """A small, consistent copy of the four tables, as driver rows."""

    nodes = [
        ("n1", "junction", 1.0, 2.0, "POINT(1 2)"),
        ("n2", "junction", 3.0, 4.0, "POINT(3 4)"),
        ("t1", "tank", 5.0, 6.0, "POINT(5 6)"),
        ("r1", "reservoir", 7.0, 8.0, "POINT(7 8)"),
    ]
    links = [
        ("p1", "pipe", "n1", "n2", "LINESTRING(1 2,3 4)"),
        ("p2", "pipe", "n2", "t1", "LINESTRING(3 4,5 6)"),
        ("pump1", "pump", "r1", "n1", "LINESTRING(7 8,1 2)"),
    ]

    counts = [0, 3, 0, 20, 119, 0]
    anomalies = [
        (
            (START + pd.Timedelta(hours=i)).to_pydatetime(),
            counts[i],
            counts[i] / TOTAL_SENSORS,
            counts[i] >= 15,
            0.1 * i,
            i in (1, 3),
        )
        for i in range(6)
    ]

    localization = []

    for i in (1, 3):
        stamp = (START + pd.Timedelta(hours=i)).to_pydatetime()

        for rank, pipe in enumerate(["p1", "p2"], start=1):
            localization.append((stamp, pipe, 1.0 / rank, rank))

    return {
        "network_anomaly_results": (
            ["event_time", "baseline_sensor_count", "baseline_sensor_fraction",
             "is_baseline_anomaly", "ml_anomaly_score", "is_ml_anomaly"],
            anomalies,
        ),
        "network_localization_results": (
            ["event_time", "pipe_id", "localization_score", "candidate_rank"],
            localization,
        ),
        "network_nodes": (
            ["node_id", "node_type", "x", "y", "geometry_wkt"], nodes,
        ),
        "network_links": (
            ["link_id", "link_type", "from_node_id", "to_node_id", "geometry_wkt"],
            links,
        ),
    }


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.description = None
        self._rows = []

    def execute(self, sql):
        text = " ".join(sql.split())
        self.connection.statements.append(text)

        if text.startswith("SET TRANSACTION"):
            self._rows, self.description = [], None
            return

        if text.startswith("SELECT current_setting"):
            self._rows = [self.connection.settings]
            self.description = [("read_only",), ("isolation",)]
            return

        match = re.match(r"SELECT COUNT\(\*\) FROM (\w+)$", text)

        if match:
            table = match.group(1)
            self._rows = [(self.connection.counts.get(
                table, len(self.connection.tables[table][1])),)]
            self.description = [("count",)]
            return

        for table, (columns, rows) in self.connection.tables.items():
            if f"FROM {table} " in text + " ":
                self._rows = rows
                self.description = [(name,) for name in columns]
                return

        raise AssertionError(f"unexpected SQL: {text}")

    def fetchall(self):
        return self._rows


class FakeConnection:
    def __init__(self, tables=None, counts=None):
        self.tables = tables or make_tables()
        self.counts = counts or {}
        self.statements = []
        self.settings = ("on", "repeatable read")
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


class FakeParquet:
    def __init__(self, rows_override=None):
        self.written = {}
        self.schemas = {}
        self.rows_override = rows_override

    def write(self, frame, path, schema):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"fake-" + str(len(frame)).encode())
        self.written[str(path)] = frame
        self.schemas[str(path)] = schema

    def describe(self, path):
        frame = self.written[str(path)]
        spec = next(s for s in TABLES if s.filename == path.name)

        return {
            "rows": self.rows_override if self.rows_override is not None else len(frame),
            "columns": dict(spec.types),
            "compression": ["SNAPPY"],
        }


def export_with(connection=None, fake=None, publish_dir=None, **kwargs):
    fake = fake or FakeParquet()

    return export.run_export(
        connection or FakeConnection(),
        publish_dir=publish_dir,
        writer=fake.write,
        describer=fake.describe,
        top_n=2,
        documented=SYNTH,
        **kwargs,
    ), fake


def frames_from(tables):
    return {
        spec.table: export.normalise(pd.DataFrame(tables[spec.table][1],
                                                  columns=tables[spec.table][0]), spec)
        for spec in TABLES
    }


def content_errors(frames):
    return failed_errors(export.check_content(frames, top_n=2, documented=SYNTH))


# ------------------------------------------------------------ read-only


def test_every_export_query_is_a_plain_select():
    for spec in TABLES:
        assert spec.query.lstrip().upper().startswith("SELECT")
        assert not re.search(
            r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT)\b",
            spec.query, re.IGNORECASE,
        )


def test_the_read_only_guard_refuses_anything_but_select():
    for sql in ("DELETE FROM network_nodes", "DROP TABLE network_links",
                "UPDATE network_nodes SET node_type = 'x'",
                "INSERT INTO network_nodes VALUES (1)"):
        try:
            export.assert_read_only(sql)
        except ValueError:
            continue

        raise AssertionError(f"not refused: {sql}")


def test_the_export_runs_in_a_read_only_snapshot_and_never_commits():
    connection = FakeConnection()

    with tempfile.TemporaryDirectory() as tmp:
        export_with(connection, publish_dir=Path(tmp))

    assert connection.statements[0] == export.SNAPSHOT_STATEMENT
    assert "READ ONLY" in connection.statements[0]
    assert "transaction_read_only" in connection.statements[1]
    assert all(s.startswith("SELECT") for s in connection.statements[1:])
    assert connection.commits == 0
    assert connection.rollbacks >= 1


def test_the_export_aborts_if_the_transaction_is_not_really_read_only():
    connection = FakeConnection()
    connection.settings = ("off", "read committed")

    with tempfile.TemporaryDirectory() as tmp:
        try:
            export_with(connection, publish_dir=Path(tmp))
        except RuntimeError as error:
            assert "not read-only" in str(error)
        else:
            raise AssertionError("the export should have aborted")

        assert not (Path(tmp) / "curated").exists()

    # No table was read, and the transaction was ended.
    assert not any("FROM network_" in s for s in connection.statements)
    assert connection.rollbacks >= 1 and connection.commits == 0


def test_the_export_aborts_if_the_isolation_level_is_wrong():
    connection = FakeConnection()
    connection.settings = ("on", "read committed")

    with tempfile.TemporaryDirectory() as tmp:
        try:
            export_with(connection, publish_dir=Path(tmp))
        except RuntimeError:
            return

    raise AssertionError("the export should have aborted")


def test_geometry_is_exported_as_plain_wkt_without_an_srid():
    for name in ("network_nodes", "network_links"):
        query = SPECS[name].query

        assert "ST_AsText(geometry) AS geometry_wkt" in query
        assert "ST_AsEWKT" not in query and "SRID" not in query.upper()


def test_the_database_is_never_modified_by_the_export():
    connection = FakeConnection()
    before = {name: list(rows) for name, (_, rows) in connection.tables.items()}

    with tempfile.TemporaryDirectory() as tmp:
        export_with(connection, publish_dir=Path(tmp))

    assert {n: list(r) for n, (_, r) in connection.tables.items()} == before


# ------------------------------------------------------------- the export


def test_a_valid_export_writes_the_four_approved_files():
    with tempfile.TemporaryDirectory() as tmp:
        status, fake = export_with(publish_dir=Path(tmp))
        curated = Path(tmp) / "curated"

        assert status["passed"] is True
        assert sorted(p.name for p in curated.iterdir()) == [
            "network_anomaly_results.parquet",
            "network_links.parquet",
            "network_localization_results.parquet",
            "network_nodes.parquet",
        ]


def test_row_counts_equal_the_database_counts():
    with tempfile.TemporaryDirectory() as tmp:
        status, _ = export_with(publish_dir=Path(tmp))
        rows = {t["table"]: (t["rows"], t["database_rows"]) for t in status["tables"]}

        assert rows == {
            "network_anomaly_results": (6, 6),
            "network_localization_results": (4, 4),
            "network_nodes": (4, 4),
            "network_links": (3, 3),
        }
        assert status["total_rows"] == 17


def test_exported_columns_follow_the_approved_design():
    with tempfile.TemporaryDirectory() as tmp:
        _, fake = export_with(publish_dir=Path(tmp))
        columns = {Path(p).name: list(f.columns) for p, f in fake.written.items()}

        assert "created_at" not in columns["network_anomaly_results.parquet"]
        assert columns["network_anomaly_results.parquet"][0] == "event_time"
        assert columns["network_nodes.parquet"] == ["node_id", "node_type", "x", "y", "geometry_wkt"]
        assert "geometry" not in columns["network_links.parquet"]


def test_the_data_types_are_exactly_the_planned_ones():
    with tempfile.TemporaryDirectory() as tmp:
        _, fake = export_with(publish_dir=Path(tmp))
        anomalies = next(f for p, f in fake.written.items() if "anomaly" in p)

        assert str(anomalies["event_time"].dtype) == "datetime64[ms]"
        assert str(anomalies["baseline_sensor_count"].dtype) == "int64"
        assert str(anomalies["ml_anomaly_score"].dtype) == "float64"
        assert str(anomalies["is_ml_anomaly"].dtype) == "bool"


def test_the_status_and_report_files_are_saved_with_checksums():
    with tempfile.TemporaryDirectory() as tmp:
        status, _ = export_with(publish_dir=Path(tmp))
        saved = json.loads((Path(tmp) / "curated_status.json").read_text())

        assert saved["passed"] is True
        assert all(len(t["sha256"]) == 64 for t in saved["tables"])
        assert (Path(tmp) / "reports" / "curated_report.json").exists()
        assert "host" not in json.dumps(saved).lower()


def test_old_curated_output_is_replaced():
    with tempfile.TemporaryDirectory() as tmp:
        stale = Path(tmp) / "curated" / "old.parquet"
        stale.parent.mkdir(parents=True)
        stale.write_bytes(b"old")

        export_with(publish_dir=Path(tmp))

        assert not stale.exists()


def test_the_summary_lists_every_table_and_the_verdict():
    with tempfile.TemporaryDirectory() as tmp:
        status, _ = export_with(publish_dir=Path(tmp))
        text = export.format_summary(status)

        assert all(spec.table in text for spec in TABLES)
        assert "PASSED THE GATE:          YES" in text


# ----------------------------------------------------- failing exports


def test_a_wrong_column_set_writes_nothing():
    tables = make_tables()
    columns, rows = tables["network_nodes"]
    tables["network_nodes"] = (columns[:-1], [r[:-1] for r in rows])

    with tempfile.TemporaryDirectory() as tmp:
        status, fake = export_with(FakeConnection(tables), publish_dir=Path(tmp))

        assert status["passed"] is False
        assert fake.written == {}
        assert "network_nodes_columns" in failed_errors(status["checks"])


def test_null_values_are_reported_and_nothing_is_written():
    tables = make_tables()
    columns, rows = tables["network_anomaly_results"]
    rows = list(rows)
    rows[0] = rows[0][:4] + (None,) + rows[0][5:]
    tables["network_anomaly_results"] = (columns, rows)

    with tempfile.TemporaryDirectory() as tmp:
        status, fake = export_with(FakeConnection(tables), publish_dir=Path(tmp))

        assert status["passed"] is False and fake.written == {}
        assert "network_anomaly_results_no_nulls" in failed_errors(status["checks"])


def test_a_row_count_that_differs_from_the_database_fails():
    connection = FakeConnection(counts={"network_links": 999})

    with tempfile.TemporaryDirectory() as tmp:
        status, fake = export_with(connection, publish_dir=Path(tmp))

        assert status["passed"] is False and fake.written == {}
        assert "network_links_rows_match_database" in failed_errors(status["checks"])


def test_a_parquet_readback_mismatch_fails_the_export_and_stops():
    with tempfile.TemporaryDirectory() as tmp:
        status, fake = export_with(fake=FakeParquet(rows_override=1), publish_dir=Path(tmp))

        assert status["passed"] is False
        assert "network_anomaly_results_parquet_rows" in failed_errors(status["checks"])
        # The export stops at the first table that does not read back.
        assert len(fake.written) == 1


def test_a_content_problem_in_the_database_data_writes_nothing():
    tables = make_tables()
    columns, rows = tables["network_localization_results"]
    rows = list(rows)
    rows[0] = (rows[0][0], "ghost", rows[0][2], rows[0][3])
    tables["network_localization_results"] = (columns, rows)

    with tempfile.TemporaryDirectory() as tmp:
        status, fake = export_with(FakeConnection(tables), publish_dir=Path(tmp))

        assert status["passed"] is False
        assert fake.written == {}
        assert not (Path(tmp) / "curated").exists()
        assert "localization_pipes_exist_in_links" in failed_errors(status["checks"])


def test_documented_count_differences_only_warn():
    connection = FakeConnection()

    with tempfile.TemporaryDirectory() as tmp:
        status, _ = export_with(connection, publish_dir=Path(tmp))

    assert status["passed"] is True
    assert "network_anomaly_results_rows_documented" in status["warnings"]


# -------------------------------------------------------- content rules


def test_valid_content_passes():
    assert content_errors(frames_from(make_tables())) == set()


def test_repeated_anomaly_timestamps_fail():
    tables = make_tables()
    columns, rows = tables["network_anomaly_results"]
    tables["network_anomaly_results"] = (columns, list(rows) + [rows[0]])

    assert "anomaly_event_time_unique" in content_errors(frames_from(tables))


def test_non_finite_scores_fail():
    frames = frames_from(make_tables())
    frames["network_anomaly_results"].loc[0, "ml_anomaly_score"] = np.inf

    assert "anomaly_scores_finite" in content_errors(frames)


def test_a_fraction_outside_zero_to_one_fails():
    frames = frames_from(make_tables())
    frames["network_anomaly_results"].loc[0, "baseline_sensor_fraction"] = 1.5

    assert "anomaly_fraction_and_count_in_range" in content_errors(frames)


def test_localization_rank_outside_the_limit_fails():
    frames = frames_from(make_tables())
    frames["network_localization_results"].loc[0, "candidate_rank"] = 9

    assert "localization_rank_range" in content_errors(frames)


def test_repeated_pipe_per_timestamp_fails():
    frames = frames_from(make_tables())
    loc = frames["network_localization_results"]
    frames["network_localization_results"] = pd.concat([loc, loc.iloc[[0]]], ignore_index=True)

    assert "localization_key_unique" in content_errors(frames)


def test_negative_localization_score_fails():
    frames = frames_from(make_tables())
    frames["network_localization_results"].loc[0, "localization_score"] = -0.1

    assert "localization_scores_valid" in content_errors(frames)


def test_a_candidate_that_is_not_in_the_links_table_fails():
    frames = frames_from(make_tables())
    frames["network_localization_results"].loc[0, "pipe_id"] = "ghost"

    assert "localization_pipes_exist_in_links" in content_errors(frames)


def test_a_candidate_that_is_not_a_pipe_fails():
    frames = frames_from(make_tables())
    frames["network_localization_results"].loc[0, "pipe_id"] = "pump1"

    assert "localization_candidates_are_pipes" in content_errors(frames)


def test_localization_for_a_non_anomaly_timestamp_fails():
    frames = frames_from(make_tables())
    frames["network_localization_results"].loc[0, "event_time"] = pd.Timestamp("2018-03-01 05:00")

    assert content_errors(frames) & {
        "localization_timestamps_exist_in_anomalies",
        "localization_only_for_ml_anomalies",
    }


def test_localization_only_for_ml_anomalies():
    frames = frames_from(make_tables())
    frames["network_localization_results"].loc[0, "event_time"] = pd.Timestamp("2018-03-01 00:00")

    assert "localization_only_for_ml_anomalies" in content_errors(frames)


def test_a_node_without_point_geometry_fails():
    frames = frames_from(make_tables())
    frames["network_nodes"].loc[0, "geometry_wkt"] = "LINESTRING(0 0,1 1)"

    assert "node_geometry_is_point" in content_errors(frames)


def test_a_link_without_linestring_geometry_fails():
    frames = frames_from(make_tables())
    frames["network_links"].loc[0, "geometry_wkt"] = "POINT(0 0)"

    assert "link_geometry_is_linestring" in content_errors(frames)


def test_a_link_to_a_missing_node_fails():
    frames = frames_from(make_tables())
    frames["network_links"].loc[0, "to_node_id"] = "nowhere"

    assert "link_endpoints_exist_in_nodes" in content_errors(frames)


def test_repeated_ids_fail():
    frames = frames_from(make_tables())
    frames["network_nodes"].loc[1, "node_id"] = "n1"
    frames["network_links"].loc[1, "link_id"] = "p1"

    assert {"node_id_unique", "link_id_unique"} <= content_errors(frames)


def test_non_finite_coordinates_fail():
    frames = frames_from(make_tables())
    frames["network_nodes"].loc[0, "x"] = np.nan

    assert "node_coordinates_finite" in content_errors(frames)


def test_an_srid_prefix_in_the_geometry_text_fails():
    frames = frames_from(make_tables())
    frames["network_nodes"].loc[0, "geometry_wkt"] = "SRID=0;POINT(1 2)"

    assert "geometry_has_no_srid_prefix" in content_errors(frames)


def test_unexpected_types_and_off_year_timestamps_only_warn():
    frames = frames_from(make_tables())
    frames["network_nodes"].loc[0, "node_type"] = "valve"
    frames["network_anomaly_results"].loc[0, "event_time"] = pd.Timestamp("2019-01-01")
    results = export.check_content(frames, top_n=2, documented=SYNTH)

    assert {"node_types_known", "anomaly_timestamps_in_2018"} <= failed_warnings(results)


# --------------------------------------------- coordinates, ranges, safety


def test_node_wkt_must_reproduce_the_exact_coordinates():
    frames = frames_from(make_tables())
    frames["network_nodes"].loc[0, "geometry_wkt"] = "POINT(1 2.5)"

    assert "node_wkt_matches_coordinates" in content_errors(frames)


def test_an_unparseable_node_wkt_fails():
    frames = frames_from(make_tables())
    frames["network_nodes"].loc[0, "geometry_wkt"] = "POINT(abc def)"

    assert "node_wkt_matches_coordinates" in content_errors(frames)


def test_coordinates_are_preserved_exactly_through_the_export():
    tables = make_tables()
    columns, rows = tables["network_nodes"]
    precise = (138.22, 1549.6400000000001)
    rows = [("n1", "junction", precise[0], precise[1],
             "POINT(138.22 1549.6400000000001)")] + list(rows[1:])
    tables["network_nodes"] = (columns, rows)

    with tempfile.TemporaryDirectory() as tmp:
        status, fake = export_with(FakeConnection(tables), publish_dir=Path(tmp))
        nodes = next(f for p, f in fake.written.items() if "nodes" in p)

        assert status["passed"] is True
        assert (nodes.loc[0, "x"], nodes.loc[0, "y"]) == precise


def test_changed_type_counts_only_warn():
    documented = dict(SYNTH, node_types={"junction": 782, "reservoir": 2, "tank": 1})
    results = export.check_content(
        frames_from(make_tables()), top_n=2, documented=documented
    )

    assert "node_type_counts_documented" in failed_warnings(results)
    assert not failed_errors(results)


def test_numeric_ranges_are_recorded_in_the_report():
    with tempfile.TemporaryDirectory() as tmp:
        status, _ = export_with(publish_dir=Path(tmp))
        anomaly = next(t for t in status["tables"] if t["table"] == "network_anomaly_results")
        localization = next(t for t in status["tables"] if t["table"] == "network_localization_results")

        assert anomaly["ranges"]["baseline_sensor_count"] == [0.0, 119.0]
        assert localization["ranges"]["candidate_rank"] == [1.0, 2.0]


def test_the_saved_files_never_contain_credentials():
    import os

    os.environ["POSTGRES_PASSWORD"] = "unit-test-secret"

    try:
        with tempfile.TemporaryDirectory() as tmp:
            export_with(publish_dir=Path(tmp))
            saved = "".join(
                p.read_text(errors="ignore")
                for p in Path(tmp).rglob("*.json")
            )

            assert "unit-test-secret" not in saved
            assert "password" not in saved.lower()
    finally:
        del os.environ["POSTGRES_PASSWORD"]


def test_a_connection_failure_is_reported_without_the_password():
    import contextlib
    import io
    import os

    os.environ["POSTGRES_PASSWORD"] = "unit-test-secret"

    def failing_connect():
        raise RuntimeError("could not log in with unit-test-secret")

    errors = io.StringIO()

    try:
        with contextlib.redirect_stderr(errors):
            code = export.main(connect=failing_connect)
    finally:
        del os.environ["POSTGRES_PASSWORD"]

    assert code == 2
    assert "Curated export failed" in errors.getvalue()
    assert "unit-test-secret" not in errors.getvalue()


def test_a_successful_run_returns_zero_and_closes_the_connection():
    import contextlib
    import io

    connection = FakeConnection()
    original = export.run_export

    with tempfile.TemporaryDirectory() as tmp:
        fake = FakeParquet()
        export.run_export = lambda conn: original(
            conn, publish_dir=Path(tmp), writer=fake.write, describer=fake.describe,
            top_n=2, documented=SYNTH,
        )

        try:
            with contextlib.redirect_stdout(io.StringIO()):
                code = export.main(connect=lambda: connection)
        finally:
            export.run_export = original

    assert code == 0 and connection.closed is True


def test_a_failed_gate_returns_a_non_zero_exit_code_and_names_the_check():
    import contextlib
    import io

    connection = FakeConnection(counts={"network_links": 999})
    original = export.run_export

    with tempfile.TemporaryDirectory() as tmp:
        fake = FakeParquet()
        export.run_export = lambda conn: original(
            conn, publish_dir=Path(tmp), writer=fake.write, describer=fake.describe,
            top_n=2, documented=SYNTH,
        )
        output = io.StringIO()

        try:
            with contextlib.redirect_stdout(output):
                code = export.main(connect=lambda: connection)
        finally:
            export.run_export = original

        assert code == 1
        assert "[FAIL] network_links_rows_match_database" in output.getvalue()
        assert fake.written == {}


def test_timestamps_with_sub_millisecond_precision_are_refused_not_truncated():
    tables = make_tables()
    columns, rows = tables["network_anomaly_results"]
    rows = list(rows)
    precise = pd.Timestamp("2018-03-01 00:00:00.000001").to_pydatetime()
    rows[0] = (precise,) + rows[0][1:]
    tables["network_anomaly_results"] = (columns, rows)

    with tempfile.TemporaryDirectory() as tmp:
        status, fake = export_with(FakeConnection(tables), publish_dir=Path(tmp))

        assert status["passed"] is False and fake.written == {}
        assert "dtype_conversion" in failed_errors(status["checks"])
        assert "sub-millisecond" in next(
            r["detail"] for r in status["checks"] if r["name"] == "dtype_conversion"
        )

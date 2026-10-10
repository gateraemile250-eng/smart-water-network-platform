"""Parquet reading and writing (Snappy compression).

pyarrow is imported inside the functions so the rest of the pipeline,
and its tests, work on machines where it is not installed.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

COMPRESSION = "snappy"
ROW_GROUP_SIZE = 500_000

# Expected Arrow types of the processed readings, as text.
PROCESSED_TYPES = {
    "event_time": "timestamp[ms]",
    "sensor_id": "string",
    "sensor_type": "string",
    "unit": "string",
    "value": "double",
}


def processed_schema() -> Any:
    """Return the Arrow schema of the processed readings table."""

    import pyarrow as pa

    return pa.schema(
        [
            ("event_time", pa.timestamp("ms")),
            ("sensor_id", pa.string()),
            ("sensor_type", pa.string()),
            ("unit", pa.string()),
            ("value", pa.float64()),
        ]
    )


def write_parquet(
    frame: pd.DataFrame,
    path: Path,
    schema: Any = None,
) -> None:
    """Write a DataFrame as one Snappy-compressed Parquet file.

    Timestamps are stored with millisecond precision, which Athena
    reads as a normal timestamp column.
    """

    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.Table.from_pandas(frame, schema=schema, preserve_index=False)

    path.parent.mkdir(parents=True, exist_ok=True)

    pq.write_table(
        table,
        path,
        compression=COMPRESSION,
        row_group_size=ROW_GROUP_SIZE,
        coerce_timestamps="ms",
        allow_truncated_timestamps=False,
        use_deprecated_int96_timestamps=False,
    )


def describe_parquet(path: Path) -> dict[str, Any]:
    """Return rows, column types, compression and size of a Parquet file."""

    import pyarrow.parquet as pq

    parquet_file = pq.ParquetFile(path)
    metadata = parquet_file.metadata
    schema = parquet_file.schema_arrow

    compression = {
        metadata.row_group(group).column(column).compression
        for group in range(metadata.num_row_groups)
        for column in range(metadata.num_columns)
    }

    return {
        "rows": metadata.num_rows,
        "row_groups": metadata.num_row_groups,
        "columns": {field.name: str(field.type) for field in schema},
        "compression": sorted(compression),
        "file_bytes": Path(path).stat().st_size,
        "created_by": metadata.created_by,
    }


def read_sample(path: Path, rows: int = 5) -> pd.DataFrame:
    """Read the first rows of a Parquet file with pandas."""

    import pyarrow.parquet as pq

    parquet_file = pq.ParquetFile(path)

    return parquet_file.read_row_group(0).slice(0, rows).to_pandas()

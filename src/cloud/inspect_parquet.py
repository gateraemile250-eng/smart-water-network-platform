"""Print what a Parquet file contains.

    python -m src.cloud.inspect_parquet <path> [--rows N]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from src.cloud import parquet_io


def main(argv: list[str] | None = None) -> int:
    """Describe one Parquet file and show its first rows."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path)
    parser.add_argument("--rows", type=int, default=5)
    args = parser.parse_args(argv)

    info = parquet_io.describe_parquet(args.path)

    print(f"File:         {args.path}")
    print(f"Rows:         {info['rows']:,}")
    print(f"Row groups:   {info['row_groups']}")
    print(f"Compression:  {', '.join(info['compression'])}")
    print(f"Size:         {info['file_bytes'] / 1e6:,.2f} MB")
    print("Columns:")

    for name, arrow_type in info["columns"].items():
        print(f"  {name:<28} {arrow_type}")

    print(f"\nFirst {args.rows} rows (read with pandas):")
    print(parquet_io.read_sample(args.path, args.rows).to_string(index=False))

    return 0


if __name__ == "__main__":
    sys.exit(main())

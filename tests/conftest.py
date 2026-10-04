"""Pytest configuration for the Smart Water test suite."""

# test_lake_dedup.py needs Spark and runs inside the Spark container
# (see its docstring), so pytest must not try to collect it.
collect_ignore = ["test_lake_dedup.py"]

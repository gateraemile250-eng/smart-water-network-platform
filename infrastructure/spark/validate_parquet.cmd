@echo off

docker run --rm ^
  -v "%cd%:/opt/project" ^
  -w /opt/project ^
  apache/spark:4.1.3-scala2.13-java17-python3-ubuntu ^
  /opt/spark/bin/spark-submit ^
  src/data/validate_parquet_storage.py
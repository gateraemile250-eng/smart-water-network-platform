@echo off

docker run --rm ^
  --network smart-water-network ^
  --env-file infrastructure/postgres/.env ^
  -e KAFKA_BOOTSTRAP_SERVERS=smart-water-kafka:29092 ^
  -e POSTGRES_HOST=smart-water-postgres ^
  -e POSTGRES_PORT=5432 ^
  -v "%cd%:/opt/project" ^
  -w /opt/project ^
  apache/spark:4.1.3-scala2.13-java17-python3-ubuntu ^
  /opt/spark/bin/spark-submit ^
  --conf spark.jars.ivy=/tmp/.ivy2 ^
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3,org.postgresql:postgresql:42.7.7 ^
  src/streaming/spark_stream_processor.py
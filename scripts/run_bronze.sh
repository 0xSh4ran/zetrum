#!/bin/bash
# =============================================================
# scripts/run_bronze.sh
# Submits the Zetrum Bronze Spark Structured Streaming job
# into the running Spark container.
#
# Usage:
#   chmod +x scripts/run_bronze.sh
#   ./scripts/run_bronze.sh
# =============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "========================================================"
echo " Zetrum Bronze Ingestion Job"
echo " Spark:  zetrum-spark-master"
echo " Jars:   Iceberg + Kafka + S3A (downloading if needed)"
echo "========================================================"

# Copy the Bronze job into the Spark container
docker cp "$PROJECT_ROOT/processing/spark/bronze/ingest_stream.py" \
    zetrum-spark-master:/opt/spark/work-dir/ingest_stream.py

echo "[+] Copied ingest_stream.py into Spark container"

# Submit the job
# Packages are downloaded once and cached in the container
docker exec zetrum-spark-master /opt/spark/bin/spark-submit \
    --master "spark://spark-master:7077" \
    --deploy-mode client \
    \
    --packages \
"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,\
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
org.apache.spark:spark-avro_2.12:3.5.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.618" \
    \
    --conf "spark.driver.host=spark-master" \
    --conf "spark.executor.memory=600m" \
    --conf "spark.driver.memory=512m" \
    --conf "spark.sql.shuffle.partitions=4" \
    \
    /opt/spark/work-dir/ingest_stream.py

echo "========================================================"
echo " Bronze job finished."
echo " Check MinIO at http://localhost:9001"
echo " Browse: zetrum-bronze bucket"
echo "========================================================"

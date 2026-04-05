#!/bin/bash
echo "========================================================"
echo " Zetrum Silver Cleanse Job"
echo " Date: ${1:-yesterday}"
echo "========================================================"

docker cp processing/spark/silver/cleanse_sessions.py \
    zetrum-spark-master:/opt/spark/work-dir/cleanse_sessions.py

docker exec zetrum-spark-master /opt/spark/bin/spark-submit \
    --master local[2] \
    --conf "spark.driver.host=spark-master" \
    --conf "spark.driver.memory=512m" \
    --conf "spark.executor.memory=800m" \
    --conf "spark.sql.shuffle.partitions=4" \
    --packages \
"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,\
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.618" \
    /opt/spark/work-dir/cleanse_sessions.py ${1:-}

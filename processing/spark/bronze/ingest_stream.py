"""
zetrum/processing/spark/bronze/ingest_stream.py

Zetrum Bronze Layer — Spark Structured Streaming Consumer
----------------------------------------------------------
Reads raw Avro clickstream events from Kafka topic,
deserializes them, and writes as Apache Iceberg tables
into the MinIO bronze bucket (S3-compatible).

Architecture:
    Kafka (zetrum.clickstream.raw)
        → Spark Structured Streaming
        → Deserialize Avro (strip Schema Registry 5-byte header)
        → Flatten into DataFrame
        → Write Iceberg table (MinIO: zetrum-bronze)

Run via spark-submit (see scripts/run_bronze.sh):
    spark-submit \
        --packages <see run_bronze.sh> \
        processing/spark/bronze/ingest_stream.py

Author: Zetrum Contributors
License: Apache 2.0
"""

import logging
import struct
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, IntegerType, DoubleType
)

# ──────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
log = logging.getLogger("zetrum.bronze")

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC             = "zetrum.clickstream.raw"
MINIO_ENDPOINT          = "http://minio:9000"
MINIO_ACCESS_KEY        = "zetrum_admin"
MINIO_SECRET_KEY        = "zetrum_secret_2024"
BRONZE_BUCKET           = "s3a://zetrum-bronze"
BRONZE_TABLE_PATH       = f"{BRONZE_BUCKET}/clickstream_events"
CHECKPOINT_PATH         = f"{BRONZE_BUCKET}/_checkpoints/clickstream"

# ──────────────────────────────────────────────
# AVRO SCHEMA (string form for from_avro)
# Must match ingestion/kafka/schemas/clickstream.avsc
# ──────────────────────────────────────────────
CLICKSTREAM_AVRO_SCHEMA = """
{
  "type": "record",
  "name": "ClickstreamEvent",
  "namespace": "io.zetrum.clickstream",
  "fields": [
    {"name": "event_id",            "type": "string"},
    {"name": "event_type",          "type": "string"},
    {"name": "event_timestamp",     "type": "long"},
    {"name": "ingestion_timestamp", "type": "long"},
    {"name": "user_id",             "type": ["null", "string"], "default": null},
    {"name": "session_id",          "type": "string"},
    {"name": "anonymous_id",        "type": "string"},
    {"name": "device", "type": {
      "type": "record", "name": "Device", "fields": [
        {"name": "device_type",       "type": "string"},
        {"name": "os",                "type": "string"},
        {"name": "browser",           "type": ["null", "string"], "default": null},
        {"name": "browser_version",   "type": ["null", "string"], "default": null},
        {"name": "app_version",       "type": ["null", "string"], "default": null},
        {"name": "screen_resolution", "type": ["null", "string"], "default": null}
      ]
    }},
    {"name": "geo", "type": {
      "type": "record", "name": "Geo", "fields": [
        {"name": "country",    "type": "string"},
        {"name": "region",     "type": ["null", "string"], "default": null},
        {"name": "city",       "type": ["null", "string"], "default": null},
        {"name": "timezone",   "type": "string"},
        {"name": "ip_address", "type": ["null", "string"], "default": null}
      ]
    }},
    {"name": "page", "type": {
      "type": "record", "name": "Page", "fields": [
        {"name": "url",      "type": "string"},
        {"name": "path",     "type": "string"},
        {"name": "referrer", "type": ["null", "string"], "default": null},
        {"name": "title",    "type": ["null", "string"], "default": null}
      ]
    }},
    {"name": "properties", "type": {
      "type": "record", "name": "EventProperties", "fields": [
        {"name": "product_id",       "type": ["null", "string"], "default": null},
        {"name": "product_name",     "type": ["null", "string"], "default": null},
        {"name": "category",         "type": ["null", "string"], "default": null},
        {"name": "price",            "type": ["null", "double"], "default": null},
        {"name": "search_query",     "type": ["null", "string"], "default": null},
        {"name": "scroll_depth_pct", "type": ["null", "int"],    "default": null},
        {"name": "click_element",    "type": ["null", "string"], "default": null},
        {"name": "error_message",    "type": ["null", "string"], "default": null},
        {"name": "order_value",      "type": ["null", "double"], "default": null}
      ]
    }},
    {"name": "source",         "type": "string"},
    {"name": "schema_version", "type": "string", "default": "1.0.0"}
  ]
}
"""


# ──────────────────────────────────────────────
# SPARK SESSION
# ──────────────────────────────────────────────
def create_spark_session() -> SparkSession:
    """
    Create a SparkSession configured for:
    - Iceberg catalog pointing to MinIO
    - S3A filesystem for MinIO access
    - Avro support
    """
    log.info("Creating Spark session...")

    spark = (
        SparkSession.builder
        .appName("zetrum-bronze-ingest")

        # ── Iceberg catalog config ──────────────────────────
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.zetrum",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.zetrum.type", "hadoop")
        .config("spark.sql.catalog.zetrum.warehouse", BRONZE_BUCKET)

        # ── S3A / MinIO config ──────────────────────────────
        .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        # ── Performance tuning for 8GB machine ─────────────
        .config("spark.executor.memory",          "800m")
        .config("spark.driver.memory",            "800m")
        .config("spark.sql.shuffle.partitions",   "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark session created successfully.")
    return spark


# ──────────────────────────────────────────────
# CREATE ICEBERG TABLE
# ──────────────────────────────────────────────
def create_bronze_table(spark: SparkSession):
    """
    Create the Iceberg bronze table if it doesn't exist.
    Partitioned by event_date for efficient time-range queries.
    """
    log.info("Ensuring bronze Iceberg table exists...")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS zetrum.clickstream_events (
            event_id            STRING,
            event_type          STRING,
            event_timestamp     TIMESTAMP,
            ingestion_timestamp TIMESTAMP,
            user_id             STRING,
            session_id          STRING,
            anonymous_id        STRING,
            device_type         STRING,
            os                  STRING,
            browser             STRING,
            browser_version     STRING,
            app_version         STRING,
            screen_resolution   STRING,
            country             STRING,
            region              STRING,
            city                STRING,
            timezone            STRING,
            page_url            STRING,
            page_path           STRING,
            referrer            STRING,
            page_title          STRING,
            product_id          STRING,
            product_name        STRING,
            category            STRING,
            price               DOUBLE,
            search_query        STRING,
            scroll_depth_pct    INT,
            click_element       STRING,
            error_message       STRING,
            order_value         DOUBLE,
            source              STRING,
            schema_version      STRING,
            event_date          DATE,
            kafka_partition     INT,
            kafka_offset        BIGINT
        )
        USING iceberg
        PARTITIONED BY (event_date)
        LOCATION '{BRONZE_TABLE_PATH}'
        TBLPROPERTIES (
            'write.format.default'       = 'parquet',
            'write.parquet.compression-codec' = 'snappy',
            'write.metadata.compression-codec' = 'gzip'
        )
    """)
    log.info("Bronze table ready.")


# ──────────────────────────────────────────────
# READ FROM KAFKA
# ──────────────────────────────────────────────
def read_kafka_stream(spark: SparkSession):
    """
    Read raw Avro messages from Kafka as a streaming DataFrame.
    Returns raw binary value + kafka metadata.
    """
    log.info(f"Connecting to Kafka: {KAFKA_BOOTSTRAP_SERVERS} | topic: {KAFKA_TOPIC}")

    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")   # Start from beginning for first run
        .option("maxOffsetsPerTrigger", 1000)     # Batch size cap
        .option("failOnDataLoss", "false")
        .load()
    )


# ──────────────────────────────────────────────
# DESERIALIZE + FLATTEN
# ──────────────────────────────────────────────
def transform(df):
    """
    1. Strip the 5-byte Schema Registry header (magic byte + schema ID)
    2. Deserialize Avro bytes using from_avro
    3. Flatten nested structs into columns
    4. Add derived columns (event_date, kafka metadata)
    """
    log.info("Applying transformations...")

    # Schema Registry prefixes every message with:
    # byte 0:    magic byte (0x00)
    # bytes 1-4: schema ID (4-byte big-endian int)
    # bytes 5+:  actual Avro payload
    # We slice off the first 5 bytes before deserializing
    stripped = df.withColumn(
        "avro_value",
        F.expr("substring(value, 6, length(value) - 5)")
    )

    # Deserialize Avro → nested struct
    from pyspark.sql.avro.functions import from_avro
    deserialized = stripped.withColumn(
        "event",
        from_avro(F.col("avro_value"), CLICKSTREAM_AVRO_SCHEMA)
    )

    # Flatten all nested fields into top-level columns
    flattened = deserialized.select(
        # Core event fields
        F.col("event.event_id"),
        F.col("event.event_type"),
        F.to_timestamp(F.col("event.event_timestamp")    / 1000).alias("event_timestamp"),
        F.to_timestamp(F.col("event.ingestion_timestamp")/ 1000).alias("ingestion_timestamp"),
        F.col("event.user_id"),
        F.col("event.session_id"),
        F.col("event.anonymous_id"),

        # Device
        F.col("event.device.device_type"),
        F.col("event.device.os"),
        F.col("event.device.browser"),
        F.col("event.device.browser_version"),
        F.col("event.device.app_version"),
        F.col("event.device.screen_resolution"),

        # Geo
        F.col("event.geo.country"),
        F.col("event.geo.region"),
        F.col("event.geo.city"),
        F.col("event.geo.timezone"),

        # Page
        F.col("event.page.url").alias("page_url"),
        F.col("event.page.path").alias("page_path"),
        F.col("event.page.referrer").alias("referrer"),
        F.col("event.page.title").alias("page_title"),

        # Properties
        F.col("event.properties.product_id"),
        F.col("event.properties.product_name"),
        F.col("event.properties.category"),
        F.col("event.properties.price"),
        F.col("event.properties.search_query"),
        F.col("event.properties.scroll_depth_pct"),
        F.col("event.properties.click_element"),
        F.col("event.properties.error_message"),
        F.col("event.properties.order_value"),

        # Source metadata
        F.col("event.source"),
        F.col("event.schema_version"),

        # Derived columns
        F.to_date(F.to_timestamp(F.col("event.event_timestamp") / 1000)).alias("event_date"),

        # Kafka metadata (useful for debugging & reprocessing)
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    )

    return flattened


# ──────────────────────────────────────────────
# WRITE TO ICEBERG
# ──────────────────────────────────────────────
def write_to_bronze(df):
    """
    Write the streaming DataFrame to the Iceberg bronze table.
    Uses micro-batch mode with a 30-second trigger interval.
    """
    log.info(f"Starting write stream to Iceberg: {BRONZE_TABLE_PATH}")

    return (
        df.writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(processingTime="30 seconds")   # Micro-batch every 30s
        .option("path", BRONZE_TABLE_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("Zetrum Bronze Ingestion Job starting")
    log.info(f"  Kafka:   {KAFKA_BOOTSTRAP_SERVERS}")
    log.info(f"  Topic:   {KAFKA_TOPIC}")
    log.info(f"  Output:  {BRONZE_TABLE_PATH}")
    log.info("=" * 60)

    spark = create_spark_session()

    try:
        create_bronze_table(spark)
        raw_df       = read_kafka_stream(spark)
        transformed  = transform(raw_df)
        query        = write_to_bronze(transformed)

        log.info("Streaming query started. Waiting for data...")
        log.info("Press Ctrl+C to stop gracefully.")
        query.awaitTermination()

    except KeyboardInterrupt:
        log.info("Interrupted — shutting down gracefully...")
    finally:
        spark.stop()
        log.info("Spark session stopped.")


if __name__ == "__main__":
    main()

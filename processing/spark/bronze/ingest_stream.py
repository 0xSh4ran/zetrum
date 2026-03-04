"""
zetrum/processing/spark/bronze/ingest_stream.py
Bronze Layer — Spark Structured Streaming Consumer
"""

import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    stream=sys.stdout
)
log = logging.getLogger("zetrum.bronze")

# ── CONFIG ──────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC             = "zetrum.clickstream.raw"
MINIO_ENDPOINT          = "http://minio:9000"
MINIO_ACCESS_KEY        = "zetrum_admin"
MINIO_SECRET_KEY        = "zetrum_secret_2024"
BRONZE_TABLE_PATH       = "s3a://zetrum-bronze/clickstream_events"
CHECKPOINT_PATH         = "s3a://zetrum-bronze/_checkpoints/clickstream"

CLICKSTREAM_AVRO_SCHEMA = """
{
  "type": "record",
  "name": "ClickstreamEvent",
  "namespace": "io.zetrum.clickstream",
  "fields": [
    {"name": "event_id",            "type": "string"},
    {"name": "event_type",          "type": "string"},
    {"name": "event_timestamp",     "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "ingestion_timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
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


def create_spark_session():
    from pyspark.sql import SparkSession
    log.info("Creating Spark session...")
    spark = (
        SparkSession.builder
        .appName("zetrum-bronze-ingest")
        # Iceberg
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.zetrum",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.zetrum.type", "hadoop")
        .config("spark.sql.catalog.zetrum.warehouse", "s3a://zetrum-bronze")
        # S3A / MinIO
        .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.fast.upload",            "true")
        # Perf
        .config("spark.executor.memory",         "800m")
        .config("spark.driver.memory",           "800m")
        .config("spark.sql.shuffle.partitions",  "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark session created successfully.")
    return spark


def test_minio_connection(spark):
    """Quick S3A connectivity check before starting the streaming job."""
    log.info("Testing MinIO S3A connectivity...")
    try:
        sc = spark.sparkContext
        hadoop_conf = sc._jsc.hadoopConfiguration()
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
            sc._jvm.java.net.URI.create("s3a://zetrum-bronze/"),
            hadoop_conf
        )
        exists = fs.exists(sc._jvm.org.apache.hadoop.fs.Path("s3a://zetrum-bronze/"))
        log.info(f"MinIO S3A connection: OK (bucket exists={exists})")
        return True
    except Exception as e:
        log.error(f"MinIO S3A connection FAILED: {e}")
        log.error("Check: is MinIO running? Is the bucket created? Are credentials correct?")
        return False


def create_bronze_table(spark):
    log.info("Ensuring bronze Iceberg table exists...")
    try:
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
                country             STRING,
                region              STRING,
                city                STRING,
                timezone            STRING,
                page_url            STRING,
                page_path           STRING,
                referrer            STRING,
                product_id          STRING,
                price               DOUBLE,
                search_query        STRING,
                scroll_depth_pct    INT,
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
        """)
        log.info("Bronze Iceberg table ready.")
    except Exception as e:
        log.error(f"Failed to create Iceberg table: {e}")
        raise


def read_kafka_stream(spark):
    log.info(f"Connecting to Kafka: {KAFKA_BOOTSTRAP_SERVERS} | topic: {KAFKA_TOPIC}")
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 1000)
        .option("failOnDataLoss", "false")
        .load()
    )


def transform(df):
    from pyspark.sql import functions as F
    from pyspark.sql.avro.functions import from_avro

    log.info("Applying Avro deserialization and flattening...")

    # Strip 5-byte Schema Registry header
    stripped = df.withColumn(
        "avro_value",
        F.expr("substring(value, 6, length(value) - 5)")
    )
    
    deserialized = stripped.withColumn(
    "event",
    from_avro(F.col("avro_value"), CLICKSTREAM_AVRO_SCHEMA,
              {"mode": "PERMISSIVE"})	
    #deserialized = stripped.withColumn(
        #"event",
        #from_avro(F.col("avro_value"), CLICKSTREAM_AVRO_SCHEMA)
    )

    return deserialized.select(
        F.col("event.event_id"),
        F.col("event.event_type"),
	F.col("event.event_timestamp").alias("event_timestamp"),
	F.col("event.ingestion_timestamp").alias("ingestion_timestamp"),
        #F.to_timestamp(F.col("event.event_timestamp")     / 1000).alias("event_timestamp"),
        #F.to_timestamp(F.col("event.ingestion_timestamp") / 1000).alias("ingestion_timestamp"),
        F.col("event.user_id"),
        F.col("event.session_id"),
        F.col("event.anonymous_id"),
        F.col("event.device.device_type"),
        F.col("event.device.os"),
        F.col("event.device.browser"),
        F.col("event.geo.country"),
        F.col("event.geo.region"),
        F.col("event.geo.city"),
        F.col("event.geo.timezone"),
        F.col("event.page.url").alias("page_url"),
        F.col("event.page.path").alias("page_path"),
        F.col("event.page.referrer").alias("referrer"),
        F.col("event.properties.product_id"),
        F.col("event.properties.price"),
        F.col("event.properties.search_query"),
        F.col("event.properties.scroll_depth_pct"),
        F.col("event.properties.order_value"),
        F.col("event.source"),
        F.col("event.schema_version"),
        F.to_date(F.col("event.event_timestamp")).alias("event_date"),
        #F.to_date(F.to_timestamp(F.col("event.event_timestamp") / 1000)).alias("event_date"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    )


def write_to_bronze(df):
    log.info(f"Starting write stream to: {BRONZE_TABLE_PATH}")
    return (
        df.writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(processingTime="15 minutes")
        .option("path", BRONZE_TABLE_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )


def main():
    log.info("=" * 60)
    log.info("Zetrum Bronze Ingestion Job starting")
    log.info(f"  Kafka:   {KAFKA_BOOTSTRAP_SERVERS}")
    log.info(f"  Topic:   {KAFKA_TOPIC}")
    log.info(f"  Output:  {BRONZE_TABLE_PATH}")
    log.info("=" * 60)

    spark = None
    try:
        spark = create_spark_session()

        # Test S3A before doing anything else
        if not test_minio_connection(spark):
            log.error("Aborting — cannot connect to MinIO")
            sys.exit(1)

        create_bronze_table(spark)
        raw_df      = read_kafka_stream(spark)
        transformed = transform(raw_df)
        query       = write_to_bronze(transformed)

        log.info("Streaming query started. Waiting for data...")
        log.info("Press Ctrl+C to stop gracefully.")
        query.awaitTermination()

    except KeyboardInterrupt:
        log.info("Interrupted — shutting down gracefully...")
    except Exception as e:
        log.error(f"FATAL ERROR: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            log.info("Spark session stopped.")


if __name__ == "__main__":
    main()

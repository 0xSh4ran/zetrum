"""
zetrum/processing/spark/silver/cleanse_sessions.py
Silver Layer — Batch Job
Reads from Bronze (zetrum_raw), cleanses, sessionizes, enriches
Writes to Silver (zetrum_silver)

Schedule: Daily at 2AM via Airflow
Input:    zetrum_raw.clickstream_events
Output:   zetrum_silver.clickstream_events
"""

import logging
import sys
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    stream=sys.stdout
)
log = logging.getLogger("zetrum.silver")

# ── CONFIG ──────────────────────────────────────────
MINIO_ENDPOINT   = "http://minio:9000"
MINIO_ACCESS_KEY = "zetrum_admin"
MINIO_SECRET_KEY = "zetrum_secret_2024"

BRONZE_TABLE = "zetrum_raw.clickstream_events"
SILVER_TABLE = "zetrum_silver.clickstream_events"
SILVER_PATH  = "s3a://zetrum-silver/clickstream_events"
CHECKPOINT   = "s3a://zetrum-silver/_checkpoints/silver"


def create_spark_session():
    from pyspark.sql import SparkSession
    log.info("Creating Spark session...")
    spark = (
        SparkSession.builder
        .appName("zetrum-silver-cleanse")
        # Bronze catalog
        .config("spark.sql.catalog.zetrum_raw",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.zetrum_raw.type", "hadoop")
        .config("spark.sql.catalog.zetrum_raw.warehouse",
                "s3a://zetrum-bronze")
        # Silver catalog
        .config("spark.sql.catalog.zetrum_silver",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.zetrum_silver.type", "hadoop")
        .config("spark.sql.catalog.zetrum_silver.warehouse",
                "s3a://zetrum-silver")
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
        # Iceberg extensions
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # Perf
        .config("spark.sql.shuffle.partitions",  "4")
        .config("spark.executor.memory",         "800m")
        .config("spark.driver.memory",           "512m")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark session created successfully.")
    return spark


def create_silver_table(spark):
    log.info("Ensuring Silver Iceberg table exists...")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            -- Core event fields (from Bronze)
            event_id              STRING,
            event_type            STRING,
            event_timestamp       TIMESTAMP,
            ingestion_timestamp   TIMESTAMP,

            -- User identity
            user_id               STRING,
            session_id            STRING,
            anonymous_id          STRING,

            -- Device
            device_type           STRING,
            os                    STRING,
            browser               STRING,

            -- Geo
            country               STRING,
            region                STRING,
            city                  STRING,
            timezone              STRING,

            -- Page
            page_url              STRING,
            page_path             STRING,
            referrer              STRING,

            -- Properties
            product_id            STRING,
            price                 DOUBLE,
            search_query          STRING,
            scroll_depth_pct      INT,
            order_value           DOUBLE,

            -- Meta
            source                STRING,
            schema_version        STRING,

            -- Partition key
            event_date            DATE,

            -- Silver enriched columns
            hour_of_day           INT,
            day_of_week           STRING,
            is_weekend            BOOLEAN,
            is_indian_user        BOOLEAN,
            device_category       STRING,

            -- Session metrics
            session_event_count   BIGINT,
            session_duration_secs BIGINT,
            is_bounce             BOOLEAN,
            session_start         TIMESTAMP,
            session_end           TIMESTAMP,

            -- Silver metadata
            processed_at          TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (event_date)
        LOCATION '{SILVER_PATH}'
        TBLPROPERTIES (
            'write.format.default'             = 'parquet',
            'write.parquet.compression-codec'  = 'snappy'
        )
    """)
    log.info("Silver table ready.")


def read_bronze(spark, process_date):
    """Read one day's Bronze data."""
    log.info(f"Reading Bronze data for date: {process_date}")
    df = spark.table(BRONZE_TABLE).filter(
        f"event_date = '{process_date}'"
    )
    count = df.count()
    log.info(f"Bronze records loaded: {count:,}")
    return df


def deduplicate(df):
    """Remove duplicate events based on event_id."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    log.info("Deduplicating on event_id...")

    before = df.count()

    # Keep the first occurrence of each event_id
    window = Window.partitionBy("event_id").orderBy("ingestion_timestamp")
    df = df.withColumn("_rn", F.row_number().over(window)) \
           .filter(F.col("_rn") == 1) \
           .drop("_rn")

    after = df.count()
    log.info(f"Deduplication: {before:,} → {after:,} "
             f"(removed {before - after:,} duplicates)")
    return df


def drop_nulls(df):
    """Drop records with null critical fields."""
    from pyspark.sql import functions as F

    log.info("Dropping null critical fields...")
    before = df.count()

    df = df.filter(
        F.col("event_id").isNotNull() &
        F.col("session_id").isNotNull() &
        F.col("event_type").isNotNull() &
        F.col("event_timestamp").isNotNull() &
        F.col("event_date").isNotNull()
    )

    after = df.count()
    log.info(f"Null drop: {before:,} → {after:,} "
             f"(removed {before - after:,} null records)")
    return df


def enrich(df):
    """Add derived columns."""
    from pyspark.sql import functions as F

    log.info("Enriching with derived columns...")

    df = df \
        .withColumn("hour_of_day",
                    F.hour("event_timestamp")) \
        .withColumn("day_of_week",
                    F.date_format("event_timestamp", "EEEE")) \
        .withColumn("is_weekend",
                    F.dayofweek("event_timestamp").isin([1, 7])) \
        .withColumn("is_indian_user",
                    F.col("country") == "IN") \
        .withColumn("device_category",
                    F.when(F.col("device_type") == "mobile",  "mobile")
                     .when(F.col("device_type") == "tablet",  "tablet")
                     .otherwise("desktop")) \
        .withColumn("processed_at",
                    F.current_timestamp())

    log.info("Enrichment complete.")
    return df


def sessionize(df):
    """
    Calculate session-level metrics:
    - session_event_count: how many events in this session
    - session_duration_secs: time from first to last event
    - session_start / session_end: timestamps
    - is_bounce: only 1 event in session
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    log.info("Calculating session metrics...")

    session_window = Window.partitionBy("session_id")

    df = df \
        .withColumn("session_event_count",
                    F.count("event_id").over(session_window)) \
        .withColumn("session_start",
                    F.min("event_timestamp").over(session_window)) \
        .withColumn("session_end",
                    F.max("event_timestamp").over(session_window)) \
        .withColumn("session_duration_secs",
                    (F.unix_timestamp("session_end") -
                     F.unix_timestamp("session_start"))) \
        .withColumn("is_bounce",
                    F.col("session_event_count") == 1)

    log.info("Sessionization complete.")
    return df


def write_silver(df, process_date):
    """Write to Silver Iceberg table."""
    from pyspark.sql import functions as F

    log.info(f"Writing Silver data for {process_date}...")

    # Select final Silver columns in correct order
    silver_df = df.select(
        "event_id", "event_type", "event_timestamp",
        "ingestion_timestamp", "user_id", "session_id",
        "anonymous_id", "device_type", "os", "browser",
        "country", "region", "city", "timezone",
        "page_url", "page_path", "referrer",
        "product_id", "price", "search_query",
        "scroll_depth_pct", "order_value",
        "source", "schema_version", "event_date",
        "hour_of_day", "day_of_week", "is_weekend",
        "is_indian_user", "device_category",
        "session_event_count", "session_duration_secs",
        "is_bounce", "session_start", "session_end",
        "processed_at"
    )

    count = silver_df.count()

    # Delete existing partition for this date (idempotent)
    try:
        spark_session = df.sql_ctx.sparkSession
        spark_session.sql(f"""
            DELETE FROM {SILVER_TABLE}
            WHERE event_date = '{process_date}'
        """)
        log.info(f"Deleted existing Silver partition: {process_date}")
    except Exception as e:
        log.info(f"No existing partition to delete: {e}")

    # Write
    silver_df.coalesce(1) \
             .writeTo(SILVER_TABLE) \
             .append()

    log.info(f"Silver write complete: {count:,} records for {process_date}")
    return count


def main():
    """
    Main entry point.
    Accepts optional date argument: python cleanse_sessions.py 2026-04-03
    Defaults to yesterday if no date provided.
    """
    # Determine which date to process
    if len(sys.argv) > 1:
        process_date = sys.argv[1]
    else:
        # Default: yesterday (sessions from yesterday are complete)
        process_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    log.info("=" * 60)
    log.info("Zetrum Silver Cleanse Job starting")
    log.info(f"  Processing date: {process_date}")
    log.info(f"  Input:  {BRONZE_TABLE}")
    log.info(f"  Output: {SILVER_TABLE}")
    log.info("=" * 60)

    spark = None
    try:
        spark = create_spark_session()
        create_silver_table(spark)

        # Pipeline
        bronze_df    = read_bronze(spark, process_date)
        deduped_df   = deduplicate(bronze_df)
        clean_df     = drop_nulls(deduped_df)
        enriched_df  = enrich(clean_df)
        sessionized  = sessionize(enriched_df)
        count        = write_silver(sessionized, process_date)

        log.info("=" * 60)
        log.info(f"Silver job complete! {count:,} records written.")
        log.info("=" * 60)

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

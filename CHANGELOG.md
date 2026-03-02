# Changelog

All notable changes to Zetrum will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Planned
- Bronze layer: Spark Structured Streaming consumer writing to Iceberg on MinIO
- Silver layer: Cleansing, deduplication, and sessionization jobs
- Gold layer: Facts and dimensions for analytics
- Apache Airflow DAGs for pipeline orchestration
- Great Expectations data quality suite
- Trino query layer over Iceberg tables
- Apache Superset dashboards
- Prometheus + Grafana monitoring stack

---

## [0.2.0] - 2026-03-02

### Added
- `infra/docker-compose.yml` — full local infrastructure stack including:
  - Apache Zookeeper
  - Apache Kafka (Confluent 7.6.0)
  - Confluent Schema Registry
  - MinIO S3-compatible object storage (with auto-created bronze/silver/gold buckets)
  - Apache Spark Master + Worker (3.5.0)
  - Kafka UI for topic monitoring
- `ingestion/kafka/schemas/clickstream.avsc` — Avro schema for clickstream events with:
  - 13 event types (page_view, click, add_to_cart, checkout_complete, etc.)
  - Device metadata (type, OS, browser, app version)
  - Geo metadata (country, region, city, timezone, hashed IP)
  - Page metadata (URL, path, referrer, title)
  - Event-specific properties (product, search, scroll, order value)
  - Schema versioning support
- `ingestion/kafka/producers/clickstream_producer.py` — realistic clickstream event generator:
  - Weighted event type distribution (mimics real traffic patterns)
  - 200 simulated users with session management
  - India-first geo data (Bengaluru, Mumbai, Delhi)
  - Avro serialization via Schema Registry
  - Configurable event count and interval via CLI args

---

## [0.1.0] - 2026-02-28

### Added
- Initial project structure with 49 directories
- Apache 2.0 LICENSE
- README.md with project description
- CONTRIBUTING.md
- CODE_OF_CONDUCT.md
- SECURITY.md
- .gitignore
- GitHub Actions CI workflow scaffold
- Documentation structure (architecture, design, runbooks)
- Empty scaffolds for all pipeline layers (ingestion, processing, orchestration, warehouse, analytics, monitoring)

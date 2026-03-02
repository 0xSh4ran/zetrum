# Zetrum — Clickstream Data Platform

> An end-to-end open-source clickstream data platform built on Linux using Apache Kafka, Apache Spark, Apache Iceberg, Apache Airflow, and Trino.

![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)
![Status](https://img.shields.io/badge/status-active%20development-orange.svg)
![Stack](https://img.shields.io/badge/stack-Kafka%20%7C%20Spark%20%7C%20Iceberg%20%7C%20MinIO%20%7C%20Trino-informational)

---

## What is Zetrum?

Zetrum is a fully open-source, production-style clickstream data platform that ingests live user interaction events (clicks, page views, searches, purchases) from web and mobile sources, processes them through a Bronze → Silver → Gold lakehouse architecture, and delivers analytics-ready datasets to BI tools.

It is built entirely on Apache-licensed, Linux-native tools — making it cloud-agnostic and deployable anywhere.

**This project maps directly to enterprise AWS stacks:**

| Zetrum (Open Source) | AWS Equivalent |
|---|---|
| Apache Kafka | Amazon Kinesis |
| MinIO | Amazon S3 |
| Apache Iceberg | AWS Glue Data Catalog / Lake Formation |
| Apache Spark | AWS Glue / Amazon EMR |
| Trino | Amazon Athena |
| Apache Airflow | Amazon MWAA |
| Apache Superset | Amazon QuickSight |

---

## Architecture

```
Event Sources (Web / Mobile / Backend)
            │
            ▼
    ┌──────────────┐
    │ Apache Kafka │  ← Real-time event streaming
    └──────┬───────┘
           │
           ▼
    ┌──────────────────────────────────────┐
    │           Lakehouse Storage          │
    │  ┌─────────┐ ┌────────┐ ┌────────┐  │
    │  │ Bronze  │ │ Silver │ │  Gold  │  │
    │  │  Raw    │ │Cleansed│ │ Facts  │  │
    │  │ Iceberg │ │Iceberg │ │Iceberg │  │
    │  └─────────┘ └────────┘ └────────┘  │
    │         MinIO (S3-compatible)        │
    └──────────────────────────────────────┘
           │
           ▼
    ┌──────────────┐     ┌──────────────────┐
    │    Trino     │────▶│ Apache Superset  │
    │ Query Engine │     │   Dashboards     │
    └──────────────┘     └──────────────────┘
           │
    ┌──────────────┐
    │  PostgreSQL  │  ← Data Marts
    └──────────────┘

Orchestration: Apache Airflow
Quality:       Great Expectations
Monitoring:    Prometheus + Grafana
```

---

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Ingestion | Apache Kafka + Schema Registry | Real-time event streaming with Avro schemas |
| Storage | MinIO | S3-compatible object storage |
| Table Format | Apache Iceberg | ACID transactions, schema evolution, time travel |
| Processing | Apache Spark (PySpark) | Batch and streaming data transformation |
| Orchestration | Apache Airflow | Pipeline scheduling and workflow management |
| Query Engine | Trino | Federated SQL analytics across the lakehouse |
| Data Quality | Great Expectations | Automated data validation and profiling |
| Data Marts | PostgreSQL | Business-facing aggregated datasets |
| Visualization | Apache Superset | Interactive dashboards and BI |
| Monitoring | Prometheus + Grafana | Pipeline health and alerting |
| CI/CD | GitHub Actions | Automated testing and deployment |

---

## Project Structure

```
zetrum/
├── ingestion/
│   ├── kafka/
│   │   ├── producers/          # Clickstream event producers
│   │   └── schemas/            # Avro schemas (.avsc)
│   └── batch/                  # Batch file ingestion jobs
├── processing/
│   └── spark/
│       ├── bronze/             # Raw ingestion from Kafka → Iceberg
│       ├── silver/             # Cleansing, deduplication, sessionization
│       └── gold/               # Aggregations, facts & dimensions
├── orchestration/
│   └── airflow/
│       └── dags/               # Airflow DAGs for pipeline scheduling
├── warehouse/
│   └── iceberg/
│       ├── ddl/                # Table creation scripts
│       ├── schemas/            # Schema definitions
│       └── migrations/         # Schema migration scripts
├── analytics/
│   ├── trino/                  # Trino query templates
│   └── superset/               # Dashboard configurations
├── monitoring/
│   ├── prometheus/             # Metrics collection config
│   └── grafana/                # Dashboard definitions
├── infra/
│   ├── docker/                 # Per-service Dockerfiles
│   ├── docker-compose.yml      # Full local stack
│   └── scripts/                # Setup and teardown scripts
├── ci/
│   └── github/workflows/       # GitHub Actions CI pipelines
└── docs/
    ├── architecture/           # Architecture diagrams and data flow
    ├── design/                 # Design decisions and data model docs
    └── runbooks/               # Operational runbooks
```

---

## Quick Start

### Prerequisites

- Ubuntu 22.04+ (or any Linux distro)
- Docker + Docker Compose v2
- Python 3.10+
- 8GB RAM minimum (16GB recommended for full stack)

### 1. Clone the repo

```bash
git clone https://github.com/0xSh4ran/zetrum.git
cd zetrum
```

### 2. Start the infrastructure

```bash
docker compose -f infra/docker-compose.yml up -d
```

This starts: Zookeeper, Kafka, Schema Registry, MinIO, Spark Master + Worker, and Kafka UI.

### 3. Create storage buckets

```bash
docker exec -it zetrum-minio bash -c "
  mc alias set zetrum http://localhost:9000 zetrum_admin zetrum_secret_2024 &&
  mc mb zetrum/zetrum-bronze &&
  mc mb zetrum/zetrum-silver &&
  mc mb zetrum/zetrum-gold
"
```

### 4. Install Python dependencies

```bash
pip install confluent-kafka==2.3.0 fastavro faker httpx authlib cachetools --user
```

### 5. Run the clickstream producer

```bash
python3 ingestion/kafka/producers/clickstream_producer.py --events 100 --interval 0.2
```

### 6. Monitor in Kafka UI

Open **http://localhost:8090** → Topics → `zetrum.clickstream.raw`

---

## UI Endpoints

| Service | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8090 | None |
| Spark Master | http://localhost:8080 | None |
| MinIO Console | http://localhost:9001 | zetrum_admin / zetrum_secret_2024 |
| Schema Registry | http://localhost:8081 | None |

---

## Roadmap

- [x] Project structure and Apache 2.0 licensing
- [x] Avro schema design for clickstream events
- [x] Kafka producer with realistic event simulation
- [x] Docker Compose local infrastructure stack
- [ ] Bronze layer — Spark Structured Streaming → Iceberg
- [ ] Silver layer — Cleansing, deduplication, sessionization
- [ ] Gold layer — Facts, dimensions, KPI aggregations
- [ ] Airflow DAGs for pipeline orchestration
- [ ] Great Expectations data quality checks
- [ ] Trino query layer
- [ ] Apache Superset dashboards
- [ ] Prometheus + Grafana monitoring
- [ ] CI/CD with GitHub Actions

---

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) before submitting a pull request.

---

## License

This project is licensed under the Apache License 2.0 — see the [LICENSE](LICENSE) file for details.

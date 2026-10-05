# Sokti — Production-Style Local-First OTT Data Platform

**Sokti** is an end-to-end modern streaming, analytics, and AI data platform built for a high-concurrency OTT media streaming service (comparable to Netflix / Disney+ Hotstar).

It demonstrates realistic production data architecture, streaming data engineering, change data capture (CDC), OLAP modeling, vector embeddings, RAG, batch orchestration, governance, observability, and self-healing recovery patterns. The system is 100% runnable locally via Docker Compose, while adhering strictly to cloud-native production standards.

---

## 1. System Architecture

```mermaid
flowchart TD
    subgraph Client Layer
        Web["Web & Mobile Apps"]
        Sim["Event Simulator (apps/event-generator)"]
    end

    subgraph Ingestion & CDC
        OLTP[("PostgreSQL OLTP (Users/Subs): 15432")]
        Debezium["Debezium CDC: 8083"]
        Kafka[("Apache Kafka KRaft: 9092")]
        SR["Schema Registry: 8081"]
    end

    subgraph Streaming & Lakehouse
        Stream["Streaming Engine (Flink/Python)"]
        DLQ["Dead Letter Queue (ott.playback.dlq.v1)"]
        MinIO[("MinIO Lakehouse (Parquet/Iceberg): 9002")]
    end

    subgraph OLAP & Data Modeling
        ClickHouse[("ClickHouse OLAP: 8123 / 9000")]
        dbt["dbt Core (13 Models, 17 Tests)"]
        Marts[("Marts: Churn, Retention, Metrics")]
    end

    subgraph AI & Semantic Search
        Mongo[("MongoDB Catalog: 27018")]
        Chunker["Semantic Chunker"]
        FastEmbed["FastEmbed BAAI/bge-small-en-v1.5"]
        PGVector[("pgvector HNSW: 15433")]
        SearchAPI["Semantic Search & RAG API: 8000"]
    end

    subgraph Batch Orchestration & Observability
        Airflow[("Apache Airflow: 8085")]
        DQ["Data Quality Engine & Quarantine"]
        Prom["Prometheus: 9090"]
        Graf["Grafana Dashboards: 3000"]
    end

    Client Layer -->|Playback & Search Events| Kafka
    OLTP -->|WAL Logical Decoding| Debezium --> Kafka
    Kafka <-->|Avro Validation| SR
    Kafka --> Stream --> ClickHouse
    Stream -->|Corrupted Events| DLQ
    Kafka --> MinIO
    ClickHouse --> dbt --> Marts
    Mongo --> Chunker --> FastEmbed --> PGVector
    PGVector --> SearchAPI
    Marts --> SearchAPI
    Airflow --> dbt
    Airflow --> DQ --> Prom --> Graf
```

---

## 2. Service Endpoints & Credentials

All services are containerized in `docker-compose.yml` and isolated on Docker network `sokti-net`:

| Service | Internal Port | Host Port | Web UI / Healthcheck | Credentials |
| :--- | :--- | :--- | :--- | :--- |
| **PostgreSQL (OLTP)** | `5432` | `15432` | `localhost:15432` (`pg_isready`) | `postgres` / `postgres` (`db: sokti`) |
| **PostgreSQL (pgvector)** | `5432` | `15433` | `localhost:15433` (`pg_isready`) | `postgres` / `postgres` (`db: postgres`) |
| **MongoDB (Catalog)** | `27017` | `27018` | `localhost:27018` (`mongosh`) | `admin` / `admin` (`db: sokti_metadata`) |
| **Apache Kafka (KRaft)** | `29092` | `9092` | `localhost:9092` | Plaintext |
| **Confluent Schema Registry** | `8081` | `8081` | [http://localhost:8081/subjects](http://localhost:8081/subjects) | None |
| **Kafka Connect (Debezium)** | `8083` | `8083` | [http://localhost:8083/connectors](http://localhost:8083/connectors) | None |
| **ClickHouse HTTP** | `8123` | `8123` | [http://localhost:8123/ping](http://localhost:8123/ping) | `default` / `sokti_pass` (`db: sokti`) |
| **ClickHouse Native** | `9000` | `9000` | `localhost:9000` | `default` / `sokti_pass` |
| **MinIO S3 API** | `9000` | `9002` | [http://localhost:9002/minio/health/live](http://localhost:9002/minio/health/live) | `minioadmin` / `minioadmin` |
| **MinIO Console** | `9001` | `9001` | [http://localhost:9001](http://localhost:9001) | `minioadmin` / `minioadmin` |
| **Apache Airflow** | `8080` | `8085` | [http://localhost:8085](http://localhost:8085) | `admin` / `admin` |
| **Prometheus** | `9090` | `9090` | [http://localhost:9090/-/healthy](http://localhost:9090/-/healthy) | None |
| **Grafana** | `3000` | `3000` | [http://localhost:3000](http://localhost:3000) | `admin` / `admin` |
| **Semantic Search & RAG API**| — | `8000` | `http://localhost:8000/docs` | None |
| **Core Platform API** | — | `8001` | `http://localhost:8001/docs` | None |

---

## 3. Quickstart Guide

### 1. Bootstrap All Services
```bash
make up
# or: docker compose up -d
```
Verify that all 11 containers achieve `healthy` status (`docker compose ps`).

### 2. Seed Relational & Document Data
Populate 1,000 synthetic users, devices, subscriptions in PostgreSQL and 110+ movies and series in MongoDB:
```bash
make seed
```

### 3. Register Avro Schemas & CDC Connectors
```bash
python ingestion/schemas/register_schemas.py
python cdc/debezium/register_connector.py
```

### 4. Run Event Generator
Simulate realistic user streaming behavior (100 EPS with traffic spikes and 1% anomaly injection):
```bash
make generate-events
# or: python apps/event-generator/generate.py --users 1000 --events-per-second 100
```

### 5. Execute dbt Analytics Pipeline
Compile and execute the 13 staging, intermediate, and analytical mart models:
```bash
make dbt-run
make dbt-test
```

### 6. Synchronize AI Vector Embeddings
Extract metadata from MongoDB, generate 384d FastEmbed embeddings, and populate pgvector with HNSW index:
```bash
make vector-sync
```

### 7. Run Full Automated Test Suite
```bash
make test
```

---

## 4. Key Subsystem Highlights

### Change Data Capture (Debezium + PostgreSQL WAL)
- Captures row-level mutations on `users`, `subscriptions`, and `plans`.
- Integration verified via `tests/integration/test_cdc_pipeline.py`.

### Streaming Processing & Resilience
- Stateful windowing, out-of-order watermarking (15m tolerance), LRU deduplication by `event_id`, and poison-pill DLQ isolation to `ott.playback.dlq.v1`.
- Simulated via `python scripts/inject_failure.py --type duplicate` and `--type poison-pill`.

### OLAP & Table Design (ClickHouse)
- MergeTree engines with date partitioning, sparse primary indexing, and TTL eviction policies.
- Benchmark results documented in [docs/clickhouse_benchmarks.md](docs/clickhouse_benchmarks.md).

### Data Lakehouse & Event Replay (MinIO S3)
- Stores raw events in date-partitioned Parquet files (`date=YYYY-MM-DD/hour=HH/`).
- Historical replay verified via `python data_lake/replay_events.py --date 2026-10-05`.

### AI Semantic Search & RAG
- **Semantic Search API** (`POST /search/semantic`): Vector cosine search using pgvector HNSW index.
- **RAG Generation** (`POST /rag/ask`): Grounded, factual OTT recommendations synthesized from retrieved catalog chunks.
- **Personalized Recommendations** (`GET /recommendations/{user_id}`): Combines ClickHouse analytical churn features with vector similarity.

### Data Governance (DPDP Act & GDPR)
- PII catalog defined in `governance/pii_catalog.yaml`.
- Real-time masking for emails and phone numbers via `governance/masking/pii_masker.py`.
- Right-to-be-forgotten deletion script (`python scripts/delete_user.py --user-id <UUID>`) purging data across PostgreSQL, ClickHouse, and Kafka.

### AI-Assisted Engineering Evidence
A complete, unvarnished breakdown of where naive LLM generation failed and the production engineering interventions required is documented in [docs/ai-assisted-development.md](docs/ai-assisted-development.md).

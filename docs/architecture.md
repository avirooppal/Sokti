# Sokti Data Platform Architecture

Sokti is a production-grade, local-first data platform built for a modern OTT / media streaming enterprise. It provides real-time event ingestion, stateful stream processing, CDC change replication, analytical data modeling, object storage lakehouse archival, semantic search & RAG, data governance, and operational observability.

---

## 1. High-Level System Architecture

```mermaid
flowchart TD
    subgraph Client Layer
        Web[Web Browser]
        Mobile[iOS / Android Mobile]
        TV[Smart TV Client]
    end

    subgraph Operational Services
        CoreAPI[Core Platform API: 8001]
        OLTP[(PostgreSQL OLTP: 15432)]
        Mongo[(MongoDB Metadata: 27018)]
    end

    subgraph Streaming & CDC Ingestion
        Debezium[Debezium CDC: 8083]
        Generator[Synthetic Event Simulator]
        Kafka[(Apache Kafka KRaft: 9092)]
        SR[Confluent Schema Registry: 8081]
        StreamEngine[Flink / Real-Time Processor]
    end

    subgraph Analytical Storage & Marts
        ClickHouse[(ClickHouse OLAP: 8123 / 9000)]
        dbt[dbt Core 1.12 ClickHouse]
        Marts[(Analytical Marts: Churn, Engagement)]
    end

    subgraph Lakehouse & Historical Archival
        MinIO[(MinIO S3 Object Storage: 9002)]
        ParquetLake[Partitioned Parquet / Iceberg]
    end

    subgraph AI, Semantic Search & RAG
        Chunker[Semantic Metadata Chunker]
        FastEmbed[FastEmbed ONNX Embedder]
        PGVector[(PostgreSQL pgvector: 15433)]
        SearchAPI[FastAPI Semantic Search & RAG: 8000]
    end

    subgraph Batch Orchestration & Governance
        Airflow[(Apache Airflow: 8085)]
        DQEngine[Data Quality Engine & Quarantine]
        Prometheus[Prometheus: 9090]
        Grafana[Grafana Dashboards: 3000]
    end

    Client Layer -->|Playback, Searches, Clicks| CoreAPI
    Client Layer -->|User & Profile Updates| OLTP
    CoreAPI -->|Produce Events| Kafka
    Generator -->|Spikes & Fault Injection| Kafka
    OLTP -->|WAL Replication| Debezium
    Debezium -->|Change Events| Kafka
    Kafka <-->|Avro Schema Validation| SR

    Kafka -->|Stream Consumption| StreamEngine
    StreamEngine -->|Deduplication & Window Aggs| ClickHouse
    StreamEngine -->|Poison Pills| Kafka

    Kafka -->|Micro-batch Archival| MinIO
    MinIO -->|Historical Replay| Kafka

    ClickHouse -->|Staging & Transforms| dbt
    dbt -->|Publish Marts| Marts

    Mongo -->|Catalog Documents| Chunker
    Chunker -->|Vectorize| FastEmbed
    FastEmbed -->|384d Dense Vectors| PGVector
    PGVector -->|HNSW ANN Search| SearchAPI
    Marts -->|User Propensity Vectors| SearchAPI

    Airflow -->|Orchestrate Batch & DQ| dbt
    Airflow -->|Trigger Vector Refresh| PGVector
    DQEngine -->|Quarantine Bad Records| ClickHouse
    DQEngine -->|Metrics| Prometheus
    Prometheus -->|Alerts & Trends| Grafana
```

---

## 2. Core Subsystems

### 2.1 Operational Datastore & CDC (PostgreSQL + Debezium)
- **Engine:** PostgreSQL 16 on port `15432`.
- **Schema:** Normalized tables for `users`, `profiles`, `plans`, `subscriptions`, `devices`, and `watchlists`.
- **CDC Ingestion:** Debezium PostgreSQL connector streams change events from WAL using the `pgoutput` plugin directly into Kafka topic `sokti_cdc.public.users`.
- **Privacy:** Email and phone numbers are protected via DPDP masking policies before reaching analytical views.

### 2.2 Streaming Message Broker & Schemas (Apache Kafka + Schema Registry)
- **Engine:** Apache Kafka in KRaft mode (no Zookeeper overhead) on port `9092`.
- **Schema Enforcement:** Confluent Schema Registry (`8081`) enforces versioned Avro schemas (`playback_event.avsc`, `search_event.avsc`, etc.) with `BACKWARD` compatibility rules.
- **Partitioning Strategy:** Playback events are partitioned on `user_id` to ensure per-user ordered event sequences for stateful session reconstruction.

### 2.3 Real-Time Stream Engine & Deduplication (Flink / Engine)
- **Features:** 
  - In-memory LRU cache deduplicating events by `event_id`.
  - Watermark generation allowing 15-minute out-of-order latency.
  - Poison pill quarantine to Dead Letter Queue `ott.playback.dlq.v1`.
  - Dynamic content catalog enrichment.
  - Tumbling window aggregations for platform metrics and watch sessions.

### 2.4 Analytical OLAP Engine & dbt Modeling (ClickHouse + dbt)
- **Engine:** ClickHouse 24.3 on ports `8123` (HTTP) and `9000` (Native).
- **Table Design:** Partitioned by date (`toYYYYMM(event_time)`), ordered by sparse index keys (`(event_type, user_id, event_time)`).
- **dbt DAG:** 13 models comprising Staging (`stg_*`), Intermediate (`int_*`), and analytical Marts (`mart_churn_features`, `mart_daily_platform_metrics`, `mart_user_retention`).
- **Tests:** 17 automated dbt tests including unique keys, non-null checks, accepted values, and custom domain constraints.

### 2.5 Object Storage Lakehouse (MinIO S3 + Parquet)
- **Engine:** MinIO S3 API on port `9002`, Console on port `9001`.
- **Structure:** Partitioned Parquet lake organized into `sokti-raw/date=YYYY-MM-DD/hour=HH/`.
- **Replay Capability:** Includes `data_lake/replay_events.py` for point-in-time event replay back to Kafka.

### 2.6 AI Metadata, Semantic Search & RAG (MongoDB + FastEmbed + pgvector)
- **Catalog Store:** MongoDB 7.0 on port `27018` holding 110 rich movies, series, episodes, and subtitles.
- **Vector Store:** PostgreSQL 16 with pgvector extension on port `15433`.
- **Embedder:** FastEmbed ONNX `BAAI/bge-small-en-v1.5` generating 384-dimensional dense vectors.
- **Search API:** FastAPI on port `8000` serving semantic vector search, hybrid retrieval, and grounded RAG answer generation.

### 2.7 Batch Orchestration & Observability (Airflow, Prometheus, Grafana)
- **Airflow:** Standalone Airflow on port `8085` executing daily batch orchestration with automated SLAs, retries, and data quality checks.
- **Prometheus:** Port `9090` scraping ClickHouse, APIs, and data quality metrics with pre-configured alert rules.
- **Grafana:** Port `3000` provisioned with streaming throughput, consumer lag, and DQ gauges.

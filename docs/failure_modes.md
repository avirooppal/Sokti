# Sokti Failure Modes, Recovery Patterns & Self-Healing Architecture

This document details the production resilience, fault tolerance, and automated recovery strategies implemented across the Sokti streaming and batch data platform.

---

## 1. Architectural Recovery Matrix

| Failure Mode | Root Cause / Trigger | Detection Mechanism | Immediate Isolation | Recovery / Remediation |
|---|---|---|---|---|
| **Poison Pills** | Malformed JSON, corrupted binary frames, missing mandatory fields | Deserialization exception in consumer or Flink job | Routed to Dead Letter Queue (`ott.playback.dlq.v1`) | Consumer moves offset forward; DLQ replay after schema fix |
| **Duplicate Events** | Client retries, network flakiness, replay from data lake | Staging dbt window `row_number()`, ClickHouse ReplacingMergeTree, Flink LRU cache | Discarded in streaming; deduplicated in warehouse staging | Exactly-once analytical marts guaranteed |
| **Schema Drift** | Incompatible app versions, unauthorized fields | Schema Registry compatibility check (BACKWARD) | Rejected at producer or quarantined by DQ engine | Version bump or quarantine routing with field stripping |
| **Late-Arriving Events** | Mobile client reconnecting after airplane mode / offline | Event-time vs processing-time watermark (allowed 15m delay) | Discarded by real-time tumbling window, routed to batch path | MinIO Parquet historical layer captures all raw; Airflow batch reconciles |
| **Kafka Broker Outage** | Pod crash, network partition, disk full | Prometheus `kafka_underreplicated_partitions`, TCP timeout | Producer retries with exponential backoff (`retries=5`) | KRaft controller elects new leader; producers buffer in memory |
| **Debezium CDC Failure** | Database lock timeout, WAL slot overflow, Postgres restart | Kafka Connect `/connectors/sokti-postgres-connector/status` | Connector transitions to `FAILED` state | Kafka Connect auto-restarts task; reads LSN from `connect-offsets` |
| **ClickHouse Downtime** | OOM during large merge, node restart | Health check `http://clickhouse:8123/ping` fails | Streaming consumers buffer batches in memory | Consumer pauses offset commits; retries until HTTP 200 |
| **Impossible Values** | Buggy client app emitting `watch_seconds > 86400` or future timestamps | ClickHouse DQ Engine rule validations | Filtered into `sokti.quarantine_events` table | Automated remediation script caps timestamps & clamps values |

---

## 2. Deep Dive: Critical Scenarios

### 2.1 Poison Pills & DLQ Routing
When an unparseable or schema-violating payload arrives on `ott.playback.events.v1`:
1. The consumer or Flink stream processor catches the parsing error without crashing the main processing thread.
2. The raw bytes, error trace, original topic, and current timestamp are wrapped into a DLQ message.
3. The message is published to `ott.playback.dlq.v1`.
4. The consumer commits the offset on the primary topic, preventing head-of-line blocking.
5. SRE alerts fire when DLQ throughput exceeds 0.1% of ingress traffic.

### 2.2 CDC Wal Offset Recovery (Debezium)
Debezium continuously persists PostgreSQL LSNs (Log Sequence Numbers) into Kafka topic `my_connect_offsets`:
- When PostgreSQL or Kafka Connect restarts, Debezium queries the last committed LSN from Kafka.
- It reconnects to the PostgreSQL replication slot `debezium_slot` using the `pgoutput` logical decoding plugin.
- PostgreSQL streams only change events that occurred *after* the committed LSN.
- If the replication slot falls behind PostgreSQL's `wal_keep_size`, Debezium triggers an automated snapshot refresh.

### 2.3 Late-Arriving Events & Dual-Path Reconciliation
- **Real-time path (Flink/Streaming):** Implements Bounded-Out-Of-Orderness watermarks with a 15-minute tolerance. Events arriving later than 15 minutes are excluded from real-time tumbling metrics to prevent unbounded state memory growth.
- **Batch reconciliation path (MinIO + ClickHouse + dbt):** The MinIO raw writer records every event regardless of arrival latency. The nightly Airflow batch DAG runs dbt models across full partition date ranges, naturally incorporating all late-arriving events into authoritative analytical marts.

### 2.4 Idempotency & Remediation
- Analytical models (`mart_churn_features`, `mart_daily_platform_metrics`, `mart_user_retention`) use `ReplacingMergeTree` or overwrite staging views.
- Running a backfill or replaying raw data from MinIO using `python data_lake/replay_events.py` is completely idempotent.

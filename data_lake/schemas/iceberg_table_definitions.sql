-- ==============================================================================
-- Sokti OTT Lakehouse - Apache Iceberg Production Table Definitions
-- Designed for Spark / Trino / Flink over S3 Object Storage (AWS S3 / MinIO)
-- Conceptual Medallion Architecture: Bronze (Raw) -> Silver (Cleaned) -> Gold (Curated)
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Ingestion Log (Append-Only, Preserves Client Raw Telemetry)
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sokti_lakehouse.bronze.playback_events (
    event_id STRING COMMENT 'Unique event UUID for deduplication',
    event_type STRING,
    schema_version STRING,
    user_id STRING COMMENT 'Pseudonymous client UUID',
    session_id STRING,
    device_id STRING,
    device_type STRING,
    app_version STRING,
    content_id STRING,
    position_seconds FLOAT,
    playback_seconds FLOAT,
    event_time TIMESTAMP,
    ingestion_time TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(event_time), hours(event_time))
LOCATION 's3://sokti-lake/bronze/playback_events'
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'history.expire.max-snapshot-age-ms' = '604800000', -- Expire snapshots older than 7 days
    'write.object-storage.enabled' = 'true'
);

-- ------------------------------------------------------------------------------
-- 2. SILVER LAYER: Cleaned, Deduplicated, and Enriched Playback Events
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sokti_lakehouse.silver.fact_playback (
    event_id STRING,
    event_type STRING,
    user_id STRING,
    session_id STRING,
    device_id STRING,
    device_type STRING,
    app_version STRING,
    content_id STRING,
    content_title STRING,
    content_duration_seconds INT,
    position_seconds FLOAT,
    playback_seconds FLOAT,
    completion_percentage FLOAT,
    is_late_arriving BOOLEAN,
    event_time TIMESTAMP,
    processed_time TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(event_time))
LOCATION 's3://sokti-lake/silver/fact_playback'
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.upsert.enabled' = 'true',
    'format-version' = '2' -- Iceberg v2 enables row-level deletes & merge-on-read
);

-- ------------------------------------------------------------------------------
-- 3. GOLD LAYER: Curated Daily Content & User Marts
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sokti_lakehouse.gold.mart_content_performance_daily (
    metric_date DATE,
    content_id STRING,
    title STRING,
    genres ARRAY<STRING>,
    release_year INT,
    total_plays BIGINT,
    total_completions BIGINT,
    unique_viewers BIGINT,
    total_watch_hours DOUBLE,
    avg_completion_rate DOUBLE,
    created_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (months(metric_date))
LOCATION 's3://sokti-lake/gold/mart_content_performance_daily'
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'format-version' = '2'
);

/*
==============================================================================
Production Engine Interoperability Rationale:
1. Trino / Presto:
   Queries Gold and Silver Iceberg tables directly via Hive Metastore or AWS Glue Catalog.
   Provides sub-second interactive BI slicing across petabytes of historical streaming data.
2. Apache Spark:
   Executes scheduled hourly/daily compaction jobs:
   CALL sokti_lakehouse.system.rewrite_data_files(table => 'silver.fact_playback');
   CALL sokti_lakehouse.system.expire_snapshots(table => 'silver.fact_playback', older_than => TIMESTAMP '...');
3. Apache Flink:
   Continuous streaming sink that writes Parquet commits directly to Iceberg Bronze tables
   leveraging Iceberg's two-phase commit (2PC) exact-once semantics.
==============================================================================
*/

CREATE DATABASE IF NOT EXISTS sokti;

CREATE TABLE IF NOT EXISTS sokti.raw_playback_events (
    event_id UUID,
    event_type LowCardinality(String),
    schema_version LowCardinality(String),
    user_id UUID,
    session_id UUID,
    device_id UUID,
    device_type LowCardinality(String),
    app_version LowCardinality(String),
    content_id String,
    position_seconds Float32,
    playback_seconds Float32,
    event_time DateTime64(3, 'UTC'),
    ingestion_time DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_type, user_id, event_time, event_id)
TTL toDateTime(event_time) + INTERVAL 90 DAY;

CREATE TABLE IF NOT EXISTS sokti.raw_search_events (
    event_id UUID,
    event_type LowCardinality(String),
    schema_version LowCardinality(String),
    user_id UUID,
    session_id UUID,
    device_id UUID,
    query_text String,
    results_count UInt32,
    selected_content_id String,
    event_time DateTime64(3, 'UTC'),
    ingestion_time DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (user_id, event_time, event_id)
TTL toDateTime(event_time) + INTERVAL 90 DAY;

CREATE TABLE IF NOT EXISTS sokti.raw_recommendation_events (
    event_id UUID,
    event_type LowCardinality(String),
    schema_version LowCardinality(String),
    user_id UUID,
    session_id UUID,
    device_id UUID,
    model_version LowCardinality(String),
    recommended_content_ids Array(String),
    clicked_content_id String,
    position_index UInt16,
    event_time DateTime64(3, 'UTC'),
    ingestion_time DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (user_id, event_time, event_id)
TTL toDateTime(event_time) + INTERVAL 90 DAY;

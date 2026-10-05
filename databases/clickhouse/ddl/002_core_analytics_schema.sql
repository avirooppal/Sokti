-- ==============================================================================
-- Sokti OTT Analytics - Core Dimensional & Fact Schema (ClickHouse DDL)
-- ==============================================================================

CREATE DATABASE IF NOT EXISTS sokti;

-- 1. Dimension Tables
CREATE TABLE IF NOT EXISTS sokti.dim_content (
    content_id LowCardinality(String),
    title String,
    genres Array(LowCardinality(String)),
    language LowCardinality(String),
    release_year UInt16,
    director LowCardinality(String),
    duration_seconds UInt32,
    maturity_rating LowCardinality(String),
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY content_id;

CREATE TABLE IF NOT EXISTS sokti.dim_device (
    device_id UUID,
    device_type LowCardinality(String),
    device_name String,
    os LowCardinality(String),
    app_version LowCardinality(String),
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (device_type, os, app_version, device_id);

-- 2. Fact Tables
CREATE TABLE IF NOT EXISTS sokti.fact_watch_sessions (
    user_id UUID,
    session_id UUID,
    content_id LowCardinality(String),
    device_type LowCardinality(String),
    app_version LowCardinality(String),
    watch_seconds Float32,
    pause_count UInt16,
    seek_count UInt16,
    completion_percentage Float32,
    session_start DateTime64(3, 'UTC'),
    session_end DateTime64(3, 'UTC'),
    created_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(session_start)
ORDER BY (user_id, content_id, session_start, session_id)
TTL toDateTime(session_start) + INTERVAL 180 DAY;

CREATE TABLE IF NOT EXISTS sokti.fact_content_engagement (
    content_id LowCardinality(String),
    user_id UUID,
    device_type LowCardinality(String),
    event_type LowCardinality(String),
    position_seconds Float32,
    playback_seconds Float32,
    event_time DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (content_id, event_time, user_id)
TTL toDateTime(event_time) + INTERVAL 180 DAY;

CREATE TABLE IF NOT EXISTS sokti.fact_searches (
    event_id UUID,
    user_id UUID,
    session_id UUID,
    device_type LowCardinality(String),
    query_text String,
    results_count UInt16,
    selected_content_id LowCardinality(String),
    has_click UInt8,
    event_time DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (user_id, event_time, event_id)
TTL toDateTime(event_time) + INTERVAL 90 DAY;

-- 3. Aggregated Reporting Marts
CREATE TABLE IF NOT EXISTS sokti.agg_content_hourly (
    content_id LowCardinality(String),
    window_hour DateTime,
    total_plays UInt32,
    total_completions UInt32,
    total_watch_seconds Float64,
    unique_viewers_count UInt32
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(window_hour)
ORDER BY (window_hour, content_id);

CREATE TABLE IF NOT EXISTS sokti.agg_platform_daily (
    metric_date Date,
    total_events UInt64,
    total_plays UInt32,
    unique_active_users UInt32,
    total_watch_hours Float64,
    avg_session_minutes Float32
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(metric_date)
ORDER BY metric_date;

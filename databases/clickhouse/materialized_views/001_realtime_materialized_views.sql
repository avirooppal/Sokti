-- ==============================================================================
-- Sokti OTT Analytics - ClickHouse Real-Time Materialized Views
-- ==============================================================================

-- Materialized View: Hourly Content Metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS sokti.mv_content_hourly
TO sokti.agg_content_hourly
AS
SELECT
    content_id,
    toStartOfHour(event_time) AS window_hour,
    countIf(event_type = 'video_play') AS total_plays,
    countIf(event_type = 'video_complete') AS total_completions,
    sum(playback_seconds) AS total_watch_seconds,
    uniqExact(user_id) AS unique_viewers_count
FROM sokti.raw_playback_events
GROUP BY
    content_id,
    window_hour;

-- Materialized View: Daily Platform Metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS sokti.mv_platform_daily
TO sokti.agg_platform_daily
AS
SELECT
    toDate(event_time) AS metric_date,
    count(*) AS total_events,
    countIf(event_type = 'video_play') AS total_plays,
    uniqExact(user_id) AS unique_active_users,
    round(sum(playback_seconds) / 3600.0, 2) AS total_watch_hours,
    round(avg(playback_seconds) / 60.0, 2) AS avg_session_minutes
FROM sokti.raw_playback_events
GROUP BY
    metric_date;

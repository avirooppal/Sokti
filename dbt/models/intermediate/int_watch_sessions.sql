{{ config(materialized='view') }}

WITH base_events AS (
    SELECT
        event_id,
        user_id,
        session_id,
        content_id,
        device_type,
        event_type,
        position_seconds,
        playback_seconds,
        event_time
    FROM {{ ref('stg_playback_events') }}
),

content_info AS (
    SELECT
        content_id,
        title,
        duration_seconds
    FROM {{ ref('stg_content') }}
),

session_aggregates AS (
    SELECT
        b.user_id,
        b.session_id,
        b.content_id,
        any(c.title) AS content_title,
        any(b.device_type) AS device_type,
        min(b.event_time) AS session_start,
        max(b.event_time) AS session_end,
        sum(b.playback_seconds) AS total_watch_seconds,
        countIf(b.event_type = 'video_pause') AS pause_count,
        countIf(b.event_type = 'video_seek') AS seek_count,
        max(b.position_seconds) AS max_position_seconds,
        any(c.duration_seconds) AS content_duration_seconds
    FROM base_events b
    LEFT JOIN content_info c ON b.content_id = c.content_id
    GROUP BY
        b.user_id,
        b.session_id,
        b.content_id
)

SELECT
    user_id,
    session_id,
    content_id,
    content_title,
    device_type,
    session_start,
    session_end,
    total_watch_seconds,
    pause_count,
    seek_count,
    if(content_duration_seconds > 0,
       least(100.0, round((max_position_seconds / content_duration_seconds) * 100.0, 2)),
       0.0) AS completion_percentage,
    dateDiff('minute', session_start, session_end) AS session_duration_minutes
FROM session_aggregates

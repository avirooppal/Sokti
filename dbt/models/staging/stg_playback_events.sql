{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('sokti', 'raw_playback_events') }}
),

ranked AS (
    SELECT
        event_id,
        event_type,
        schema_version,
        user_id,
        session_id,
        device_id,
        device_type,
        app_version,
        content_id,
        position_seconds,
        playback_seconds,
        event_time,
        ingestion_time,
        row_number() OVER (PARTITION BY event_id ORDER BY ingestion_time DESC) AS rn
    FROM source
    WHERE content_id IS NOT NULL 
      AND content_id != ''
      AND event_type IN ('video_play', 'video_pause', 'video_seek', 'video_stop', 'video_complete')
      AND event_time <= now() + INTERVAL 2 HOUR
)

SELECT
    event_id,
    event_type,
    schema_version,
    user_id,
    session_id,
    device_id,
    device_type,
    app_version,
    content_id,
    position_seconds,
    playback_seconds,
    event_time,
    ingestion_time
FROM ranked
WHERE rn = 1


{{ config(materialized='view') }}

-- Reads from dim_device / raw user events to create pseudonymous user staging table
SELECT DISTINCT
    user_id,
    device_type,
    app_version,
    toDateTime(min(event_time)) AS first_seen_at,
    toDateTime(max(event_time)) AS last_seen_at
FROM {{ ref('stg_playback_events') }}
GROUP BY
    user_id,
    device_type,
    app_version

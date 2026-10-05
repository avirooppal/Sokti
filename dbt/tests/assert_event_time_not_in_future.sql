-- Custom dbt test: event_time must not be in the future (beyond 2-hour clock skew threshold)
SELECT
    event_id,
    event_time
FROM {{ ref('stg_playback_events') }}
WHERE event_time > now() + INTERVAL 2 HOUR

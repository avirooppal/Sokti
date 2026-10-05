-- Custom dbt test: watch_seconds must always be >= 0.0
SELECT
    session_id,
    total_watch_seconds
FROM {{ ref('int_watch_sessions') }}
WHERE total_watch_seconds < 0.0

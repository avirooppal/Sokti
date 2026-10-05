-- Custom dbt test: completion_percentage must be between 0.0 and 100.0
SELECT
    session_id,
    completion_percentage
FROM {{ ref('int_watch_sessions') }}
WHERE completion_percentage < 0.0 OR completion_percentage > 100.0

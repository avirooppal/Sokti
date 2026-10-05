{{ config(materialized='view') }}

WITH daily_sessions AS (
    SELECT
        user_id,
        toDate(session_start) AS activity_date,
        count(session_id) AS sessions_count,
        sum(total_watch_seconds) AS daily_watch_seconds,
        avg(completion_percentage) AS avg_completion_pct
    FROM {{ ref('int_watch_sessions') }}
    GROUP BY
        user_id,
        activity_date
)

SELECT
    user_id,
    activity_date,
    sessions_count,
    daily_watch_seconds,
    round(daily_watch_seconds / 3600.0, 2) AS daily_watch_hours,
    avg_completion_pct
FROM daily_sessions

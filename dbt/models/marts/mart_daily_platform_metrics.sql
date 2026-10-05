{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='metric_date'
) }}

SELECT
    activity_date AS metric_date,
    count(DISTINCT user_id) AS daily_active_users,
    sum(sessions_count) AS total_watch_sessions,
    round(sum(daily_watch_hours), 2) AS total_watch_hours,
    round(avg(daily_watch_seconds / 60.0), 2) AS avg_daily_watch_minutes,
    round(avg(avg_completion_pct), 2) AS avg_platform_completion_pct
FROM {{ ref('int_daily_user_activity') }}
GROUP BY metric_date

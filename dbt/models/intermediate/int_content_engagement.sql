{{ config(materialized='view') }}

WITH content_metrics AS (
    SELECT
        content_id,
        content_title,
        toDate(session_start) AS engagement_date,
        count(session_id) AS total_sessions,
        uniqExact(user_id) AS unique_viewers,
        sum(total_watch_seconds) AS total_watch_seconds,
        round(avg(completion_percentage), 2) AS avg_completion_percentage
    FROM {{ ref('int_watch_sessions') }}
    GROUP BY
        content_id,
        content_title,
        engagement_date
)

SELECT
    content_id,
    content_title,
    engagement_date,
    total_sessions,
    unique_viewers,
    total_watch_seconds,
    round(total_watch_seconds / 3600.0, 2) AS total_watch_hours,
    avg_completion_percentage,
    -- Rank content by popularity per day
    dense_rank() OVER (PARTITION BY engagement_date ORDER BY total_watch_seconds DESC) AS daily_rank
FROM content_metrics

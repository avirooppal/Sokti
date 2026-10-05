{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(content_id, engagement_date)'
) }}

SELECT
    e.content_id,
    e.content_title,
    e.engagement_date,
    c.genres,
    c.language,
    c.release_year,
    c.director,
    e.total_sessions,
    e.unique_viewers,
    e.total_watch_hours,
    e.avg_completion_percentage,
    e.daily_rank,
    -- 7-day rolling watch hours window
    sum(e.total_watch_hours) OVER (
        PARTITION BY e.content_id 
        ORDER BY e.engagement_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_watch_hours
FROM {{ ref('int_content_engagement') }} e
LEFT JOIN {{ ref('stg_content') }} c ON e.content_id = c.content_id

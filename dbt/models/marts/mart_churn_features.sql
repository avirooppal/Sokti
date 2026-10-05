{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='user_id'
) }}

WITH user_sessions AS (
    SELECT
        s.user_id,
        s.session_start,
        s.total_watch_seconds,
        s.completion_percentage,
        c.genres
    FROM {{ ref('int_watch_sessions') }} s
    LEFT JOIN {{ ref('stg_content') }} c ON s.content_id = c.content_id
),

user_watch_stats AS (
    SELECT
        user_id,
        -- 7-day watch minutes
        round(sumIf(total_watch_seconds, session_start >= now() - INTERVAL 7 DAY) / 60.0, 2) AS genre_watch_minutes_7d,
        -- 30-day watch minutes
        round(sumIf(total_watch_seconds, session_start >= now() - INTERVAL 30 DAY) / 60.0, 2) AS genre_watch_minutes_30d,
        -- Average completion rate
        round(avg(completion_percentage), 2) AS content_completion_rate,
        -- Average session length in minutes
        round(avg(total_watch_seconds) / 60.0, 2) AS average_session_length,
        -- Active days in last 30 days
        uniqExactIf(toDate(session_start), session_start >= now() - INTERVAL 30 DAY) AS active_days_30d
    FROM user_sessions
    GROUP BY user_id
),

user_searches AS (
    SELECT
        user_id,
        countIf(event_time >= now() - INTERVAL 7 DAY) AS search_count_7d
    FROM {{ ref('stg_search_events') }}
    GROUP BY user_id
),

user_recs AS (
    SELECT
        user_id,
        round(countIf(event_type = 'recommendation_click') / greatest(count(*), 1) * 100.0, 2) AS recommendation_ctr
    FROM {{ source('sokti', 'raw_recommendation_events') }}
    GROUP BY user_id
)

SELECT
    user_id,
    w.genre_watch_minutes_7d,
    w.genre_watch_minutes_30d,
    w.content_completion_rate,
    coalesce(s.search_count_7d, 0) AS search_count_7d,
    w.active_days_30d,
    coalesce(r.recommendation_ctr, 0.0) AS recommendation_ctr,
    w.average_session_length
FROM user_watch_stats w
LEFT JOIN user_searches s USING (user_id)
LEFT JOIN user_recs r USING (user_id)


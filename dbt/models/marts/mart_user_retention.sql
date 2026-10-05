{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(cohort_week, activity_week)'
) }}

WITH user_first_seen AS (
    SELECT
        user_id,
        toMonday(min(activity_date)) AS cohort_week
    FROM {{ ref('int_daily_user_activity') }}
    GROUP BY user_id
),

user_activities AS (
    SELECT
        a.user_id,
        f.cohort_week,
        toMonday(a.activity_date) AS activity_week,
        dateDiff('week', f.cohort_week, toMonday(a.activity_date)) AS week_number
    FROM {{ ref('int_daily_user_activity') }} a
    INNER JOIN user_first_seen f ON a.user_id = f.user_id
)

SELECT
    cohort_week,
    activity_week,
    week_number,
    count(DISTINCT user_id) AS active_users_in_cohort
FROM user_activities
GROUP BY
    cohort_week,
    activity_week,
    week_number

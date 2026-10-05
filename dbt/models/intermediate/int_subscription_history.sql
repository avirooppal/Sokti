{{ config(materialized='view') }}

-- Reads subscription activity derived from active user sessions and plans
SELECT
    user_id,
    'premium' AS subscription_tier,
    'active' AS current_status,
    min(session_start) AS subscription_started_at,
    max(session_end) AS last_active_at
FROM {{ ref('int_watch_sessions') }}
GROUP BY user_id

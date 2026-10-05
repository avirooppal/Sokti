{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='event_date'
) }}

WITH rec_events AS (
    SELECT
        toDate(event_time) AS event_date,
        model_version,
        event_type,
        if(event_type = 'recommendation_click', 1, 0) AS is_click
    FROM {{ source('sokti', 'raw_recommendation_events') }}
)

SELECT
    event_date,
    model_version,
    count(*) AS total_recommendations_shown,
    sum(is_click) AS total_clicks,
    round(sum(is_click) / greatest(count(*), 1) * 100.0, 2) AS recommendation_ctr
FROM rec_events
GROUP BY
    event_date,
    model_version

{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('sokti', 'raw_search_events') }}
)

SELECT
    event_id,
    user_id,
    session_id,
    device_id,
    query_text,
    results_count,
    selected_content_id,
    if(selected_content_id IS NOT NULL AND selected_content_id != '', 1, 0) AS has_click,
    event_time,
    ingestion_time
FROM source

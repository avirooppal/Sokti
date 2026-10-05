-- Custom dbt test: playback content_id must exist in content dimension catalog
SELECT
    p.content_id
FROM {{ ref('stg_playback_events') }} p
LEFT JOIN {{ ref('stg_content') }} c ON p.content_id = c.content_id
WHERE c.content_id IS NULL

{{ config(materialized='view') }}

SELECT
    content_id,
    title,
    genres,
    language,
    release_year,
    director,
    duration_seconds,
    maturity_rating
FROM {{ source('sokti', 'dim_content') }}

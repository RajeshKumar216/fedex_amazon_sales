-- models/dim_fulfilled_by.sql
-- {{ config(materialized='table') }}
{{ config(
    materialized='incremental',
    unique_key='fulfilled_by_id'
) }}

WITH distinct_fulfilldby_data AS (
    SELECT DISTINCT
        fulfilled_by AS fulfil
    FROM {{ source('raw_data', 'cleaned_dataset') }}
    WHERE fulfilled_by IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY fulfil) AS fulfilled_by_id,
    fulfil AS fulfilled_by
FROM distinct_fulfilldby_data


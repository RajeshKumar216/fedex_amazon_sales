-- models/dim_fulfilment.sql
-- {{ config(materialized='table') }}
{{ config(
    materialized='incremental',
    unique_key='fulfilment_id'
) }}

WITH distinct_fulfil_data AS (
    SELECT DISTINCT
        fulfilment AS fulfil
    FROM {{ source('raw_data', 'cleaned_dataset') }}
    WHERE fulfilment IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY fulfil) AS fulfilment_id,
    fulfil AS fulfilment
FROM distinct_fulfil_data


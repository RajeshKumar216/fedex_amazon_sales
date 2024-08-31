-- models/dim_promotion.sql
-- {{ config(materialized='table') }}
{{ config(
    materialized='incremental',
    unique_key='promotion_id'
) }}

WITH base_data AS (
    SELECT DISTINCT
    promotion_ids
    FROM {{ source('raw_data', 'cleaned_dataset') }}
    WHERE promotion_ids is NOT NULL
    )

SELECT 
    ROW_NUMBER() OVER( ORDER BY promotion_ids) as promotion_id,
    promotion_ids as promotion
    FROM base_data

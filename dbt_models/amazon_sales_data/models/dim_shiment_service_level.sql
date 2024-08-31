-- models/dim_shiment_service_level.sql
-- {{ config(materialized='table') }}
{{ config(
    materialized='incremental',
    unique_key='ship_service_level_id'
) }}


with base_data as (
    SELECT DISTINCT
    ship_service_level
    FROM {{ source('raw_data', 'cleaned_dataset') }}
    WHERE ship_service_level is NOT NULL
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY ship_service_level) as ship_service_level_id,
    ship_service_level
    FROM base_data
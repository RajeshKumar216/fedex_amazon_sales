-- models/dim_sales_channel.sql
-- {{ config(materialized='table') }}
{{ config(
    materialized='incremental',
    unique_key='sales_channel_id'
) }}


with base_data as (
    SELECT DISTINCT
    sales_channel
    FROM {{ source('raw_data', 'cleaned_dataset') }}
    WHERE sales_channel is NOT NULL
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY sales_channel) as sales_channel_id,
    sales_channel
    FROM base_data
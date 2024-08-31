-- models/dim_order_status.sql
-- for first time need to run with below 
-- {{ config(materialized='table') }}

{{ config(materialized='incremental',
    unique_key='order_status_id') 
    }}

WITH order_data AS (
    SELECT
        DISTINCT
        status
    FROM
        {{ source('raw_data', 'cleaned_dataset') }}
    WHERE
        status IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY status) AS order_status_id,
    status AS order_status
FROM
    order_data

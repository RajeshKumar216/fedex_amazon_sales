-- models/dim_courier_status.sql
-- for first time need to run with below 
-- {{ config(materialized='table') }}

{{ config(materialized='incremental',
    unique_key='courier_status_id')
     }}


WITH distinct_courier_status AS (
    SELECT
        DISTINCT
        courier_status AS courier_status_value
    FROM
        {{ source('raw_data', 'cleaned_dataset') }}
    WHERE
        courier_status IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY courier_status_value) AS courier_status_id,
    courier_status_value AS courier_status
FROM
    distinct_courier_status

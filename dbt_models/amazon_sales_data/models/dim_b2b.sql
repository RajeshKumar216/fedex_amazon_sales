-- models/dim_b2b.sql
-- for first time need to run with below 
-- {{ config(materialized='table') }}

{{ config(materialized='incremental',
    unique_key='b2b_id') 
    }}

WITH distinct_b2b AS (
    SELECT
        DISTINCT
        b2b AS b2b_value
    FROM
        {{ source('raw_data', 'cleaned_dataset') }}
    WHERE
        b2b IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY b2b_value) AS b2b_id,
    b2b_value AS b2b
FROM
    distinct_b2b

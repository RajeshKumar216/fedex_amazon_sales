-- for first time need to run with below 
-- {{ config(materialized='table') }}

{{ config(
    materialized='incremental',
    unique_key='date_id'
) }}

WITH date_data AS (
    SELECT
        DISTINCT
        date AS cl_date
    FROM
        {{ source('raw_data', 'cleaned_dataset') }}
    WHERE
        date IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY cl_date) AS date_id,
    cl_date AS date,
    EXTRACT(DAY FROM cl_date) AS day,
    EXTRACT(MONTH FROM cl_date) AS month,
    EXTRACT(YEAR FROM cl_date) AS year,
    EXTRACT(QUARTER FROM cl_date) AS quarter,
    CASE
        WHEN EXTRACT(DOW FROM cl_date) IN (0, 6) THEN TRUE
        ELSE FALSE
    END AS is_weekend,
    CASE
        WHEN EXTRACT(MONTH FROM cl_date) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM cl_date) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM cl_date) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END AS season

FROM
    date_data

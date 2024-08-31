-- models/dim_location.sql
-- {{ config(materialized='table') }}
{{ config(
    materialized='incremental',
    unique_key='location_id'
) }}

with location_data as (
    SELECT DISTINCT
    ship_city, ship_state, ship_postal_code, ship_country, classified_city
    FROM
    {{source('raw_data', 'cleaned_dataset')}}
    WHERE
    ship_city IS NOT NULL
    AND ship_state IS NOT NULL
    AND ship_postal_code IS NOT NULL
    AND ship_country IS NOT NULL
    AND classified_city IS NOT NULL
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY ship_city, ship_state,
                                ship_postal_code, ship_country,
                                classified_city) AS location_id,
    ship_city as city,
    ship_state as state,
    ship_postal_code as postal_code,
    ship_country as country,
    classified_city
FROM    
    location_data

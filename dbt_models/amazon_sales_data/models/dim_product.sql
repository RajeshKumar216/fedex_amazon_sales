-- models/dim_product.sql
-- {{ config(materialized='table') }}
{{ config(
    materialized='incremental',
    unique_key='product_id'
) }}


with product_data as (
    SELECT  DISTINCT
        sku,
        style,
        category,
        size,
        asin
    FROM {{ source('raw_data', 'cleaned_dataset') }}
    WHERE
    sku IS NOT NULL
    AND style IS NOT NULL
    AND category IS NOT NULL
    AND size IS NOT NULL
    AND asin IS NOT NULL
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY sku, style,category,size,asin) as  product_id,
    sku,
    style,
    category,
    size,
    asin
    FROM product_data

-- models/fact_sales.sql
-- {{ config(materialized='table') }}
{{ config(
    materialized='incremental',
    unique_key='order_id' 
) }}

WITH base_data AS (
    SELECT 
        r.order_id,
        r.date,
        r.sku,
        r.style,
        r.category,
        r.size,
        r.asin,
        r.ship_city,
        r.ship_state,
        r.ship_postal_code,
        r.ship_country,
        r.promotion_ids,
        r.fulfilment,
        r.b2b,
        r.courier_status,
        r.fulfilled_by,
        r.status,
        r.sales_channel,
        r.ship_service_level,
        r.qty,
        r.amount,
        r.currency
    FROM {{ source('raw_data', 'cleaned_dataset') }} as r
)

SELECT 
    r.order_id,
    d.date_id,
    p.product_id,
    l.location_id,
    pr.promotion_id,
    f.fulfilment_id,
    b.b2b_id,
    cs.courier_status_id,
    fb.fulfilled_by_id,
    os.order_status_id,
    sc.sales_channel_id,
    ss.ship_service_level_id,
    r.qty,
    r.amount,
    r.currency
FROM base_data r
LEFT JOIN {{ ref('dim_date') }} d
    ON r.date = d.date
LEFT JOIN {{ ref('dim_product') }} p
    ON r.sku = p.sku
    AND r.style = p.style
    AND r.category = p.category
    AND r.size = p.size
    AND r.asin = p.asin
LEFT JOIN {{ ref('dim_location') }} l
    ON r.ship_city = l.ship_city 
    AND r.ship_state = l.ship_state
    AND r.ship_postal_code = l.ship_postal_code
    AND r.ship_country = l.ship_country
LEFT JOIN {{ ref('dim_promotion') }} pr
    ON r.promotion_ids = pr.promotion
LEFT JOIN {{ ref('dim_fulfilment') }} f
    ON r.fulfilment = f.fulfilment
LEFT JOIN {{ ref('dim_b2b') }} b
    ON r.b2b = b.b2b
LEFT JOIN {{ ref('dim_courier_status') }} cs
    ON r.courier_status = cs.courier_status
LEFT JOIN {{ ref('dim_fulfilled_by') }} fb
    ON r.fulfilled_by = fb.fulfilled_by
LEFT JOIN {{ ref('dim_order_status') }} os
    ON r.status = os.order_status
LEFT JOIN {{ ref('dim_sales_channel') }} sc
    ON r.sales_channel = sc.sales_channel
LEFT JOIN {{ ref('dim_shiment_service_level') }} ss
    ON r.ship_service_level = ss.ship_service_level

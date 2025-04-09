WITH orders AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
),
order_fulfillment AS (
    SELECT *
    FROM {{ ref('stg_order_fulfillment') }}
),
joined_data AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_placement_date AS order_date,  -- to join with dim_date if needed
        ofu.on_time,
        ofu.in_full,
        ofu.ontime_in_full AS otif
    FROM orders o
    JOIN order_fulfillment ofu
        ON o.order_id = ofu.order_id
)
SELECT
    ROUND((SUM(on_time::numeric) / COUNT(*)) * 100, 2) AS on_time_percentage,
    ROUND((SUM(in_full::numeric) / COUNT(*)) * 100, 2) AS in_full_percentage,
    ROUND((SUM(otif::numeric) / COUNT(*)) * 100, 2) AS otif_percentage
FROM joined_data

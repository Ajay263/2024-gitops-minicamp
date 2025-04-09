-- models/fact_order_line_performance.sql
WITH order_lines AS (
    SELECT *
    FROM {{ ref('stg_order_lines') }}
),
orders AS (
    SELECT order_id, customer_id, order_placement_date
    FROM {{ ref('stg_orders') }}
)
SELECT
    ol.order_line_id,
    ol.order_id,
    ol.product_id,
    ol.order_qty,
    ol.delivery_qty,
    ol.agreed_delivery_date,
    ol.actual_delivery_date,
    -- Calculate Volume Fill Rate: delivered_qty / order_qty (as a decimal)
    CASE
        WHEN ol.order_qty > 0 THEN (ol.delivery_qty::numeric / ol.order_qty)
        ELSE NULL
    END AS volume_fill_rate,
    -- Calculate Line Fill Rate: 1 if fully delivered, else 0
    CASE
        WHEN ol.delivery_qty = ol.order_qty THEN 1
        ELSE 0
    END AS line_fill_rate
FROM order_lines ol
JOIN orders o
    ON ol.order_id = o.order_id

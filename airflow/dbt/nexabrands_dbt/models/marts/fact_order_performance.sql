WITH orders AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
),

order_fulfillment AS (
    SELECT *
    FROM {{ ref('stg_order_fulfillment') }}
),

customers AS (
    SELECT *
    FROM {{ ref('stg_customers') }}
),

-- First get the base order data with fulfillment metrics
base_orders AS (
    SELECT
        o.order_id,
        o.customer_id,
        COALESCE(c.customer_name, 'Unknown') as customer_name,
        COALESCE(c.city, 'Unknown') as city,
        DATE_TRUNC('day', o.order_placement_date) AS order_date,
        ofu.on_time,
        ofu.in_full,
        ofu.ontime_in_full AS otif
    FROM orders o
    JOIN order_fulfillment ofu
        ON o.order_id = ofu.order_id
    LEFT JOIN customers c
        ON o.customer_id = c.customer_id
),

-- Calculate daily aggregates by city and customer
daily_metrics AS (
    SELECT
        order_date,
        city,
        customer_name,
        COUNT(*) AS total_orders,
        ROUND(100.0 * AVG(on_time), 2) AS on_time_percentage,
        ROUND(100.0 * AVG(in_full), 2) AS in_full_percentage,
        ROUND(100.0 * AVG(otif), 2) AS otif_percentage
    FROM base_orders
    GROUP BY order_date, city, customer_name
),

-- Calculate rolling 7-day averages
rolling_metrics AS (
    SELECT
        order_date,
        city,
        customer_name,
        total_orders,
        on_time_percentage,
        in_full_percentage,
        otif_percentage,
        ROUND(AVG(on_time_percentage) OVER (
            PARTITION BY city, customer_name
            ORDER BY order_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2) AS seven_day_avg_on_time,
        ROUND(AVG(in_full_percentage) OVER (
            PARTITION BY city, customer_name
            ORDER BY order_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2) AS seven_day_avg_in_full,
        ROUND(AVG(otif_percentage) OVER (
            PARTITION BY city, customer_name
            ORDER BY order_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2) AS seven_day_avg_otif
    FROM daily_metrics
)

SELECT * FROM rolling_metrics
ORDER BY order_date DESC, city, customer_name

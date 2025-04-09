-- models/dim_customers.sql
WITH customers AS (
    SELECT *
    FROM {{ ref('stg_customers') }}
),
customer_targets AS (
    SELECT *
    FROM {{ ref('stg_customer_targets') }}
)
SELECT
    c.customer_id,
    c.customer_name,
    c.city,
    ct.ontime_target,
    ct.infull_target,
    ct.otif_target
FROM customers c
LEFT JOIN customer_targets ct
    ON c.customer_id = ct.customer_id

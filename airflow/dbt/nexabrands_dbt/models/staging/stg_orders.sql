
-- models/staging/stg_orders.sql
WITH source AS (
    SELECT * FROM {{ source('nexabrands_datawarehouse', 'orders') }}
),
staged AS (
    SELECT
        order_id,
        customer_id,
        order_placement_date
    FROM source
)
SELECT * FROM staged

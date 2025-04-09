
-- models/staging/stg_order_fulfillment.sql
WITH source AS (
    SELECT * FROM {{ source('nexabrands_datawarehouse', 'order_fulfillment') }}
),
staged AS (
    SELECT
        order_id,
        on_time,
        in_full,
        otif as ontime_in_full
    FROM source
)
SELECT * FROM staged

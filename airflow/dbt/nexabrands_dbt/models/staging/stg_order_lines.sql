-- models/staging/stg_order_lines.sql
WITH source AS (
    SELECT * FROM {{ source('nexabrands_datawarehouse', 'order_lines') }}
),
staged AS (
    SELECT
        order_id,
        product_id,
        order_qty,
        agreed_delivery_date,
        actual_delivery_date,
        delivery_qty,
        {{ generate_surrogate_key(['order_id', 'product_id']) }} as order_line_id
    FROM source
)
SELECT * FROM staged

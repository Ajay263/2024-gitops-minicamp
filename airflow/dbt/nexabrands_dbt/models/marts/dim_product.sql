-- models/dim_products.sql
WITH source AS (
    SELECT *
    FROM {{ ref('stg_products') }}
)
SELECT
    product_id,
    product_name,
    category
FROM source

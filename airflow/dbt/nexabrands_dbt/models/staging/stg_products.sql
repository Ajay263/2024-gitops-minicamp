
-- models/staging/stg_products.sql
WITH source AS (
    SELECT * FROM {{ source('nexabrands_datawarehouse', 'products') }}
),
staged AS (
    SELECT
        product_id,
        product_name,
        category
    FROM source
)
SELECT * FROM staged

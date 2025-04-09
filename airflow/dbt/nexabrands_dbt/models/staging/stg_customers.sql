
-- models/staging/stg_customers.sql
WITH source AS (
    SELECT * FROM {{ source('nexabrands_datawarehouse', 'customers') }}
),
staged AS (
    SELECT
        customer_id,
        customer_name,
        city
    FROM source
)
SELECT * FROM staged

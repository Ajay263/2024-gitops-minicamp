WITH source AS (
    SELECT *
    FROM {{ source('nexabrands_datawarehouse', 'customer_targets') }}
),
staged AS (
    SELECT
        customer_id,
        "ontime_target",
        "infull_target",
        "otif_target"
    FROM source
)
SELECT *
FROM staged

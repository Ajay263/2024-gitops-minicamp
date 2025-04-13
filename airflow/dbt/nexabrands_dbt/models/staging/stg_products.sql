-- models/staging/stg_products.sql
with source as (
    select * from {{ source('nexabrands_datawarehouse', 'products') }}
),

staged as (
    select
        product_id,
        product_name,
        category
    from source
)

select * from staged

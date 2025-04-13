-- models/staging/stg_orders.sql
with source as (
    select * from {{ source('nexabrands_datawarehouse', 'orders') }}
),

staged as (
    select
        order_id,
        customer_id,
        order_placement_date
    from source
)

select * from staged

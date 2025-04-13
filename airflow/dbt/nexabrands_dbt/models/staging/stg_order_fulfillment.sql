-- models/staging/stg_order_fulfillment.sql
with source as (
    select * from {{ source('nexabrands_datawarehouse', 'order_fulfillment') }}
),

staged as (
    select
        order_id,
        on_time,
        in_full,
        otif as ontime_in_full
    from source
)

select * from staged

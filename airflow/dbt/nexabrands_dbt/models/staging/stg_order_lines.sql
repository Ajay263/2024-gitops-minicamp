-- models/staging/stg_order_lines.sql
with source as (
    select * from {{ source('nexabrands_datawarehouse', 'order_lines') }}
),

staged as (
    select
        order_id,
        product_id,
        order_qty,
        agreed_delivery_date,
        actual_delivery_date,
        delivery_qty,




























        
            
            {{ generate_surrogate_key(['order_id', 'product_id']) }} as order_line_id
    from source
)

select * from staged

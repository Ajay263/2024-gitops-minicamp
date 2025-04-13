with orders as (
    select *
    from {{ ref('stg_orders') }}
),

order_fulfillment as (
    select *
    from {{ ref('stg_order_fulfillment') }}
),

joined_data as (
    select
        o.order_id,
        o.customer_id,
        -- to join with dim_date if needed
        o.order_placement_date as order_date,
        ofu.on_time,
        ofu.in_full,
        ofu.ontime_in_full as otif
    from orders as o
    inner join order_fulfillment as ofu
        on o.order_id = ofu.order_id
)

select
    ROUND((SUM(on_time::numeric) / COUNT(*)) * 100, 2) as on_time_percentage,
    ROUND((SUM(in_full::numeric) / COUNT(*)) * 100, 2) as in_full_percentage,
    ROUND((SUM(otif::numeric) / COUNT(*)) * 100, 2) as otif_percentage
from joined_data

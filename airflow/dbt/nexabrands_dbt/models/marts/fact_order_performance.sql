with orders as (
    select *
    from {{ ref('stg_orders') }}
),

order_fulfillment as (
    select *
    from {{ ref('stg_order_fulfillment') }}
),

customers as (
    select *
    from {{ ref('stg_customers') }}
),

-- First get the base order data with fulfillment metrics
base_orders as (
    select
        o.order_id,
        o.customer_id,
        ofu.on_time,
        ofu.in_full,
        ofu.ontime_in_full as otif,
        COALESCE(c.customer_name, 'Unknown') as customer_name,
        COALESCE(c.city, 'Unknown') as city,
        DATE_TRUNC('day', o.order_placement_date) as order_date
    from orders as o
    inner join order_fulfillment as ofu
        on o.order_id = ofu.order_id
    left join customers as c
        on o.customer_id = c.customer_id
),

daily_metrics as (
    select
        order_date,
        city,
        customer_name,
        COUNT(*) as total_orders,
        ROUND(100.0 * AVG(on_time), 2) as on_time_percentage,
        ROUND(100.0 * AVG(in_full), 2) as in_full_percentage,
        ROUND(100.0 * AVG(otif), 2) as otif_percentage
    from base_orders
    group by order_date, city, customer_name
)

select
    order_date,
    city,
    customer_name,
    total_orders,
    on_time_percentage,
    in_full_percentage,
    otif_percentage
from daily_metrics
order by order_date desc, city asc, customer_name asc

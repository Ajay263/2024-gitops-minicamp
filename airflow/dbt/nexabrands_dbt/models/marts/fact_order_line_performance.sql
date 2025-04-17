-- models/fact_order_line_performance.sql
with order_lines as (
    select *
    from {{ ref('stg_order_lines') }}
),

orders as (
    select
        order_id,
        customer_id,
        order_placement_date
    from {{ ref('stg_orders') }}
)

select
    ol.order_line_id,
    ol.order_id,
    ol.product_id,
    ol.order_qty,
    ol.delivery_qty,
    ol.agreed_delivery_date,
    ol.actual_delivery_date,
    -- Calculate Volume Fill Rate: delivered_qty / order_qty (as a decimal)
    case
        when ol.order_qty > 0 and ol.delivery_qty = ol.order_qty then 1
      else 0
    end as volume_fill_rate,
    -- Calculate Line Fill Rate: 1 if fully delivered, else 0
    case
        when ol.delivery_qty = ol.order_qty then 1
        else 0
    end as line_fill_rate
from order_lines as ol
inner join orders as o
    on ol.order_id = o.order_id

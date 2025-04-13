-- models/dim_customers.sql
with customers as (
    select *
    from {{ ref('stg_customers') }}
),

customer_targets as (
    select *
    from {{ ref('stg_customer_targets') }}
)

select
    c.customer_id,
    c.customer_name,
    c.city,
    ct.ontime_target,
    ct.infull_target,
    ct.otif_target
from customers as c
left join customer_targets as ct
    on c.customer_id = ct.customer_id

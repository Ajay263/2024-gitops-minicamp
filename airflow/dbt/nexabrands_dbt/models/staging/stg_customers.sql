-- models/staging/stg_customers.sql
with source as (
    select * from {{ source('nexabrands_datawarehouse', 'customers') }}
),

staged as (
    select
        customer_id,
        customer_name,
        city
    from source
)

select * from staged

with source as (
    select *
    from {{ source('nexabrands_datawarehouse', 'customer_targets') }}
),

staged as (
    select
        customer_id,
        ontime_target,
        infull_target,
        otif_target
    from source
)

select *
from staged

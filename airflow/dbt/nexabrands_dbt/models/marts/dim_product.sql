with source as (
    select *
    from {{ ref('stg_products') }}
)

select
    product_id,
    product_name,
    category
from source

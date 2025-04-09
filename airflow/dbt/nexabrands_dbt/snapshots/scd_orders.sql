{% snapshot scd_orders %}
{{
    config(
        target_schema='nexabrands_external',
        unique_key='order_id',
        strategy='timestamp',
        updated_at='order_placement_date',
        invalidate_hard_deletes=True
    )
}}

select *
FROM  {{ source('nexabrands_datawarehouse', 'orders') }}

{% endsnapshot %}

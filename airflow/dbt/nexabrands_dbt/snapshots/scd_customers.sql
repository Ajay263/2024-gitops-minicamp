{% snapshot scd_customers %}
{{
    config(
        target_schema='nexabrands_external',
        unique_key='customer_id',
        strategy='check',
        check_cols='all',
        invalidate_hard_deletes=True
    )
}}

    select *
    from {{ source('nexabrands_datawarehouse', 'customers') }}

{% endsnapshot %}

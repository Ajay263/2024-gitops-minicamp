-- models/marts/dim_date.sql
with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2025-12-31' as date)"
    ) }}
),

enriched_dates as (
    select
        date_day as date1,
        -- Year
        cast(to_char(date_day, 'YYYYMMDD') as integer) as date_key,
        -- Quarter
        extract(year from date_day) as year,
        extract(quarter from date_day) as quarter,
        -- Month
        'Q'
        || extract(quarter from date_day)
        || '-'
        || extract(year from date_day) as quarter_year,
        extract(month from date_day) as month_number,
        to_char(date_day, 'Month') as month_name,
        -- Week
        to_char(date_day, 'Mon-YYYY') as month_year,
        extract(week from date_day) as week_number,
        date_trunc('week', date_day) as week_start_date,
        -- Day
        date_trunc('week', date_day) + interval '6 days' as week_end_date,
        extract(day from date_day) as day_of_month,
        extract(dow from date_day) as day_of_week_number,
        -- Fiscal year (assuming starts in April)
        to_char(date_day, 'Day') as day_name,
        -- Is this a weekday?
        case
            when extract(month from date_day) >= 4
                then extract(year from date_day)
            else extract(year from date_day) - 1
        end as fiscal_year,
        -- Create date key for joining
        case
            when extract(dow from date_day) in (0, 6) then 0
            else 1
        end as is_weekday
    from date_spine
)

select * from enriched_dates

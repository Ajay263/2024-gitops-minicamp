-- models/marts/dim_date.sql
WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2025-12-31' as date)"
    ) }}
),
enriched_dates AS (
    SELECT
        date_day as date,
        -- Year
        EXTRACT(YEAR FROM date_day) as year,
        -- Quarter
        EXTRACT(QUARTER FROM date_day) as quarter,
        'Q' || EXTRACT(QUARTER FROM date_day) || '-' || EXTRACT(YEAR FROM date_day) as quarter_year,
        -- Month
        EXTRACT(MONTH FROM date_day) as month_number,
        TO_CHAR(date_day, 'Month') as month_name,
        TO_CHAR(date_day, 'Mon-YYYY') as month_year,
        -- Week
        EXTRACT(WEEK FROM date_day) as week_number,
        DATE_TRUNC('week', date_day) as week_start_date,
        DATE_TRUNC('week', date_day) + INTERVAL '6 days' as week_end_date,
        -- Day
        EXTRACT(DAY FROM date_day) as day_of_month,
        EXTRACT(DOW FROM date_day) as day_of_week_number,
        TO_CHAR(date_day, 'Day') as day_name,
        -- Fiscal year (assuming starts in April)
        CASE
            WHEN EXTRACT(MONTH FROM date_day) >= 4
            THEN EXTRACT(YEAR FROM date_day)
            ELSE EXTRACT(YEAR FROM date_day) - 1
        END as fiscal_year,
        -- Is this a weekday?
        CASE
            WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN 0
            ELSE 1
        END as is_weekday,
        -- Create date key for joining
        TO_CHAR(date_day, 'YYYYMMDD')::INTEGER as date_key
    FROM date_spine
)
SELECT * FROM enriched_dates

select
    current_date as current_date_value,
    current_timestamp as current_timestamp_value,
    current_timestamp::timestamp at time zone 'UTC' as current_utc_timestamp

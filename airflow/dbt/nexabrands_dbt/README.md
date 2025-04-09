
UPDATE raw.scd_customers
SET city = 'Midrand'
WHERE customer_id = 10;


SELECT * FROM raw.scd_customers WHERE customer_id=10;






UPDATE raw.orders
SET customer_id = 71977
WHERE customer_id = 789220;


SELECT * FROM raw.orders
WHERE customer_id = 71977;

## Showing curent date
`dbt run --select check_current_date
`
your_model

``
dbt show --select check_current_date
``
dbt show --select  your_model


dbt show  --select  your_model

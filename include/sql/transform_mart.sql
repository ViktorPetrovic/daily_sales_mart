{% set yesterday = (logical_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') %}

TRUNCATE TABLE mart.daily_sales_mart;

INSERT INTO mart.daily_sales_mart (
    sale_date,
    customer_id,
    customer_name,
    segment,
    product_name,
    quantity,
    revenue_rub 
)
SELECT 
    o.order_date as sale_date,
    o.customer_id,
    c.customer_name,
    c.segment,
    o.product_name,
    SUM(o.quantity) as quantity,
    SUM(o.quantity * o.unit_price) as revenue_rub
FROM staging.orders_raw o
LEFT JOIN staging.customers_raw c ON c.customer_id = o.customer_id
WHERE o.status = 'completed' AND o.order_date = '{{ yesterday }}'::date
GROUP BY o.order_date, o.customer_id, c.customer_name, c.segment, o.product_name;
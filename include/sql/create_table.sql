CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;


CREATE TABLE IF NOT EXISTS staging.orders_raw (
    order_id INT PRIMARY KEY,
    order_date DATE,
    customer_id INT,
    product_name VARCHAR(50),
    quantity INT,
    unit_price DECIMAL(10, 2),
    currency VARCHAR(3) DEFAULT 'RUB',
    status VARCHAR(30),
    load_date DATE
);

CREATE TABLE IF NOT EXISTS staging.customers_raw (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(50),
    segment VARCHAR(30),
    city VARCHAR(20),
    load_date DATE
);

CREATE TABLE IF NOT EXISTS mart.daily_sales_mart (
    sale_date DATE,
    customer_id INT,
    customer_name VARCHAR(50),
    segment VARCHAR(30),
    product_name VARCHAR(50),
    quantity INT,
    revenue_rub DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    email VARCHAR NOT NULL,
    country VARCHAR NOT NULL,
    updated_at VARCHAR NOT NULL,
    loaded_run_id VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_orders (
    order_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR NOT NULL,
    amount DECIMAL(12, 2) NOT NULL,
    status VARCHAR NOT NULL,
    order_ts VARCHAR NOT NULL,
    updated_at VARCHAR NOT NULL,
    loaded_run_id VARCHAR NOT NULL
);

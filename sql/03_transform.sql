-- Staging tables
CREATE OR REPLACE TEMP TABLE stg_customers (
    customer_id VARCHAR,
    name VARCHAR,
    email VARCHAR,
    country VARCHAR,
    updated_at VARCHAR,
    loaded_run_id VARCHAR
);

CREATE OR REPLACE TEMP TABLE stg_orders (
    order_id VARCHAR,
    customer_id VARCHAR,
    amount DECIMAL(12, 2),
    status VARCHAR,
    order_ts VARCHAR,
    updated_at VARCHAR,
    loaded_run_id VARCHAR
);

-- Deduplication views: latest updated_at wins per business key
CREATE OR REPLACE TEMP VIEW dedup_stg_customers AS
SELECT * EXCLUDE rn FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS rn
    FROM stg_customers
) WHERE rn = 1;

CREATE OR REPLACE TEMP VIEW dedup_stg_orders AS
SELECT * EXCLUDE rn FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS rn
    FROM stg_orders
) WHERE rn = 1;

-- Idempotent upsert into dim_customer
INSERT INTO dim_customer (customer_id, name, email, country, updated_at, loaded_run_id)
SELECT customer_id, name, email, country, updated_at, loaded_run_id
FROM dedup_stg_customers
ON CONFLICT (customer_id) DO UPDATE SET
    name = EXCLUDED.name,
    email = EXCLUDED.email,
    country = EXCLUDED.country,
    updated_at = EXCLUDED.updated_at,
    loaded_run_id = EXCLUDED.loaded_run_id
WHERE EXCLUDED.updated_at >= dim_customer.updated_at;

-- Quarantine orphan orders referencing non-existent customers
INSERT INTO rejected_records (run_id, entity, reason, payload)
SELECT
    loaded_run_id AS run_id,
    'order' AS entity,
    'Foreign key violation: customer_id ' || customer_id || ' does not exist in dim_customer' AS reason,
    to_json(struct_pack(
        order_id := order_id,
        customer_id := customer_id,
        amount := amount,
        status := status,
        order_ts := order_ts,
        updated_at := updated_at
    )) AS payload
FROM dedup_stg_orders
WHERE customer_id NOT IN (SELECT customer_id FROM dim_customer);

-- Idempotent upsert valid orders into fact_orders
INSERT INTO fact_orders (order_id, customer_id, amount, status, order_ts, updated_at, loaded_run_id)
SELECT order_id, customer_id, amount, status, order_ts, updated_at, loaded_run_id
FROM dedup_stg_orders
WHERE customer_id IN (SELECT customer_id FROM dim_customer)
ON CONFLICT (order_id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id,
    amount = EXCLUDED.amount,
    status = EXCLUDED.status,
    order_ts = EXCLUDED.order_ts,
    updated_at = EXCLUDED.updated_at,
    loaded_run_id = EXCLUDED.loaded_run_id
WHERE EXCLUDED.updated_at >= fact_orders.updated_at;

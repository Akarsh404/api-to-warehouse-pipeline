CREATE TABLE IF NOT EXISTS raw_customers (
    run_id VARCHAR NOT NULL,
    endpoint VARCHAR NOT NULL,
    page INTEGER NOT NULL,
    fetched_at VARCHAR NOT NULL,
    http_status INTEGER NOT NULL,
    payload_hash VARCHAR NOT NULL,
    payload JSON NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_orders (
    run_id VARCHAR NOT NULL,
    endpoint VARCHAR NOT NULL,
    page INTEGER NOT NULL,
    fetched_at VARCHAR NOT NULL,
    http_status INTEGER NOT NULL,
    payload_hash VARCHAR NOT NULL,
    payload JSON NOT NULL
);

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional
import duckdb

from src.config import settings
from src.extract import ExtractedPage
from src.logging_setup import get_logger

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"


@dataclass
class TransformResult:
    customers_staged: int = 0
    customers_deduped: int = 0
    customer_duplicates_removed: int = 0
    orders_staged: int = 0
    orders_deduped: int = 0
    order_duplicates_removed: int = 0
    orphan_orders_rejected: int = 0
    orders_loaded: int = 0


def get_connection(db_path: Optional[str] = None) -> duckdb.DuckDBPyConnection:
    target_path = db_path if db_path is not None else settings.db_path
    if target_path != ":memory:":
        Path(target_path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(target_path)


def compute_payload_hash(payload: Any) -> str:
    canonical_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def init_raw_schema(conn: duckdb.DuckDBPyConnection) -> None:
    raw_sql_path = SQL_DIR / "01_raw.sql"
    if raw_sql_path.exists():
        with open(raw_sql_path, "r", encoding="utf-8") as f:
            conn.execute(f.read())
    else:
        conn.execute("""
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
        """)


def _get_raw_table_for_endpoint(endpoint: str) -> str:
    clean = endpoint.strip("/").lower()
    if "customer" in clean:
        return "raw_customers"
    if "order" in clean:
        return "raw_orders"
    raise ValueError(f"Unknown endpoint mapping for raw storage: {endpoint}")


def save_raw_page(
    conn: duckdb.DuckDBPyConnection,
    run_id: str,
    page: ExtractedPage,
) -> str:
    table_name = _get_raw_table_for_endpoint(page.endpoint)
    payload_hash = compute_payload_hash(page.payload)
    payload_json = json.dumps(page.payload)

    conn.execute(
        f"""
        INSERT INTO {table_name} (
            run_id, endpoint, page, fetched_at, http_status, payload_hash, payload
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            page.endpoint,
            page.page,
            page.fetched_at,
            page.http_status,
            payload_hash,
            payload_json,
        ],
    )
    return payload_hash


def save_raw_pages(
    conn: duckdb.DuckDBPyConnection,
    run_id: str,
    pages: List[ExtractedPage],
) -> int:
    logger = get_logger("raw_loader", run_id=run_id)
    saved_count = 0
    for page in pages:
        save_raw_page(conn, run_id, page)
        saved_count += 1
    logger.info(f"Persisted {saved_count} raw pages to warehouse")
    return saved_count


def fetch_raw_records(
    conn: duckdb.DuckDBPyConnection,
    run_id: str,
    endpoint: str,
) -> List[Dict[str, Any]]:
    table_name = _get_raw_table_for_endpoint(endpoint)
    rows = conn.execute(
        f"SELECT payload FROM {table_name} WHERE run_id = ? ORDER BY page ASC",
        [run_id],
    ).fetchall()

    all_records: List[Dict[str, Any]] = []
    for (payload_raw,) in rows:
        payload = json.loads(payload_raw) if isinstance(payload_raw, str) else payload_raw
        records = payload.get("data", [])
        if isinstance(records, list):
            all_records.extend(records)
    return all_records


def init_control_schema(conn: duckdb.DuckDBPyConnection) -> None:
    control_sql_path = SQL_DIR / "04_control.sql"
    if control_sql_path.exists():
        with open(control_sql_path, "r", encoding="utf-8") as f:
            conn.execute(f.read())


def save_rejected_records(
    conn: duckdb.DuckDBPyConnection,
    records: List[Any],
) -> int:
    if not records:
        return 0

    for rec in records:
        payload_json = json.dumps(rec.payload) if not isinstance(rec.payload, str) else rec.payload
        conn.execute(
            """
            INSERT INTO rejected_records (run_id, entity, reason, payload)
            VALUES (?, ?, ?, ?)
            """,
            [rec.run_id, rec.entity, rec.reason, payload_json],
        )
    return len(records)


def init_curated_schema(conn: duckdb.DuckDBPyConnection) -> None:
    curated_sql_path = SQL_DIR / "02_curated.sql"
    if curated_sql_path.exists():
        with open(curated_sql_path, "r", encoding="utf-8") as f:
            conn.execute(f.read())


def init_all_schemas(conn: duckdb.DuckDBPyConnection) -> None:
    init_raw_schema(conn)
    init_control_schema(conn)
    init_curated_schema(conn)


def transform_and_load_curated(
    conn: duckdb.DuckDBPyConnection,
    run_id: str,
    valid_customers: List[Dict[str, Any]],
    valid_orders: List[Dict[str, Any]],
) -> TransformResult:
    logger = get_logger("transformer", run_id=run_id)
    result = TransformResult()

    conn.execute("""
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
    """)

    if valid_customers:
        cust_tuples = [
            (c["customer_id"], c["name"], c["email"], c["country"], c["updated_at"], run_id)
            for c in valid_customers
        ]
        conn.executemany("INSERT INTO stg_customers VALUES (?, ?, ?, ?, ?, ?)", cust_tuples)
    result.customers_staged = len(valid_customers)

    if valid_orders:
        ord_tuples = [
            (o["order_id"], o["customer_id"], o["amount"], o["status"], o["order_ts"], o["updated_at"], run_id)
            for o in valid_orders
        ]
        conn.executemany("INSERT INTO stg_orders VALUES (?, ?, ?, ?, ?, ?, ?)", ord_tuples)
    result.orders_staged = len(valid_orders)

    conn.execute("""
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
    """)

    result.customers_deduped = conn.execute("SELECT COUNT(*) FROM dedup_stg_customers").fetchone()[0]
    result.customer_duplicates_removed = result.customers_staged - result.customers_deduped

    result.orders_deduped = conn.execute("SELECT COUNT(*) FROM dedup_stg_orders").fetchone()[0]
    result.order_duplicates_removed = result.orders_staged - result.orders_deduped

    conn.execute("""
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
    """)

    orphan_count = conn.execute("""
        SELECT COUNT(*) FROM dedup_stg_orders
        WHERE customer_id NOT IN (SELECT customer_id FROM dim_customer)
    """).fetchone()[0]
    result.orphan_orders_rejected = orphan_count

    if orphan_count > 0:
        conn.execute("""
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
        """)

    conn.execute("""
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
    """)

    result.orders_loaded = result.orders_deduped - result.orphan_orders_rejected

    logger.info(
        f"Transform complete: {result.customers_deduped} customers loaded ({result.customer_duplicates_removed} dupes removed), "
        f"{result.orders_loaded} orders loaded ({result.order_duplicates_removed} dupes removed, {result.orphan_orders_rejected} orphans quarantined)"
    )
    return result



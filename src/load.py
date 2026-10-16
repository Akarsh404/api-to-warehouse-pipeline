from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
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


def get_watermark(conn: duckdb.DuckDBPyConnection, endpoint: str) -> Optional[str]:
    row = conn.execute(
        "SELECT last_updated_at FROM watermarks WHERE endpoint = ?",
        [endpoint],
    ).fetchone()
    return row[0] if row else None


def update_watermark(
    conn: duckdb.DuckDBPyConnection,
    endpoint: str,
    last_updated_at: str,
) -> None:
    conn.execute(
        """
        INSERT INTO watermarks (endpoint, last_updated_at)
        VALUES (?, ?)
        ON CONFLICT (endpoint) DO UPDATE SET
            last_updated_at = EXCLUDED.last_updated_at
        WHERE EXCLUDED.last_updated_at > watermarks.last_updated_at;
        """,
        [endpoint, last_updated_at],
    )


def compute_high_watermark(records: List[Dict[str, Any]]) -> Optional[str]:
    parsed: List[Tuple[datetime, str]] = []
    for r in records:
        ts_str = r.get("updated_at")
        if isinstance(ts_str, str):
            try:
                dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                parsed.append((dt, ts_str))
            except ValueError:
                pass
    if not parsed:
        return None
    parsed.sort(key=lambda x: x[0])
    return parsed[-1][1]


def get_all_watermarks(conn: duckdb.DuckDBPyConnection) -> Dict[str, str]:
    rows = conn.execute("SELECT endpoint, last_updated_at FROM watermarks").fetchall()
    return {endpoint: watermark for endpoint, watermark in rows}


def start_pipeline_run(
    conn: duckdb.DuckDBPyConnection,
    run_id: str,
    started_at: Optional[str] = None,
) -> None:
    start_time = started_at or datetime.now().astimezone().isoformat()
    conn.execute(
        """
        INSERT INTO pipeline_runs (run_id, started_at, status)
        VALUES (?, ?, 'RUNNING')
        """,
        [run_id, start_time],
    )


def complete_pipeline_run(
    conn: duckdb.DuckDBPyConnection,
    run_id: str,
    status: str,
    ended_at: Optional[str] = None,
    rows_fetched: int = 0,
    rows_loaded: int = 0,
    rows_rejected: int = 0,
    duplicates_removed: int = 0,
    retries: int = 0,
) -> None:
    end_time = ended_at or datetime.now().astimezone().isoformat()
    conn.execute(
        """
        UPDATE pipeline_runs
        SET ended_at = ?,
            status = ?,
            rows_fetched = ?,
            rows_loaded = ?,
            rows_rejected = ?,
            duplicates_removed = ?,
            retries = ?
        WHERE run_id = ?
        """,
        [
            end_time,
            status,
            rows_fetched,
            rows_loaded,
            rows_rejected,
            duplicates_removed,
            retries,
            run_id,
        ],
    )


def get_pipeline_run(conn: duckdb.DuckDBPyConnection, run_id: str) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT run_id, started_at, ended_at, status, rows_fetched, rows_loaded,
               rows_rejected, duplicates_removed, retries
        FROM pipeline_runs
        WHERE run_id = ?
        """,
        [run_id],
    ).fetchone()

    if not row:
        return None

    return {
        "run_id": row[0],
        "started_at": row[1],
        "ended_at": row[2],
        "status": row[3],
        "rows_fetched": row[4],
        "rows_loaded": row[5],
        "rows_rejected": row[6],
        "duplicates_removed": row[7],
        "retries": row[8],
    }


def get_top_rejection_reasons(
    conn: duckdb.DuckDBPyConnection,
    run_id: str,
    limit: int = 3,
) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT reason, COUNT(*) as count
        FROM rejected_records
        WHERE run_id = ?
        GROUP BY reason
        ORDER BY count DESC
        LIMIT ?
        """,
        [run_id, limit],
    ).fetchall()
    return [{"reason": r[0], "count": r[1]} for r in rows]





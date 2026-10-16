import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional
import duckdb

from src.config import settings
from src.extract import ExtractedPage
from src.logging_setup import get_logger

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"


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

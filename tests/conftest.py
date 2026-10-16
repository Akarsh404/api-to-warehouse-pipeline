from typing import Any, Dict
import duckdb
import pytest

from src.load import get_connection, init_all_schemas


@pytest.fixture
def db_conn() -> duckdb.DuckDBPyConnection:
    conn = get_connection(":memory:")
    init_all_schemas(conn)
    yield conn
    conn.close()


@pytest.fixture
def sample_metrics() -> Dict[str, Any]:
    return {
        "run_id": "test_run_101",
        "status": "SUCCESS",
        "rows_fetched": 100,
        "rows_loaded": 90,
        "rows_rejected": 10,
        "duplicates_removed": 5,
        "retries": 1,
        "duration_seconds": 2.5,
        "top_rejection_reasons": [{"reason": "Missing email", "count": 10}],
    }

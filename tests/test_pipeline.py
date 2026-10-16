from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch
import pytest
import requests

from src.ai.base import SummaryProvider
from src.ai.summarizer import Summarizer, validate_summary_numbers
from src.extract import Extractor, NonRetryableHTTPError, RetryExhaustedError
from src.load import (
    complete_pipeline_run,
    get_watermark,
    save_rejected_records,
    start_pipeline_run,
    transform_and_load_curated,
    update_watermark,
)
from src.validate import validate_records


def test_1_dedup_keeps_latest_updated_at(db_conn):
    customers = [
        {
            "customer_id": "CUST-001",
            "name": "Alice Smith",
            "email": "alice@example.com",
            "country": "US",
            "updated_at": "2026-10-01T10:00:00Z",
        }
    ]
    orders = [
        {
            "order_id": "ORD-001",
            "customer_id": "CUST-001",
            "amount": 50.00,
            "status": "pending",
            "order_ts": "2026-10-01T09:00:00Z",
            "updated_at": "2026-10-01T10:00:00Z",
        },
        {
            "order_id": "ORD-001",
            "customer_id": "CUST-001",
            "amount": 50.00,
            "status": "completed",
            "order_ts": "2026-10-01T09:00:00Z",
            "updated_at": "2026-10-02T10:00:00Z",
        },
    ]

    t_res = transform_and_load_curated(db_conn, "RUN-TEST", customers, orders)
    assert t_res.orders_staged == 2
    assert t_res.orders_deduped == 1
    assert t_res.order_duplicates_removed == 1

    row = db_conn.execute("SELECT status, updated_at FROM fact_orders WHERE order_id = 'ORD-001'").fetchone()
    assert row[0] == "completed"
    assert row[1] == "2026-10-02T10:00:00Z"

    # Ingest an older duplicate to verify stale update is discarded
    stale_orders = [
        {
            "order_id": "ORD-001",
            "customer_id": "CUST-001",
            "amount": 50.00,
            "status": "cancelled",
            "order_ts": "2026-10-01T09:00:00Z",
            "updated_at": "2026-09-30T10:00:00Z",
        }
    ]
    transform_and_load_curated(db_conn, "RUN-TEST-2", customers, stale_orders)
    row_stale = db_conn.execute("SELECT status FROM fact_orders WHERE order_id = 'ORD-001'").fetchone()
    assert row_stale[0] == "completed"


def test_2_null_invalid_record_quarantined_not_loaded(db_conn):
    raw_customers = [
        {"customer_id": "CUST-BAD-1", "name": None, "email": "valid@example.com", "country": "US", "updated_at": "2026-10-01T10:00:00Z"}
    ]
    raw_orders = [
        {"order_id": "ORD-BAD-1", "customer_id": "CUST-001", "amount": "NOT_A_NUMBER", "status": "pending", "order_ts": "2026-10-01T09:00:00Z", "updated_at": "2026-10-01T10:00:00Z"}
    ]

    c_val = validate_records(raw_customers, "customers", "RUN-TEST")
    o_val = validate_records(raw_orders, "orders", "RUN-TEST")

    assert len(c_val.valid_records) == 0
    assert len(c_val.rejected_records) == 1
    assert "name" in c_val.rejected_records[0].reason

    assert len(o_val.valid_records) == 0
    assert len(o_val.rejected_records) == 1
    assert "amount" in o_val.rejected_records[0].reason

    save_rejected_records(db_conn, c_val.rejected_records + o_val.rejected_records)
    transform_and_load_curated(db_conn, "RUN-TEST", c_val.valid_records, o_val.valid_records)

    assert db_conn.execute("SELECT COUNT(*) FROM dim_customer").fetchone()[0] == 0
    assert db_conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0] == 0
    assert db_conn.execute("SELECT COUNT(*) FROM rejected_records WHERE run_id = 'RUN-TEST'").fetchone()[0] == 2


def test_3_retry_succeeds_after_two_500_errors():
    mock_session = MagicMock()
    resp_500_1 = MagicMock(status_code=500, text="Server Error 1")
    resp_500_2 = MagicMock(status_code=500, text="Server Error 2")
    resp_200 = MagicMock(status_code=200)
    resp_200.json.return_value = {"data": [{"customer_id": "C1"}], "total_pages": 1, "has_next": False}

    mock_session.get.side_effect = [resp_500_1, resp_500_2, resp_200]
    sleeps = []

    extractor = Extractor(
        base_url="http://mock",
        max_retries=3,
        session=mock_session,
        sleep_fn=lambda s: sleeps.append(s),
    )

    page = extractor.fetch_page("/customers", page=1)
    assert page.record_count == 1
    assert len(sleeps) == 2
    assert extractor.total_retries == 2


def test_4_retry_exhaustion_marks_run_failed(db_conn):
    mock_session = MagicMock()
    resp_500 = MagicMock(status_code=500, text="Internal Server Error")
    mock_session.get.return_value = resp_500

    extractor = Extractor(
        base_url="http://mock",
        max_retries=2,
        session=mock_session,
        sleep_fn=lambda s: None,
    )

    with pytest.raises(RetryExhaustedError):
        extractor.fetch_page("/customers", page=1)

    start_pipeline_run(db_conn, run_id="FAIL-RUN-1")
    complete_pipeline_run(db_conn, run_id="FAIL-RUN-1", status="FAILED", retries=extractor.total_retries)

    row = db_conn.execute("SELECT status, retries FROM pipeline_runs WHERE run_id = 'FAIL-RUN-1'").fetchone()
    assert row[0] == "FAILED"
    assert row[1] == 2


def test_5_idempotency_two_runs_identical_counts(db_conn):
    customers = [
        {"customer_id": f"CUST-{i:03d}", "name": f"User {i}", "email": f"u{i}@test.com", "country": "US", "updated_at": "2026-10-01T10:00:00Z"}
        for i in range(1, 11)
    ]
    orders = [
        {"order_id": f"ORD-{i:03d}", "customer_id": f"CUST-{i:03d}", "amount": 25.00, "status": "completed", "order_ts": "2026-10-01T09:00:00Z", "updated_at": "2026-10-01T10:00:00Z"}
        for i in range(1, 11)
    ]

    transform_and_load_curated(db_conn, "RUN-IDEM-1", customers, orders)
    c_count_1 = db_conn.execute("SELECT COUNT(*) FROM dim_customer").fetchone()[0]
    o_count_1 = db_conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0]
    assert c_count_1 == 10
    assert o_count_1 == 10

    # Second run with exact same data
    transform_and_load_curated(db_conn, "RUN-IDEM-2", customers, orders)
    c_count_2 = db_conn.execute("SELECT COUNT(*) FROM dim_customer").fetchone()[0]
    o_count_2 = db_conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0]
    assert c_count_1 == c_count_2
    assert o_count_1 == o_count_2


def test_6_orphan_order_is_rejected(db_conn):
    valid_customers = [
        {"customer_id": "CUST-REAL", "name": "Real Customer", "email": "real@example.com", "country": "US", "updated_at": "2026-10-01T10:00:00Z"}
    ]
    orders_with_orphan = [
        {"order_id": "ORD-VALID", "customer_id": "CUST-REAL", "amount": 10.00, "status": "completed", "order_ts": "2026-10-01T09:00:00Z", "updated_at": "2026-10-01T10:00:00Z"},
        {"order_id": "ORD-ORPHAN", "customer_id": "CUST-GHOST-999", "amount": 99.00, "status": "pending", "order_ts": "2026-10-01T09:00:00Z", "updated_at": "2026-10-01T10:00:00Z"},
    ]

    t_res = transform_and_load_curated(db_conn, "RUN-ORPHAN", valid_customers, orders_with_orphan)
    assert t_res.orphan_orders_rejected == 1
    assert t_res.orders_loaded == 1

    loaded_orders = [r[0] for r in db_conn.execute("SELECT order_id FROM fact_orders").fetchall()]
    assert "ORD-VALID" in loaded_orders
    assert "ORD-ORPHAN" not in loaded_orders

    rejected = db_conn.execute("SELECT reason, payload FROM rejected_records WHERE run_id = 'RUN-ORPHAN'").fetchall()
    assert len(rejected) == 1
    assert "CUST-GHOST-999" in rejected[0][0]


def test_7_ai_provider_exception_falls_back_to_rule_based(db_conn, sample_metrics):
    mock_provider = MagicMock(spec=SummaryProvider)
    mock_provider.summarize.side_effect = requests.exceptions.ConnectionError("Ollama daemon unavailable")

    summarizer = Summarizer(provider=mock_provider, provider_type="ollama", run_id=sample_metrics["run_id"])
    result = summarizer.generate_summary(sample_metrics)

    assert result.provider == "rule_based"
    assert result.model == "template"
    assert sample_metrics["run_id"] in result.summary
    assert str(sample_metrics["rows_fetched"]) in result.summary

    summarizer.save_summary(db_conn, result)
    saved = db_conn.execute("SELECT provider, validated FROM run_summaries WHERE run_id = ?", [sample_metrics["run_id"]]).fetchone()
    assert saved[0] == "rule_based"
    assert saved[1] is True


def test_8_ai_summary_numeric_validation(sample_metrics):
    valid_summary = (
        f"Pipeline run {sample_metrics['run_id']} finished with status {sample_metrics['status']}. "
        f"Processed {sample_metrics['rows_fetched']} rows and loaded {sample_metrics['rows_loaded']} records. "
        f"{sample_metrics['rows_rejected']} rows were quarantined."
    )
    assert validate_summary_numbers(valid_summary, sample_metrics) is True

    # Hallucinated numbers not present in metrics
    hallucinated_summary = "Pipeline run processed 9999 rows and rejected 8888 records."
    assert validate_summary_numbers(hallucinated_summary, sample_metrics) is False


def test_watermark_advancement(db_conn):
    assert get_watermark(db_conn, "/customers") is None

    update_watermark(db_conn, "/customers", "2026-10-05T10:00:00Z")
    assert get_watermark(db_conn, "/customers") == "2026-10-05T10:00:00Z"

    # Regressive timestamp does not overwrite
    update_watermark(db_conn, "/customers", "2026-10-01T10:00:00Z")
    assert get_watermark(db_conn, "/customers") == "2026-10-05T10:00:00Z"

    # Progressive timestamp advances watermark
    update_watermark(db_conn, "/customers", "2026-10-20T10:00:00Z")
    assert get_watermark(db_conn, "/customers") == "2026-10-20T10:00:00Z"

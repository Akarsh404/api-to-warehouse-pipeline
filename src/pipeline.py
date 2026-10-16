import argparse
from datetime import datetime, timezone
import json
from typing import Any, Dict, Optional
import uuid

from src.config import settings
from src.extract import Extractor, RetryExhaustedError
from src.load import (
    complete_pipeline_run,
    compute_high_watermark,
    get_connection,
    get_pipeline_run,
    get_top_rejection_reasons,
    get_watermark,
    init_all_schemas,
    save_raw_pages,
    save_rejected_records,
    start_pipeline_run,
    transform_and_load_curated,
    update_watermark,
)
from src.logging_setup import get_logger, setup_logging
from src.validate import validate_records


def run_pipeline(
    db_path: Optional[str] = None,
    base_url: Optional[str] = None,
    run_id: Optional[str] = None,
    full_refresh: bool = False,
    fail_rate: Optional[float] = None,
) -> Dict[str, Any]:
    active_run_id = run_id or f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    logger = setup_logging(run_id=active_run_id)
    logger.info(f"Initiating pipeline run={active_run_id}")

    conn = get_connection(db_path)
    init_all_schemas(conn)

    start_time = datetime.now(timezone.utc)
    start_pipeline_run(conn, run_id=active_run_id, started_at=start_time.isoformat())

    extractor = Extractor(base_url=base_url, run_id=active_run_id)

    wm_customers = None if full_refresh else get_watermark(conn, "/customers")
    wm_orders = None if full_refresh else get_watermark(conn, "/orders")

    rows_fetched = 0
    raw_customers_list = []
    raw_orders_list = []
    endpoint_failures = 0

    # 1. Extract Customers
    try:
        c_res = extractor.extract_endpoint("/customers", updated_since=wm_customers, fail_rate=fail_rate)
        save_raw_pages(conn, active_run_id, c_res.pages)
        rows_fetched += c_res.total_records
        for page in c_res.pages:
            items = page.payload.get("data", [])
            if isinstance(items, list):
                raw_customers_list.extend(items)
    except RetryExhaustedError as exc:
        logger.error(f"Customer extraction failed: {exc}")
        endpoint_failures += 1

    # 2. Extract Orders
    try:
        o_res = extractor.extract_endpoint("/orders", updated_since=wm_orders, fail_rate=fail_rate)
        save_raw_pages(conn, active_run_id, o_res.pages)
        rows_fetched += o_res.total_records
        for page in o_res.pages:
            items = page.payload.get("data", [])
            if isinstance(items, list):
                raw_orders_list.extend(items)
    except RetryExhaustedError as exc:
        logger.error(f"Orders extraction failed: {exc}")
        endpoint_failures += 1

    # 3. Determine run status
    if endpoint_failures == 2:
        final_status = "FAILED"
    elif endpoint_failures == 1:
        final_status = "PARTIAL"
    else:
        final_status = "SUCCESS"

    # 4. Validation
    c_val = validate_records(raw_customers_list, "customers", run_id=active_run_id)
    o_val = validate_records(raw_orders_list, "orders", run_id=active_run_id)

    initial_rejected = len(c_val.rejected_records) + len(o_val.rejected_records)
    save_rejected_records(conn, c_val.rejected_records + o_val.rejected_records)

    # 5. Transformation, Dedup, Upsert
    t_res = transform_and_load_curated(
        conn,
        run_id=active_run_id,
        valid_customers=c_val.valid_records,
        valid_orders=o_val.valid_records,
    )

    # 6. Watermark Progression
    if raw_customers_list and c_val.valid_records:
        new_wm_c = compute_high_watermark(c_val.valid_records)
        if new_wm_c:
            update_watermark(conn, "/customers", new_wm_c)

    if raw_orders_list and o_val.valid_records:
        new_wm_o = compute_high_watermark(o_val.valid_records)
        if new_wm_o:
            update_watermark(conn, "/orders", new_wm_o)

    # 7. Audit Metrics Recording
    end_time = datetime.now(timezone.utc)
    total_rejected = initial_rejected + t_res.orphan_orders_rejected
    total_loaded = t_res.customers_deduped + t_res.orders_loaded
    total_dupes = t_res.customer_duplicates_removed + t_res.order_duplicates_removed

    complete_pipeline_run(
        conn=conn,
        run_id=active_run_id,
        status=final_status,
        ended_at=end_time.isoformat(),
        rows_fetched=rows_fetched,
        rows_loaded=total_loaded,
        rows_rejected=total_rejected,
        duplicates_removed=total_dupes,
        retries=extractor.total_retries,
    )

    metrics = get_pipeline_run(conn, active_run_id) or {}
    metrics["top_rejection_reasons"] = get_top_rejection_reasons(conn, active_run_id, limit=3)
    duration = (end_time - start_time).total_seconds()
    metrics["duration_seconds"] = round(duration, 2)

    logger.info(
        f"Pipeline run finished: status={final_status} fetched={rows_fetched} "
        f"loaded={total_loaded} rejected={total_rejected} dupes_removed={total_dupes} "
        f"retries={extractor.total_retries} duration={metrics['duration_seconds']}s"
    )

    return metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="API-to-Warehouse Ingestion Pipeline")
    parser.add_argument("--db-path", type=str, default=None, help="DuckDB warehouse file path")
    parser.add_argument("--api-url", type=str, default=None, help="Base URL for REST API")
    parser.add_argument("--run-id", type=str, default=None, help="Custom identifier for run")
    parser.add_argument("--full-refresh", action="store_true", help="Ignore watermarks and do full fetch")
    parser.add_argument("--fail-rate", type=float, default=None, help="Injected failure rate (0.0 to 1.0)")
    args = parser.parse_args()

    metrics = run_pipeline(
        db_path=args.db_path,
        base_url=args.api_url,
        run_id=args.run_id,
        full_refresh=args.full_refresh,
        fail_rate=args.fail_rate,
    )
    print(json.dumps(metrics, indent=2))

from typing import Any, Dict

from src.ai.base import SummaryProvider


class RuleBasedProvider(SummaryProvider):
    def summarize(self, metrics: Dict[str, Any]) -> str:
        run_id = metrics.get("run_id", "N/A")
        status = metrics.get("status", "UNKNOWN")
        rows_fetched = metrics.get("rows_fetched", 0)
        rows_loaded = metrics.get("rows_loaded", 0)
        rows_rejected = metrics.get("rows_rejected", 0)
        duplicates_removed = metrics.get("duplicates_removed", 0)
        retries = metrics.get("retries", 0)
        duration = metrics.get("duration_seconds", 0.0)

        top_reasons = metrics.get("top_rejection_reasons", [])
        reason_text = ""
        if top_reasons:
            top_reason = top_reasons[0].get("reason", "")
            reason_text = f" The primary quarantine cause was: '{top_reason}'."

        s1 = f"Pipeline run {run_id} finished with status {status} in {duration} seconds."
        s2 = f"Ingestion processed {rows_fetched} total rows, successfully loading {rows_loaded} records into the warehouse."
        s3 = f"A total of {duplicates_removed} duplicate records were deduplicated, and {rows_rejected} records were quarantined.{reason_text}"

        if status == "SUCCESS" and rows_rejected == 0:
            s4 = "No anomalies detected; pipeline execution is healthy and no operational action is required."
        elif status == "SUCCESS":
            s4 = f"Operation succeeded with {retries} retries; recommend reviewing the rejected_records table for quarantined payloads."
        else:
            s4 = f"Pipeline finished in {status} state with {retries} retries; recommend inspecting API connectivity and re-running."

        return f"{s1} {s2} {s3} {s4}"

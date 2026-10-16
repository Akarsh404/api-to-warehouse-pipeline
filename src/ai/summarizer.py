from dataclasses import dataclass
import logging
import re
from typing import Any, Dict, Optional, Set
import duckdb

from src.ai.base import SummaryProvider
from src.ai.ollama_provider import OllamaProvider
from src.ai.rule_based import RuleBasedProvider
from src.config import settings
from src.logging_setup import get_logger

PROMPT_VERSION = "v1.0"


@dataclass
class SummaryResult:
    run_id: str
    summary: str
    provider: str
    model: str
    prompt_version: str
    validated: bool


def extract_numbers_from_summary(text: str) -> Set[str]:
    return set(re.findall(r"\b\d+(?:\.\d+)?\b", text))


def get_expected_metrics_numbers(metrics: Dict[str, Any]) -> Set[str]:
    valid_nums: Set[str] = set()

    for field in ("rows_fetched", "rows_loaded", "rows_rejected", "duplicates_removed", "retries"):
        val = metrics.get(field)
        if val is not None:
            valid_nums.add(str(val))

    duration = metrics.get("duration_seconds")
    if duration is not None:
        valid_nums.add(str(duration))
        valid_nums.add(str(int(round(float(duration)))))

    for item in metrics.get("top_rejection_reasons", []):
        if "count" in item:
            valid_nums.add(str(item["count"]))

    run_id = str(metrics.get("run_id", ""))
    for num_part in re.findall(r"\d+", run_id):
        valid_nums.add(num_part)

    # Allow common small ordinal / sentence counters
    valid_nums.update({"1", "2", "3", "4", "5"})
    return valid_nums


def validate_summary_numbers(summary: str, metrics: Dict[str, Any]) -> bool:
    found_numbers = extract_numbers_from_summary(summary)
    if not found_numbers:
        return True

    expected = get_expected_metrics_numbers(metrics)
    for num in found_numbers:
        if num not in expected:
            if num.endswith(".0") and num[:-2] in expected:
                continue
            return False
    return True


class Summarizer:
    def __init__(
        self,
        provider: Optional[SummaryProvider] = None,
        provider_type: Optional[str] = None,
        run_id: str = "N/A",
    ) -> None:
        self.run_id = run_id
        self.logger = get_logger("summarizer", run_id=run_id)
        self.provider_type = (provider_type or settings.summary_provider).lower()

        self.rule_based_provider = RuleBasedProvider()
        if provider:
            self.active_provider = provider
        elif self.provider_type == "ollama":
            self.active_provider = OllamaProvider()
        else:
            self.active_provider = self.rule_based_provider

    def generate_summary(self, metrics: Dict[str, Any]) -> SummaryResult:
        run_id = metrics.get("run_id", self.run_id)
        provider_name = self.provider_type
        model_name = getattr(self.active_provider, "model", "template")

        raw_summary = ""
        is_fallback = False

        if self.provider_type == "ollama":
            try:
                self.logger.info("Generating run summary with OllamaProvider")
                raw_summary = self.active_provider.summarize(metrics)
                words = raw_summary.strip().split()
                if len(words) < 5 or len(words) > 150:
                    raise ValueError(f"Summary length anomalous ({len(words)} words)")
            except Exception as exc:
                self.logger.warning(f"Ollama generation failed ({exc}); falling back to RuleBasedProvider")
                is_fallback = True

        if self.provider_type != "ollama" or is_fallback:
            provider_name = "rule_based"
            model_name = "template"
            raw_summary = self.rule_based_provider.summarize(metrics)

        is_validated = validate_summary_numbers(raw_summary, metrics)
        if not is_validated:
            self.logger.warning("Summary failed numerical validation check against run metrics")

        return SummaryResult(
            run_id=run_id,
            summary=raw_summary,
            provider=provider_name,
            model=model_name,
            prompt_version=PROMPT_VERSION,
            validated=is_validated,
        )

    def save_summary(
        self,
        conn: duckdb.DuckDBPyConnection,
        summary_result: SummaryResult,
    ) -> None:
        conn.execute(
            """
            INSERT INTO run_summaries (run_id, summary, provider, model, prompt_version, validated)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (run_id) DO UPDATE SET
                summary = EXCLUDED.summary,
                provider = EXCLUDED.provider,
                model = EXCLUDED.model,
                prompt_version = EXCLUDED.prompt_version,
                validated = EXCLUDED.validated;
            """,
            [
                summary_result.run_id,
                summary_result.summary,
                summary_result.provider,
                summary_result.model,
                summary_result.prompt_version,
                summary_result.validated,
            ],
        )
        self.logger.info(f"Persisted run summary to run_summaries (validated={summary_result.validated})")

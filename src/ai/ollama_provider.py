import json
from typing import Any, Dict, Optional
import requests

from src.ai.base import SummaryProvider
from src.config import settings


class OllamaProvider(SummaryProvider):
    def __init__(
        self,
        host: Optional[str] = None,
        model: Optional[str] = None,
        timeout_seconds: float = 8.0,
    ) -> None:
        self.host = (host or settings.ollama_host).rstrip("/")
        self.model = model or settings.ollama_model
        self.timeout = timeout_seconds

    def _build_prompt(self, metrics: Dict[str, Any]) -> str:
        return (
            "You are an automated data platform assistant. "
            "Write a concise 3 to 5 sentence operational summary of this data pipeline execution. "
            "Describe: (1) what happened, (2) anomalies/quarantined records/retries, and (3) recommended action. "
            "Do not invent any numbers; use only the exact figures provided below:\n\n"
            f"Run ID: {metrics.get('run_id')}\n"
            f"Status: {metrics.get('status')}\n"
            f"Duration: {metrics.get('duration_seconds')} seconds\n"
            f"Rows Fetched: {metrics.get('rows_fetched')}\n"
            f"Rows Loaded: {metrics.get('rows_loaded')}\n"
            f"Rows Rejected: {metrics.get('rows_rejected')}\n"
            f"Duplicates Removed: {metrics.get('duplicates_removed')}\n"
            f"Retries: {metrics.get('retries')}\n"
            f"Top Rejection Reasons: {json.dumps(metrics.get('top_rejection_reasons', []))}\n\n"
            "Respond ONLY with the 3-5 sentence plain-text summary."
        )

    def summarize(self, metrics: Dict[str, Any]) -> str:
        url = f"{self.host}/api/generate"
        prompt = self._build_prompt(metrics)
        payload = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
        }

        response = requests.post(url, json=payload, timeout=self.timeout)
        response.raise_for_status()

        data = response.json()
        summary = data.get("response", "").strip()
        if not summary:
            raise ValueError("Ollama returned an empty summary response")

        return summary

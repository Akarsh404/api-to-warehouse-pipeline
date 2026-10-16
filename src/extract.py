from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging
import random
import time
from typing import Any, Callable, Dict, List, Optional
import requests

from src.config import settings
from src.logging_setup import get_logger


class ExtractionError(Exception):
    """Base exception for extraction failures."""
    pass


class NonRetryableHTTPError(ExtractionError):
    """Raised when encountering a non-retryable 4xx client error."""
    pass


class RetryExhaustedError(ExtractionError):
    """Raised when an extraction operation exhausts its retry budget."""
    pass


@dataclass(frozen=True)
class ExtractedPage:
    endpoint: str
    page: int
    http_status: int
    payload: Dict[str, Any]
    fetched_at: str
    record_count: int


@dataclass
class ExtractionResult:
    pages: List[ExtractedPage] = field(default_factory=list)
    total_records: int = 0
    retries_count: int = 0


class Extractor:
    def __init__(
        self,
        base_url: Optional[str] = None,
        max_retries: Optional[int] = None,
        timeout: Optional[float] = None,
        run_id: str = "N/A",
        base_backoff: float = 0.5,
        max_backoff: float = 30.0,
        session: Optional[requests.Session] = None,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        self.base_url = (base_url or settings.api_base_url).rstrip("/")
        self.max_retries = max_retries if max_retries is not None else settings.max_retries
        self.timeout = timeout if timeout is not None else settings.request_timeout_seconds
        self.run_id = run_id
        self.base_backoff = base_backoff
        self.max_backoff = max_backoff
        self.session = session or requests.Session()
        self.sleep_fn = sleep_fn
        self.logger = get_logger("extractor", run_id=run_id)

    def _calculate_backoff(self, attempt: int, response: Optional[requests.Response]) -> float:
        if response is not None and response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                try:
                    return min(float(retry_after), self.max_backoff)
                except ValueError:
                    pass

        # Exponential backoff with random full jitter
        exponential = self.base_backoff * (2 ** (attempt - 1))
        jittered = exponential + random.uniform(0.05, 0.25)
        return min(jittered, self.max_backoff)

    def fetch_page(
        self,
        endpoint: str,
        page: int,
        limit: int = 20,
        updated_since: Optional[str] = None,
        fail_rate: Optional[float] = None,
    ) -> ExtractedPage:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        params: Dict[str, Any] = {"page": page, "limit": limit}
        if updated_since:
            params["updated_since"] = updated_since
        if fail_rate is not None:
            params["fail_rate"] = fail_rate

        attempt = 1
        max_attempts = self.max_retries + 1

        while attempt <= max_attempts:
            self.logger.info(
                f"Requesting endpoint={endpoint} page={page} attempt={attempt}/{max_attempts}"
            )
            response: Optional[requests.Response] = None

            try:
                response = self.session.get(url, params=params, timeout=self.timeout)

                # Success
                if 200 <= response.status_code < 300:
                    payload = response.json()
                    records = payload.get("data", [])
                    record_count = len(records) if isinstance(records, list) else 0
                    fetched_at = datetime.now(timezone.utc).isoformat()

                    self.logger.info(
                        f"Fetched endpoint={endpoint} page={page} status={response.status_code} "
                        f"records={record_count}"
                    )
                    return ExtractedPage(
                        endpoint=endpoint,
                        page=page,
                        http_status=response.status_code,
                        payload=payload,
                        fetched_at=fetched_at,
                        record_count=record_count,
                    )

                # Retryable rate limit (429) or server errors (5xx)
                if response.status_code == 429 or 500 <= response.status_code < 600:
                    self.logger.warning(
                        f"Retryable HTTP {response.status_code} for endpoint={endpoint} page={page} "
                        f"(attempt {attempt}/{max_attempts})"
                    )
                else:
                    # Non-retryable 4xx
                    err_msg = (
                        f"Non-retryable HTTP {response.status_code} for endpoint={endpoint} page={page}: "
                        f"{response.text}"
                    )
                    self.logger.error(err_msg)
                    raise NonRetryableHTTPError(err_msg)

            except requests.exceptions.RequestException as exc:
                self.logger.warning(
                    f"Network error on endpoint={endpoint} page={page} (attempt {attempt}/{max_attempts}): {exc}"
                )

            # Check if retry budget is exhausted
            if attempt >= max_attempts:
                status_desc = f"HTTP {response.status_code}" if response is not None else "network timeout/error"
                err_msg = (
                    f"Retries exhausted ({max_attempts} attempts) for endpoint={endpoint} page={page} "
                    f"due to {status_desc}"
                )
                self.logger.error(err_msg)
                raise RetryExhaustedError(err_msg)

            sleep_duration = self._calculate_backoff(attempt, response)
            self.logger.info(f"Sleeping {sleep_duration:.2f}s before retry attempt {attempt + 1}")
            self.sleep_fn(sleep_duration)
            attempt += 1

        raise RetryExhaustedError(f"Extraction failed for {endpoint} page {page}")

    def extract_endpoint(
        self,
        endpoint: str,
        limit: int = 20,
        updated_since: Optional[str] = None,
        fail_rate: Optional[float] = None,
    ) -> ExtractionResult:
        result = ExtractionResult()
        current_page = 1

        self.logger.info(f"Starting extraction for endpoint={endpoint}")

        while True:
            # We track attempt count to compute total retries made
            page_data = self.fetch_page(
                endpoint=endpoint,
                page=current_page,
                limit=limit,
                updated_since=updated_since,
                fail_rate=fail_rate,
            )

            result.pages.append(page_data)
            result.total_records += page_data.record_count

            has_next = page_data.payload.get("has_next", False)
            total_pages = page_data.payload.get("total_pages", current_page)

            if not has_next or current_page >= total_pages:
                break
            current_page += 1

        self.logger.info(
            f"Completed extraction for endpoint={endpoint}: total_pages={len(result.pages)} "
            f"total_records={result.total_records}"
        )
        return result

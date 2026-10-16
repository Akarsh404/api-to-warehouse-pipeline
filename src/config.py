"""Configuration module for the API-to-warehouse pipeline.

Loads application settings from environment variables with safe defaults.
"""

from dataclasses import dataclass
import os
from pathlib import Path

# Base directory of the repository (parent of src/)
BASE_DIR = Path(__file__).resolve().parent.parent

# Load environment variables from .env file if python-dotenv is installed
try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=BASE_DIR / ".env")
except ImportError:
    pass


def _get_int(key: str, default: int) -> int:
    """Parse an integer environment variable with a fallback default."""
    val = os.getenv(key)
    if val is None or not val.strip():
        return default
    try:
        return int(val.strip())
    except ValueError:
        return default


def _get_float(key: str, default: float) -> float:
    """Parse a float environment variable with a fallback default."""
    val = os.getenv(key)
    if val is None or not val.strip():
        return default
    try:
        return float(val.strip())
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    """Pipeline configuration settings."""

    api_base_url: str = os.getenv("API_BASE_URL", "http://127.0.0.1:8000")
    db_path: str = os.getenv("DB_PATH", "data/warehouse.duckdb")
    summary_provider: str = os.getenv("SUMMARY_PROVIDER", "rule_based")
    ollama_host: str = os.getenv("OLLAMA_HOST", "http://localhost:11434")
    ollama_model: str = os.getenv("OLLAMA_MODEL", "llama3.2")
    max_retries: int = _get_int("MAX_RETRIES", 3)
    request_timeout_seconds: float = _get_float("REQUEST_TIMEOUT_SECONDS", 10.0)


def get_settings() -> Settings:
    """Instantiate and return the current settings."""
    return Settings(
        api_base_url=os.getenv("API_BASE_URL", "http://127.0.0.1:8000"),
        db_path=os.getenv("DB_PATH", "data/warehouse.duckdb"),
        summary_provider=os.getenv("SUMMARY_PROVIDER", "rule_based"),
        ollama_host=os.getenv("OLLAMA_HOST", "http://localhost:11434"),
        ollama_model=os.getenv("OLLAMA_MODEL", "llama3.2"),
        max_retries=_get_int("MAX_RETRIES", 3),
        request_timeout_seconds=_get_float("REQUEST_TIMEOUT_SECONDS", 10.0),
    )


# Default settings instance for direct import
settings = get_settings()

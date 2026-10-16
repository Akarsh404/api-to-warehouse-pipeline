from abc import ABC, abstractmethod
from typing import Any, Dict


class SummaryProvider(ABC):
    @abstractmethod
    def summarize(self, metrics: Dict[str, Any]) -> str:
        """Generate a natural language summary from aggregated pipeline metrics."""
        pass

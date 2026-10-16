import logging
import sys
from typing import Optional


class RunIdFilter(logging.Filter):
    def __init__(self, run_id: str = "N/A") -> None:
        super().__init__()
        self.run_id = run_id

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "run_id"):
            record.run_id = self.run_id
        return True


def setup_logging(run_id: str = "N/A", log_level: int = logging.INFO) -> logging.Logger:
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] [run_id=%(run_id)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    handler.addFilter(RunIdFilter(run_id=run_id))

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers.clear()
    root_logger.addHandler(handler)

    return logging.getLogger("pipeline")


def get_logger(name: str, run_id: Optional[str] = None) -> logging.Logger:
    logger = logging.getLogger(name)
    if run_id:
        logger.addFilter(RunIdFilter(run_id=run_id))
    return logger

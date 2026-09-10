"""Structured JSON logging configuration with distributed context propagation."""

import json
import logging
import sys
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any

# Context variables for distributed tracing across async tasks
current_correlation_id: ContextVar[str | None] = ContextVar(
    "current_correlation_id",
    default=None,
)
current_resource_id: ContextVar[str | None] = ContextVar(
    "current_resource_id",
    default=None,
)


class JSONLogFormatter(logging.Formatter):
    """Custom logging formatter outputting standard JSON objects for ELK and Grafana Loki."""

    def format(self, record: logging.LogRecord) -> str:
        """Formats log record into a single-line JSON string.

        Args:
            record: Standard Python logging record.

        Returns:
            JSON-serialized log message.
        """
        log_payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Inject tracing metadata from async context
        correlation_id = current_correlation_id.get()
        if correlation_id:
            log_payload["correlation_id"] = correlation_id

        resource_id = current_resource_id.get()
        if resource_id:
            log_payload["resource_id"] = resource_id

        # Include exception traceback if present
        if record.exc_info:
            log_payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_payload, ensure_ascii=False)


def setup_logging(log_level: str = "INFO", json_mode: bool = True) -> None:
    """Configures root logger with JSON or standard formatting.

    Args:
        log_level: Logging severity threshold.
        json_mode: If True, uses JSONLogFormatter; otherwise uses standard text format.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Remove pre-existing handlers to prevent log duplication
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    if json_mode:
        stream_handler.setFormatter(JSONLogFormatter())
    else:
        stream_handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"),
        )

    root_logger.addHandler(stream_handler)

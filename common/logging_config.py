"""
Structured (JSON) logging setup.

Financial-grade systems need machine-parseable logs so anomalies, errors
and audit events can be shipped to Splunk / ELK / Datadog and alerted on.
Plain `print()` or unstructured log lines are not acceptable in this codebase.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any


class JsonFormatter(logging.Formatter):
    """Renders each LogRecord as a single-line JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Allow callers to attach structured context via `extra={"extra_fields": {...}}`
        extra_fields = getattr(record, "extra_fields", None)
        if extra_fields:
            payload.update(extra_fields)

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str)


def configure_logging(service_name: str, log_level: str = "INFO") -> logging.Logger:
    """Idempotently configure root logging for a service and return its logger."""
    root = logging.getLogger()
    root.setLevel(log_level.upper())

    # Avoid duplicate handlers if configure_logging() is called more than once.
    if not any(isinstance(h, logging.StreamHandler) for h in root.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        root.addHandler(handler)

    logger = logging.getLogger(service_name)
    return logger


def log_with_context(
    logger: logging.Logger, level: int, message: str, **context: Any
) -> None:
    """Helper to attach structured fields to a log line.

    Example:
        log_with_context(logger, logging.WARNING, "Anomaly detected",
                          symbol="BTCUSDT", deviation=4.2, price=63501.12)
    """
    logger.log(level, message, extra={"extra_fields": context})

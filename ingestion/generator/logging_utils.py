"""Structured logging configuration for generator workflows."""

from __future__ import annotations

import json
import logging
from typing import Any


class JsonFormatter(logging.Formatter):
    """Emit logs as JSON for easier ingestion by observability tools."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, datefmt="%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        context = getattr(record, "context", None)
        if context is not None:
            payload["context"] = context

        return json.dumps(payload, separators=(",", ":"))


def configure_logging(level: str = "INFO") -> logging.Logger:
    """Configure root logger with JSON formatter and return module logger."""
    resolved_level = getattr(logging, level.upper(), logging.INFO)
    root_logger = logging.getLogger()
    root_logger.setLevel(resolved_level)

    if not root_logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        root_logger.addHandler(handler)
    else:
        for handler in root_logger.handlers:
            handler.setFormatter(JsonFormatter())

    return logging.getLogger("ingestion.generator")


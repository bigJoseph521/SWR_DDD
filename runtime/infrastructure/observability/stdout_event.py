"""Structured single-line stdout events for local CLI runs (colorlog)."""

from __future__ import annotations

import json
import logging
import sys
import threading
from datetime import datetime, timezone
from typing import Any

import colorlog

_STDOUT_LOCK = threading.Lock()
_LEVEL_MAP: dict[str, int] = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "WARN": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}

_event_logger: logging.Logger | None = None


def _event_logger_instance() -> logging.Logger:
    global _event_logger
    if _event_logger is not None:
        return _event_logger
    logger = colorlog.getLogger("strategy_worker_runtime.stdout_event")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    if not logger.handlers:
        handler = colorlog.StreamHandler(sys.stdout)
        handler.setFormatter(
            colorlog.ColoredFormatter(
                "%(log_color)s%(message)s%(reset)s",
                log_colors={
                    "DEBUG": "cyan",
                    "INFO": "green",
                    "WARNING": "yellow",
                    "ERROR": "red",
                    "CRITICAL": "red,bg_white",
                },
            )
        )
        logger.addHandler(handler)
    _event_logger = logger
    return logger


def write_stdout_event(
    *,
    level: str = "INFO",
    event_name: str,
    message: str,
    **extra: Any,
) -> None:
    """
    Print ``[<iso8601>] {"level":"...","event_name":"...","message":"...",...}`` via colorlog.

    Optional ``extra`` fields are merged at the top level (omit ``None`` values).
    """
    record: dict[str, Any] = {
        "level": str(level).upper(),
        "event_name": event_name,
        "message": message,
    }
    for key, value in extra.items():
        if value is not None:
            record[key] = value
    timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    line = (
        f"[{timestamp}] "
        f"{json.dumps(record, separators=(',', ':'), ensure_ascii=True, default=str)}"
    )
    log_level = _LEVEL_MAP.get(str(level).upper(), logging.INFO)
    with _STDOUT_LOCK:
        _event_logger_instance().log(log_level, line)
        print(file=sys.stdout, flush=True)

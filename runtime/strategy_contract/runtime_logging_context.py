from __future__ import annotations

import logging
from types import SimpleNamespace
from typing import Any

from alphovex_sdk.context.logging_context import LoggingContext


class RuntimeLoggingContext(LoggingContext):
    """Maps :class:`LoggingContext` calls to stdlib logging and optional ``LoggerBackend``."""

    __slots__ = ("_backend", "_logger")

    def __init__(
        self, *, logger_backend: object, name: str = "strategy_worker_runtime.strategy"
    ) -> None:
        self._backend = logger_backend
        self._logger = logging.getLogger(name)

    def _emit_backend(self, level: str, message: str, kwargs: dict[str, Any]) -> None:
        emit = getattr(self._backend, "emit_log_record", None)
        if not callable(emit):
            return
        emit(SimpleNamespace(level=level, message=message, context=kwargs))

    def debug(self, message: str, **kwargs: Any) -> None:
        self._logger.debug("%s | %s", message, kwargs)
        self._emit_backend("DEBUG", message, kwargs)

    def info(self, message: str, **kwargs: Any) -> None:
        self._logger.info("%s | %s", message, kwargs)
        self._emit_backend("INFO", message, kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        self._logger.warning("%s | %s", message, kwargs)
        self._emit_backend("WARNING", message, kwargs)

    def error(self, message: str, **kwargs: Any) -> None:
        self._logger.error("%s | %s", message, kwargs)
        self._emit_backend("ERROR", message, kwargs)

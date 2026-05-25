from __future__ import annotations

from datetime import datetime
from typing import Protocol


class ManagerStatusPort(Protocol):
    """SRM runtime status reporting (HTTP ``/internal/v1/runtimes/{id}/status``)."""

    def emit_runtime_status(
        self,
        *,
        local_state: str,
        observed_at: datetime,
        source: str,
        reason_code: str | None = None,
        message: str | None = None,
        retryable: bool | None = None,
    ) -> None: ...

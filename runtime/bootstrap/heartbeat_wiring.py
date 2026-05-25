from __future__ import annotations

from datetime import datetime

from runtime.application.ports.manager_status_port import ManagerStatusPort


class ManagerGatewayStatusAdapter:
    """Adapts manager gateway heartbeat API to :class:`ManagerStatusPort`."""

    def __init__(self, gateway: object) -> None:
        self._gateway = gateway

    def emit_runtime_status(
        self,
        *,
        local_state: str,
        observed_at: datetime,
        source: str,
        reason_code: str | None = None,
        message: str | None = None,
        retryable: bool | None = None,
    ) -> None:
        emit = getattr(self._gateway, "emit_heartbeat", None)
        if not callable(emit):
            raise TypeError("manager gateway missing emit_heartbeat")
        emit(
            local_state=local_state,
            observed_at=observed_at,
            source=source,
            reason_code=reason_code,
            message=message,
            retryable=retryable,
        )

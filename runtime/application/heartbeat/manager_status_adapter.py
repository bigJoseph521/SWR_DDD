from __future__ import annotations

from datetime import datetime

from runtime.infrastructure.http.srm.manager_gateway import ManagerGateway


class ManagerGatewayStatusAdapter:
    """Adapts :class:`ManagerGateway` to :class:`ManagerStatusPort`."""

    def __init__(self, gateway: ManagerGateway) -> None:
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
        self._gateway.emit_heartbeat(
            local_state=local_state,
            observed_at=observed_at,
            source=source,
            reason_code=reason_code,
            message=message,
            retryable=retryable,
        )

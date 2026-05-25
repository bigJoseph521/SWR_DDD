from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Callable

from runtime.application.ports.manager_status_port import ManagerStatusPort
from runtime.application.runtime_state.runtime_state import RuntimeState
from runtime.domain.enums import WorkerPhase
from runtime.application.ports.srm_status_sources import SRM_STATUS_SOURCE_HEARTBEAT


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class HeartbeatService:
    """Application-level heartbeat orchestration (SRM status payload built in infrastructure)."""

    def __init__(
        self,
        *,
        runtime_state: RuntimeState,
        manager_status: ManagerStatusPort,
        on_domain_heartbeat: Callable[[], None] | None = None,
        shutdown_in_progress: Callable[[], bool] | None = None,
    ) -> None:
        self._runtime_state = runtime_state
        self._manager_status = manager_status
        self._on_domain_heartbeat = on_domain_heartbeat
        self._shutdown_in_progress = shutdown_in_progress or (lambda: False)

    def emit_periodic_heartbeat(self) -> None:
        if self._shutdown_in_progress():
            return
        if self._runtime_state.phase not in (WorkerPhase.READY, WorkerPhase.RUNNING):
            return
        if self._on_domain_heartbeat is not None:
            self._on_domain_heartbeat()
        self.emit_runtime_status(source=SRM_STATUS_SOURCE_HEARTBEAT)

    def emit_runtime_status(
        self,
        *,
        observed_at: datetime | None = None,
        source: str = SRM_STATUS_SOURCE_HEARTBEAT,
        reason_code: str | None = None,
        message: str | None = None,
        retryable: bool | None = None,
    ) -> None:
        if self._shutdown_in_progress():
            return
        when = observed_at if observed_at is not None else _utc_now()
        self._manager_status.emit_runtime_status(
            local_state=self._runtime_state.phase.value,
            observed_at=when,
            source=str(source),
            reason_code=reason_code,
            message=message,
            retryable=retryable,
        )

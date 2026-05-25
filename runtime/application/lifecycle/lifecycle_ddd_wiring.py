from __future__ import annotations

from typing import Any

from runtime.application.event_handling.event_dispatcher import EventDispatcher
from runtime.application.heartbeat.heartbeat_service import HeartbeatService
from runtime.application.runtime_state.runtime_state import RuntimeState
from runtime.application.strategy_execution.strategy_execution_service import (
    StrategyExecutionService,
)


class LifecycleDddWiring:
    """Bundles DDD application services wired into :class:`LifecycleService`."""

    def __init__(
        self,
        *,
        runtime_state: RuntimeState,
        specs: Any | None,
        heartbeat_service: HeartbeatService | None,
        strategy_execution: StrategyExecutionService | None,
        event_dispatcher: EventDispatcher | None,
    ) -> None:
        self.runtime_state = runtime_state
        self.specs = specs
        self.heartbeat_service = heartbeat_service
        self.strategy_execution = strategy_execution
        self.event_dispatcher = event_dispatcher

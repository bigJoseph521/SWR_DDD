from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Callable, Mapping

from runtime.bootstrap.runtime_spec_builder import (
    BuiltRuntimeSpecs,
    build_runtime_specs_from_settings,
)
from runtime.application.event_handling.event_dispatcher import EventDispatcher
from runtime.application.event_handling.market_event_handler import MarketEventHandler
from runtime.application.event_handling.portfolio_update_handler import (
    PortfolioUpdateHandler,
)
from runtime.application.event_handling.sdk_account_context_updater import (
    SdkAccountContextUpdater,
)
from runtime.application.event_handling.runtime_event_handler import RuntimeEventHandler
from runtime.application.heartbeat.heartbeat_service import HeartbeatService
from runtime.application.heartbeat.manager_status_adapter import (
    ManagerGatewayStatusAdapter,
)
from runtime.application.runtime_state.runtime_state import RuntimeState
from runtime.application.strategy_execution.strategy_execution_service import (
    StrategyExecutionService,
)
from runtime.application.strategy_execution.strategy_adapter import StrategyAdapter
from runtime.application.strategy_execution.strategy_error_boundary import StrategyCallResult
from runtime.infrastructure.config.settings import Settings
from runtime.domain.enums import WorkerMode, WorkerPhase
from runtime.infrastructure.clock.epoch_time import utc_from_epoch_millis
from runtime.infrastructure.observability.domain_events import StrategyWorkerDomainEvent


def _parse_tick_timestamp(tick: Mapping[str, Any]) -> datetime | None:
    et = tick.get("event_time")
    if isinstance(et, datetime):
        return et if et.tzinfo is not None else et.replace(tzinfo=timezone.utc)
    raw_ms = tick.get("ts_ms")
    if raw_ms is not None:
        try:
            return utc_from_epoch_millis(int(raw_ms))
        except (TypeError, ValueError, OverflowError):
            return None
    return None


class LifecycleDddWiring:
    """Bundles DDD application services wired into :class:`LifecycleService`."""

    def __init__(
        self,
        *,
        runtime_state: RuntimeState,
        specs: BuiltRuntimeSpecs | None,
        heartbeat_service: HeartbeatService | None,
        strategy_execution: StrategyExecutionService | None,
        event_dispatcher: EventDispatcher | None,
    ) -> None:
        self.runtime_state = runtime_state
        self.specs = specs
        self.heartbeat_service = heartbeat_service
        self.strategy_execution = strategy_execution
        self.event_dispatcher = event_dispatcher


def build_lifecycle_ddd_wiring(
    *,
    launch_mode: WorkerMode,
    worker_runtime_settings: Settings | Any | None,
    strategy_adapter: StrategyAdapter,
    runtime_state: RuntimeState,
    manager_gateway: object,
    shutdown_in_progress: Callable[[], bool],
    emit_domain_event: Callable[..., None],
    domain_event_sampler: object,
    mode: WorkerMode,
) -> LifecycleDddWiring:
    specs: BuiltRuntimeSpecs | None = None
    if isinstance(worker_runtime_settings, Settings):
        specs = build_runtime_specs_from_settings(worker_runtime_settings)

    calculation = specs.calculation if specs is not None else None
    strategy_execution = StrategyExecutionService(
        adapter=strategy_adapter,
        calculation_spec=calculation,
    )

    def on_signal_generated(fields: Mapping[str, str]) -> None:
        count, sample = domain_event_sampler.next("signal_generated")  # type: ignore[attr-defined]
        emit_domain_event(
            StrategyWorkerDomainEvent.SIGNAL_GENERATED,
            "Strategy processed a market data event",
            level=logging.INFO if sample else logging.DEBUG,
            event_extras={
                **dict(fields),
                "signal_count": count,
                "sampled": sample,
            },
        )

    def on_execution_failed(result: StrategyCallResult[Any]) -> None:
        emit_domain_event(
            StrategyWorkerDomainEvent.FAILED,
            "Strategy adapter rejected or failed processing a market data tick.",
            level=logging.WARNING,
            event_extras={
                "error_code": result.error_code,
                "reason_code": result.reason_code,
                "diagnostics": dict(result.diagnostics or {}),
            },
        )

    market_handler = MarketEventHandler(
        strategy_execution=strategy_execution,
        runtime_state=runtime_state,
        mode=mode,
        on_signal_generated=on_signal_generated,
        on_execution_failed=on_execution_failed,
        extract_timestamp=_parse_tick_timestamp,
    )
    portfolio_handler: PortfolioUpdateHandler | None = None
    bridge = getattr(strategy_adapter, "_replay_sdk_bridge", None)
    if bridge is not None:
        account = getattr(getattr(bridge, "strategy_context", None), "account", None)
        if account is not None:
            portfolio_handler = PortfolioUpdateHandler(
                context_updater=SdkAccountContextUpdater(account),
            )

    runtime_handler = RuntimeEventHandler(
        market_handler=market_handler,
        portfolio_handler=portfolio_handler,
    )
    event_dispatcher = EventDispatcher(
        runtime_handler=runtime_handler,
        strategy_execution=strategy_execution,
    )

    heartbeat_service: HeartbeatService | None = None
    if hasattr(manager_gateway, "emit_heartbeat"):
        adapter = ManagerGatewayStatusAdapter(manager_gateway)  # type: ignore[arg-type]

        def on_domain_heartbeat() -> None:
            count, sample = domain_event_sampler.next("heartbeat_sent")  # type: ignore[attr-defined]
            emit_domain_event(
                StrategyWorkerDomainEvent.HEARTBEAT_SENT,
                "Worker heartbeat sent to strategy-runtime-manager",
                level=logging.INFO if sample else logging.DEBUG,
                event_extras={
                    "phase": runtime_state.phase.value,
                    "heartbeat_count": count,
                    "sampled": sample,
                },
            )

        heartbeat_service = HeartbeatService(
            runtime_state=runtime_state,
            manager_status=adapter,
            on_domain_heartbeat=on_domain_heartbeat,
            shutdown_in_progress=shutdown_in_progress,
        )

    return LifecycleDddWiring(
        runtime_state=runtime_state,
        specs=specs,
        heartbeat_service=heartbeat_service,
        strategy_execution=strategy_execution,
        event_dispatcher=event_dispatcher,
    )

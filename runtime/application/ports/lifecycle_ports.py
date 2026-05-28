from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Mapping, Protocol

from runtime.application.ports.launch_context import LaunchContext
from runtime.application.ports.backtest_sdk_bridge_port import BacktestSdkBridgePort
from runtime.application.ports.worker_domain_events import StrategyWorkerDomainEvent
from runtime.domain.enums import WorkerMode
from runtime.domain.model.platform_trace_spec import PlatformTraceSpec
from runtime.domain.worker_identity import WorkerIdentity


class RuntimeLoggerPort(Protocol):
    def emit(
        self,
        *,
        event_name: str,
        message: str,
        level: int = ...,
        event_extras: Mapping[str, Any] | None = None,
    ) -> None: ...


class StateJournalPort(Protocol):
    def record_phase_change(
        self,
        *,
        previous_phase: object | None,
        phase: object,
        level: str,
        reason_code: str | None,
    ) -> None: ...

    def record_first_data(self, *, observed_at: datetime) -> None: ...

    def record_manager_stop_request(
        self,
        *,
        last_data_event_at: datetime | None,
        last_clock_at: datetime | None,
        last_replay_cursor: str,
        stop_requested_at: datetime,
    ) -> None: ...

    def record_launch_failed_event(
        self,
        *,
        occurred_at: datetime,
        reason_code: str,
        details: Mapping[str, Any],
    ) -> None: ...

    def record_order_intent(
        self, source: str, payload: Mapping[str, Any], result: Mapping[str, Any]
    ) -> None: ...

    def record_startup_step(self, step: str) -> None: ...

    def record_shutdown_step(self, step: str) -> None: ...

    def allocate_order_intent_id(self) -> str: ...


class LaunchMetadataValidatorPort(Protocol):
    def validate(self, payload: Mapping[str, object]) -> LaunchContext: ...


class BootstrapEntrypointView(Protocol):
    entrypoint_spec: str
    symbol: object


class BootstrapSuccessView(Protocol):
    entrypoint: BootstrapEntrypointView


class BootstrapPipelineResultView(Protocol):
    success: bool
    success_payload: BootstrapSuccessView | None
    failure: object | None


class BootstrapPipelinePort(Protocol):
    def run(self, launch: LaunchContext) -> BootstrapPipelineResultView: ...


class StrategyAssignmentKeyView(Protocol):
    runtime_id: str
    strategy_version_id: str
    tenant_id: str
    mode: WorkerMode
    launch_attempt: int
    trader_id: str | None
    account_id: str | None


class StrategyInstanceCoordinatorPort(Protocol):
    def create(
        self, key: StrategyAssignmentKeyView, factory: Callable[[], object]
    ) -> object: ...

    def stop(self, adapter: object) -> object: ...


class WorkerRuntimeConfigPort(Protocol):
    """Duck-typed runtime tuning (bundle + env) without infrastructure Settings."""

    strategy_runtime_manager_base_url: str
    deployment_id: str
    runtime_manager_heartbeat_timeout_seconds: float
    replay_bar_timeframe: str
    market_data_redis_url: str
    market_data_feeds: tuple[str, ...]
    market_data_realtime_partition_count: int
    market_data_redis_use_consumer_group: bool
    market_data_consumer_group_prefix: str
    market_data_consumer_name_prefix: str
    market_data_stream_start_id: str
    market_data_xread_block_ms: int
    market_data_xread_count: int
    portfolio_update_enabled: bool
    portfolio_update_redis_url: str
    portfolio_update_channel_prefix: str
    order_intent_correlation_id: str
    disable_order_intent_grpc: bool


class DomainEventEmitterPort(Protocol):
    def next_sample(self, key: str) -> tuple[int, bool]: ...

    def emit_bound(
        self,
        logger: RuntimeLoggerPort,
        *,
        event: StrategyWorkerDomainEvent | str,
        message: str,
        level: int = ...,
        event_extras: Mapping[str, Any] | None = None,
        platform_trace: PlatformTraceSpec | None = None,
        env_correlation_fallback: str = "",
    ) -> None: ...


class SrmLifecycleReporterPort(Protocol):
    def report_bootstrap_success(
        self,
        *,
        runtime_id: str,
        mode: WorkerMode,
        owner_resource_id: str,
        srm_base_url: str,
        timeout_seconds: float,
    ) -> None: ...

    def report_bootstrap_failure(self, error: object, **kwargs: object) -> None: ...

    def initiate_stop_status_update(self, **kwargs: object) -> Mapping[str, object]: ...

    def resolve_shutdown_report(
        self, *, canonical_reason: str, message: str | None
    ) -> tuple[str, str]: ...

    def is_failure_shutdown(self, canonical_reason: str) -> bool: ...

    def report_final_shutdown(self, **kwargs: object) -> None: ...


class SdkBridgeFactoryPort(Protocol):
    def build(
        self,
        *,
        strategy_instance: object,
        simulated_clock: object,
        launch: LaunchContext,
        launch_payload: Mapping[str, object],
        worker_identity: WorkerIdentity,
        runtime_dependencies: object,
        worker_config: WorkerRuntimeConfigPort | None,
        on_order_intent_result: Callable[[str, dict[str, Any], dict[str, Any]], None]
        | None,
        latest_market_event_at: Callable[[], datetime | None] | None,
        allocate_order_intent_id: Callable[[], str] | None,
        platform_trace: PlatformTraceSpec | None,
    ) -> tuple[
        BacktestSdkBridgePort | None, Callable[[Any], dict[str, Any]] | None
    ]: ...

    def platform_trace(
        self, worker_config: WorkerRuntimeConfigPort | None
    ) -> PlatformTraceSpec | None: ...

    def calculation_bar_timeframe(
        self, worker_config: WorkerRuntimeConfigPort | None
    ) -> str | None: ...


class BacktestBarTimeframeFilterPort(Protocol):
    def should_skip(
        self, tick: Mapping[str, Any], *, expected_bar_timeframe: str
    ) -> bool: ...


# Backward-compat alias.
ReplayTickFilterPort = BacktestBarTimeframeFilterPort


class SimulatedClockFactoryPort(Protocol):
    def build_seeded(self, wall_clock: object | None) -> object: ...

    def is_simulated(self, clock: object) -> bool: ...


@dataclass(frozen=True, slots=True)
class LifecycleHostPorts:
    """Concrete lifecycle side-effects wired in bootstrap (feeds, SRM, SDK bridge)."""

    backtest_bar_timeframe_filter: BacktestBarTimeframeFilterPort
    srm: SrmLifecycleReporterPort
    domain_events: DomainEventEmitterPort
    sdk_bridge: SdkBridgeFactoryPort
    simulated_clock: SimulatedClockFactoryPort
    start_market_data_feed: Callable[[object], None]
    stop_market_data_feed: Callable[[], None]
    start_portfolio_update_feed: Callable[[object], None]
    stop_portfolio_update_feed: Callable[[], None]
    classify_bootstrap_stages: Callable[
        [Exception, list[str]], tuple[list[str], list[str], list[str]]
    ]
    srm_status_source_heartbeat: str
    srm_status_source_update: str

from __future__ import annotations

import logging
from typing import Any, Callable, Mapping

from runtime.application.ports.launch_context import LaunchContext
from runtime.application.ports.backtest_sdk_bridge_port import BacktestSdkBridgePort
from runtime.application.ports.worker_domain_events import StrategyWorkerDomainEvent
from runtime.infrastructure.backtest.backtest_bar_timeframe_filter import (
    should_skip_backtest_historical_market_event,
)
from runtime.bootstrap.runtime_spec_builder import (
    build_platform_trace_from_settings,
    build_runtime_specs_from_settings,
)
from runtime.bootstrap.sdk_order_intent_wiring import build_sdk_order_intent_submitter
from runtime.bootstrap.srm_env_status_report import (
    initiate_stop_status_update,
    is_failure_shutdown,
    print_bootstrap_failure_outcome,
    print_bootstrap_success_outcome,
    print_shutdown_status_outcome,
    report_bootstrap_failure_to_srm,
    report_bootstrap_success_to_srm,
    report_final_shutdown_to_srm,
    resolve_shutdown_report,
)
from runtime.domain.enums import WorkerMode
from runtime.domain.model.platform_trace_spec import PlatformTraceSpec
from runtime.domain.worker_identity import WorkerIdentity
from runtime.infrastructure.clock.clock import SimulatedClock
from runtime.infrastructure.config.settings import Settings
from runtime.infrastructure.observability.domain_events import (
    DomainEventSampler,
    emit_bound_domain_event,
)
from runtime.infrastructure.sdk.runtime_sdk_bridge import build_runtime_sdk_bridge


class BacktestBarTimeframeFilterAdapter:
    def should_skip(
        self, tick: Mapping[str, Any], *, expected_bar_timeframe: str
    ) -> bool:
        return should_skip_backtest_historical_market_event(
            tick, expected_bar_timeframe=expected_bar_timeframe
        )


# Backward-compat alias.
ReplayTickFilterAdapter = BacktestBarTimeframeFilterAdapter


class DomainEventEmitterAdapter:
    def __init__(self, *, sample_every: int = 100) -> None:
        self._sampler = DomainEventSampler(sample_every=sample_every)

    def next(self, key: str) -> tuple[int, bool]:
        return self._sampler.next(key)

    def next_sample(self, key: str) -> tuple[int, bool]:
        return self.next(key)

    def emit_bound(
        self,
        logger: object,
        *,
        event: StrategyWorkerDomainEvent | str,
        message: str,
        level: int = logging.INFO,
        event_extras: Mapping[str, Any] | None = None,
        platform_trace: PlatformTraceSpec | None = None,
        env_correlation_fallback: str = "",
    ) -> None:
        merged_extras: dict[str, Any] = dict(event_extras or {})
        if platform_trace is not None:
            merged_extras = platform_trace.merge_event_extras(
                merged_extras,
                env_correlation_fallback=env_correlation_fallback,
            )
        emit = getattr(logger, "emit", None)
        if not callable(emit):
            emit_bound_domain_event(
                logger,  # type: ignore[arg-type]
                event_name=event,
                message=message,
                level=level,
                event_extras=merged_extras,
            )
            return
        try:
            emit_bound_domain_event(
                logger,  # type: ignore[arg-type]
                event_name=event,
                message=message,
                level=level,
                event_extras=merged_extras,
            )
        except Exception:
            return


class SrmLifecycleReporterAdapter:
    def report_bootstrap_success(
        self,
        *,
        runtime_id: str,
        mode: WorkerMode,
        owner_resource_id: str,
        srm_base_url: str,
        timeout_seconds: float,
    ) -> None:
        try:
            srm_response = report_bootstrap_success_to_srm(
                runtime_id=runtime_id,
                mode=mode.value,
                owner_resource_id=owner_resource_id,
                srm_base_url=srm_base_url,
                timeout_seconds=timeout_seconds,
            )
            print_bootstrap_success_outcome(srm_response=srm_response)
        except Exception:
            pass

    def report_bootstrap_failure(self, error: object, **kwargs: object) -> None:
        try:
            srm_response = report_bootstrap_failure_to_srm(error, **kwargs)  # type: ignore[arg-type]
            print_bootstrap_failure_outcome(error, srm_response=srm_response)  # type: ignore[arg-type]
        except Exception:
            pass

    def initiate_stop_status_update(self, **kwargs: object) -> Mapping[str, object]:
        body, _srm_response, canonical, resolved_message = initiate_stop_status_update(
            **kwargs  # type: ignore[arg-type]
        )
        return {
            "body": body,
            "canonical": canonical,
            "resolved_message": resolved_message,
        }

    def resolve_shutdown_report(
        self, *, canonical_reason: str, message: str | None
    ) -> tuple[str, str]:
        return resolve_shutdown_report(
            canonical_reason=canonical_reason, message=message
        )

    def is_failure_shutdown(self, canonical_reason: str) -> bool:
        return is_failure_shutdown(canonical_reason)

    def report_final_shutdown(self, **kwargs: object) -> None:
        canonical_reason = str(kwargs.get("canonical_reason", ""))
        message = kwargs.get("message")
        reason_code, resolved_message = resolve_shutdown_report(
            canonical_reason=canonical_reason,
            message=str(message) if message is not None else None,
        )
        runtime_status = (
            "FAILED" if is_failure_shutdown(canonical_reason) else "STOPPED"
        )
        try:
            srm_response = report_final_shutdown_to_srm(**kwargs)  # type: ignore[arg-type]
            print_shutdown_status_outcome(
                runtime_status=runtime_status,
                reason_code=reason_code,
                message=resolved_message,
                srm_response=srm_response,
            )
        except Exception:
            pass


class SdkBridgeFactoryAdapter:
    def build(
        self,
        *,
        strategy_instance: object,
        simulated_clock: object,
        launch: LaunchContext,
        launch_payload: Mapping[str, object],
        worker_identity: WorkerIdentity,
        runtime_dependencies: object,
        worker_config: object | None,
        on_order_intent_result: Callable[[str, dict[str, Any], dict[str, Any]], None]
        | None,
        latest_market_event_at: Callable[[], object] | None,
        allocate_order_intent_id: Callable[[], str] | None,
        platform_trace: PlatformTraceSpec | None,
    ) -> tuple[BacktestSdkBridgePort | None, Callable[[Any], dict[str, Any]] | None]:
        wrs = worker_config if isinstance(worker_config, Settings) else None
        submitter = build_sdk_order_intent_submitter(
            dependencies=runtime_dependencies,
            launch_spec=launch,  # type: ignore[arg-type]
            worker_identity=worker_identity,
            on_order_intent_result=on_order_intent_result,
            order_intent_correlation_id=(
                str(getattr(wrs, "order_intent_correlation_id", "") or "")
                if wrs is not None
                else ""
            ),
            disable_order_intent_grpc=(
                bool(getattr(wrs, "disable_order_intent_grpc", False))
                if wrs is not None
                else False
            ),
            latest_market_event_at=latest_market_event_at,  # type: ignore[arg-type]
            allocate_order_intent_id=allocate_order_intent_id,
            platform_trace=platform_trace,
        )
        calculation_spec = None
        default_tf = "1m"
        if isinstance(wrs, Settings):
            calculation_spec = build_runtime_specs_from_settings(wrs).calculation
            default_tf = calculation_spec.bar_timeframe
        elif wrs is not None:
            default_tf = str(getattr(wrs, "replay_bar_timeframe", None) or "1m")
        bridge = build_runtime_sdk_bridge(
            strategy=strategy_instance,
            launch_spec=launch,  # type: ignore[arg-type]
            worker_identity=worker_identity,
            simulated_clock=simulated_clock,  # type: ignore[arg-type]
            launch_payload=launch_payload,
            submit_sdk_order_intent=submitter,
            default_timeframe=default_tf,
            calculation_spec=calculation_spec,
        )
        return bridge, submitter

    def platform_trace(self, worker_config: object | None) -> PlatformTraceSpec | None:
        if isinstance(worker_config, Settings):
            return build_platform_trace_from_settings(worker_config)
        return None

    def calculation_bar_timeframe(self, worker_config: object | None) -> str | None:
        if isinstance(worker_config, Settings):
            return build_runtime_specs_from_settings(
                worker_config
            ).calculation.bar_timeframe
        return None


class SimulatedClockFactoryAdapter:
    def build_seeded(self, wall_clock: object | None) -> object:
        from datetime import datetime, timezone

        clock = SimulatedClock()
        wall: datetime | None = None
        if wall_clock is not None and hasattr(wall_clock, "astimezone"):
            wall = wall_clock  # type: ignore[assignment]
        elif wall_clock is not None:
            now_fn = getattr(wall_clock, "now", None)
            if callable(now_fn):
                try:
                    wall = now_fn()
                except Exception:
                    wall = None
        if wall is None:
            wall = datetime.now(timezone.utc)
        if wall.tzinfo is None or wall.utcoffset() is None:
            wall = wall.replace(tzinfo=timezone.utc)
        clock.set_time(wall.astimezone(timezone.utc))
        return clock

    def is_simulated(self, clock: object) -> bool:
        return isinstance(clock, SimulatedClock)

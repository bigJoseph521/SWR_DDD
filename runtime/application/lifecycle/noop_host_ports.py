from __future__ import annotations

from typing import Any

from runtime.application.ports.lifecycle_ports import (
    DomainEventEmitterPort,
    LifecycleHostPorts,
    BacktestBarTimeframeFilterPort,
    SdkBridgeFactoryPort,
    SimulatedClockFactoryPort,
    SrmLifecycleReporterPort,
)
from runtime.application.lifecycle.bootstrap_stages import (
    BOOTSTRAP_STAGE_ORDER,
    classify_bootstrap_stages,
)


class _NoopBacktestBarTimeframeFilter:
    def should_skip(
        self, tick: object, *, expected_bar_timeframe: str
    ) -> bool:
        return False


class _NoopSrm:
    def report_bootstrap_success(self, **kwargs: object) -> None:
        pass

    def report_bootstrap_failure(self, error: object, **kwargs: object) -> None:
        pass

    def initiate_stop_status_update(self, **kwargs: object) -> dict[str, object]:
        return {"status": "STOPPING"}

    def resolve_shutdown_report(
        self, *, canonical_reason: str, message: str | None
    ) -> tuple[str, str]:
        return canonical_reason, message or ""

    def is_failure_shutdown(self, canonical_reason: str) -> bool:
        return False

    def report_final_shutdown(self, **kwargs: object) -> None:
        pass


class _NoopDomainEvents:
    def next(self, key: str) -> tuple[int, bool]:
        return (0, False)

    def next_sample(self, key: str) -> tuple[int, bool]:
        return self.next(key)

    def emit_bound(self, logger: object, **kwargs: object) -> None:
        pass


class _NoopSdkBridge:
    def build(self, **kwargs: object) -> tuple[None, None]:
        return None, None

    def platform_trace(self, worker_config: object | None) -> None:
        return None

    def calculation_bar_timeframe(self, worker_config: object | None) -> str | None:
        return None


class _NoopSimulatedClock:
    def build_seeded(self, wall_clock: object | None) -> object:
        return wall_clock

    def is_simulated(self, clock: object) -> bool:
        return hasattr(clock, "set_time")


def noop_lifecycle_host_ports() -> LifecycleHostPorts:
    return LifecycleHostPorts(
        backtest_bar_timeframe_filter=_NoopBacktestBarTimeframeFilter(),
        srm=_NoopSrm(),
        domain_events=_NoopDomainEvents(),
        sdk_bridge=_NoopSdkBridge(),
        simulated_clock=_NoopSimulatedClock(),
        start_market_data_feed=lambda: None,
        stop_market_data_feed=lambda: None,
        start_portfolio_update_feed=lambda: None,
        stop_portfolio_update_feed=lambda: None,
        classify_bootstrap_stages=classify_bootstrap_stages,
        srm_status_source_heartbeat="heartbeat",
        srm_status_source_update="update",
    )

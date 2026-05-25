from __future__ import annotations

from runtime.application.ports.lifecycle_ports import LifecycleHostPorts
from runtime.bootstrap.lifecycle_feed_wiring import (
    maybe_start_market_data_feed,
    maybe_start_portfolio_update_feed,
    stop_market_data_feed,
    stop_portfolio_update_feed,
)
from runtime.application.lifecycle.bootstrap_stages import classify_bootstrap_stages
from runtime.bootstrap.lifecycle_host_adapters import (
    DomainEventEmitterAdapter,
    BacktestBarTimeframeFilterAdapter,
    SdkBridgeFactoryAdapter,
    SimulatedClockFactoryAdapter,
    SrmLifecycleReporterAdapter,
)
from runtime.infrastructure.http.srm.heartbeat import (
    SRM_STATUS_SOURCE_HEARTBEAT,
    SRM_STATUS_SOURCE_UPDATE,
)


def build_lifecycle_host_ports(lifecycle: object) -> LifecycleHostPorts:
    """Wire concrete bootstrap/infrastructure adapters for :class:`LifecycleService`."""

    return LifecycleHostPorts(
        backtest_bar_timeframe_filter=BacktestBarTimeframeFilterAdapter(),
        srm=SrmLifecycleReporterAdapter(),
        domain_events=DomainEventEmitterAdapter(),
        sdk_bridge=SdkBridgeFactoryAdapter(),
        simulated_clock=SimulatedClockFactoryAdapter(),
        start_market_data_feed=lambda: maybe_start_market_data_feed(lifecycle),
        stop_market_data_feed=lambda: stop_market_data_feed(lifecycle),
        start_portfolio_update_feed=lambda: maybe_start_portfolio_update_feed(
            lifecycle
        ),
        stop_portfolio_update_feed=lambda: stop_portfolio_update_feed(lifecycle),
        classify_bootstrap_stages=classify_bootstrap_stages,
        srm_status_source_heartbeat=SRM_STATUS_SOURCE_HEARTBEAT,
        srm_status_source_update=SRM_STATUS_SOURCE_UPDATE,
    )


__all__ = ["build_lifecycle_host_ports"]

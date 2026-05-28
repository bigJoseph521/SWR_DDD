"""Infrastructure implementations of backtest-runner application ports."""

from __future__ import annotations

from typing import Any, Mapping

from runtime.domain.launch_spec import LaunchSpec
from runtime.infrastructure.backtest_runner import (
    market_data_wire,
    order_intent_wire,
    portfolio_wire,
)
from runtime.infrastructure.backtest_runner.order_intent_collector import (
    StdioOrderIntentCollector,
)
from runtime.infrastructure.backtest_runner.portfolio_sync import (
    apply_portfolio_snapshot_to_bridge,
)


class MarketDataWireAdapter:
    def normalize_event(self, event: Mapping[str, Any]) -> dict[str, Any]:
        return market_data_wire.normalize_stdio_market_data_event(event)


class OrderIntentWireAdapter:
    def drain_to_wire_intents(
        self, collector: object, *, launch_spec: LaunchSpec
    ) -> list[dict[str, Any]]:
        if not isinstance(collector, StdioOrderIntentCollector):
            raise TypeError("expected StdioOrderIntentCollector")
        return order_intent_wire.drain_collector_to_wire_intents(
            collector, launch_spec=launch_spec
        )


class PortfolioWireAdapter:
    def validate_portfolio_wire(self, portfolio: Mapping[str, Any]) -> None:
        portfolio_wire.validate_portfolio_wire(portfolio)

    def validate_open_orders_wire(self, orders: list[Any]) -> None:
        portfolio_wire.validate_open_orders_wire(orders)

    def default_runner_portfolio_wire(self) -> dict[str, Any]:
        return portfolio_wire.default_runner_portfolio_wire()


class PortfolioSyncAdapter:
    def __init__(self, bridge: object) -> None:
        self._bridge = bridge

    def apply_portfolio_snapshot(
        self, portfolio_wire: Mapping[str, Any], *, launch_spec: LaunchSpec
    ) -> None:
        apply_portfolio_snapshot_to_bridge(
            self._bridge, portfolio_wire, launch_spec=launch_spec
        )

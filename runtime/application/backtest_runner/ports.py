"""Ports for backtest-runner subprocess wiring (implemented in infrastructure)."""

from __future__ import annotations

from typing import Any, Mapping, Protocol

from runtime.domain.launch_spec import LaunchSpec


class MarketDataWirePort(Protocol):
    def normalize_event(self, event: Mapping[str, Any]) -> dict[str, Any]: ...


class OrderIntentWirePort(Protocol):
    def drain_to_wire_intents(
        self, collector: object, *, launch_spec: LaunchSpec
    ) -> list[dict[str, Any]]: ...


class PortfolioSyncPort(Protocol):
    def apply_portfolio_snapshot(
        self, portfolio_wire: Mapping[str, Any], *, launch_spec: LaunchSpec
    ) -> None: ...


class PortfolioWirePort(Protocol):
    def validate_portfolio_wire(self, portfolio: Mapping[str, Any]) -> None: ...

    def validate_open_orders_wire(self, orders: list[Any]) -> None: ...

    def default_runner_portfolio_wire(self) -> dict[str, Any]: ...

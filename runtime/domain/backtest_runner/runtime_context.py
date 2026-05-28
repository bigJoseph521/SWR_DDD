"""In-memory stdio backtest session context (runner-injected snapshots)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class BacktestRuntimeContext:
    """
    Runner-owned replay state for one stdio subprocess.

    Portfolio snapshots are mirrored into the bound SDK account context.
    Open orders are stored here only (no public SDK surface in this milestone).
    """

    backtest_job_id: str | None = None
    strategy_id: str | None = None
    strategy_version_id: str | None = None
    latest_portfolio_snapshot: dict[str, Any] = field(default_factory=dict)
    latest_open_orders_snapshot: list[Any] = field(default_factory=list)
    portfolio_snapshot_received: bool = False
    open_orders_snapshot_received: bool = False

    def replace_portfolio_snapshot(self, portfolio: dict[str, Any]) -> None:
        self.latest_portfolio_snapshot = dict(portfolio)
        self.portfolio_snapshot_received = True

    def replace_open_orders_snapshot(self, orders: list[Any]) -> None:
        self.latest_open_orders_snapshot = list(orders)
        self.open_orders_snapshot_received = True

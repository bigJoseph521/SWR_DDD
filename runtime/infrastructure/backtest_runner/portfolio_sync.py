"""Apply stdio portfolio snapshots to the bound SDK account context."""

from __future__ import annotations

from typing import Any, Mapping

from runtime.application.ports.backtest_sdk_bridge_port import BacktestSdkBridgePort
from runtime.domain.launch_spec import LaunchSpec
from runtime.infrastructure.backtest_runner.portfolio_wire import (
    portfolio_wire_to_snapshot,
)
from runtime.infrastructure.sdk.runtime_account_context import RuntimeAccountContext


def apply_portfolio_snapshot_to_bridge(
    bridge: BacktestSdkBridgePort,
    portfolio_wire: Mapping[str, Any],
    *,
    launch_spec: LaunchSpec,
) -> None:
    account_id = launch_spec.account_id or launch_spec.trader_id or "stdio-backtest"
    snap = portfolio_wire_to_snapshot(
        portfolio_wire,
        account_id=account_id,
        run_id=launch_spec.runtime_id,
    )
    account = bridge.strategy_context.account
    if not isinstance(account, RuntimeAccountContext):
        raise RuntimeError("strategy account context is not RuntimeAccountContext")
    portfolio_svc = account._portfolio
    replace = getattr(portfolio_svc, "replace_snapshot", None)
    if not callable(replace):
        raise RuntimeError("portfolio service does not support replace_snapshot")
    replace(snap)

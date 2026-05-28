"""Loaded strategy state for one backtest-runner subprocess."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from runtime.application.ports.backtest_sdk_bridge_port import BacktestSdkBridgePort
from runtime.application.strategy_execution.strategy_adapter import StrategyAdapter
from runtime.domain.launch_spec import LaunchSpec


@dataclass(frozen=True, slots=True)
class LoadedSubprocessSession:
    launch_spec: LaunchSpec
    adapter: StrategyAdapter
    sdk_bridge: BacktestSdkBridgePort
    order_intent_collector: object
    work_root: Path

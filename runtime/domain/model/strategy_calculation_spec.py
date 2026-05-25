from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True, slots=True)
class StrategyCalculationSpec:
    """
    Strategy calculation input exposed to user strategy execution.

    Must not include platform metadata (strategy_id, strategy_version_id, runtime_id, etc.).
    """

    params: Mapping[str, Any]
    symbol: str | None
    bar_timeframe: str

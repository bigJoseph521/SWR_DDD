from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, Protocol


class MarketDataFeedPort(Protocol):
    """Mode-specific market-data feeder (stdin for BACKTEST, Redis for PAPER/LIVE)."""

    def start(self, on_tick: Callable[[Mapping[str, Any]], None]) -> None: ...

    def stop(self) -> None: ...

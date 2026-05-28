from __future__ import annotations

from typing import Protocol


class BacktestSdkBridgePort(Protocol):
    """
    In-memory SDK data bridge for BACKTEST (historical bars/quotes in strategy context).

    Not a separate strategy execution path: StrategyAdapter uses this only to invoke SDK
    hooks with the same wire events as other modes.
    """

    @property
    def strategy_context(self) -> object: ...

    def dispatch_on_bar(
        self,
        raw_event: object,
        mapped: object,
        on_bar: object,
    ) -> object: ...

    def dispatch_on_quote(
        self,
        raw_event: object,
        mapped: object,
        on_quote: object,
    ) -> object: ...

    def dispatch_on_timer(
        self,
        raw_event: object,
        mapped: object,
        on_timer: object,
    ) -> object: ...

    def dispatch_on_tick(
        self,
        raw_event: object,
        mapped: object,
        on_tick: object,
    ) -> object: ...

"""BACKTEST helpers (bar-timeframe filtering for stdin/Redis dispatch)."""

from runtime.infrastructure.backtest.backtest_bar_timeframe_filter import (
    should_skip_backtest_historical_market_event,
)

__all__ = ["should_skip_backtest_historical_market_event"]

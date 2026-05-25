from __future__ import annotations

from runtime.application.strategy_execution.strategy_adapter import StrategyAdapter


def test_on_event_ok_when_strategy_has_no_on_event_hook() -> None:
    class _NoHooks:
        pass

    adapter = StrategyAdapter(strategy=_NoHooks())
    result = adapter.on_event(
        {
            "type": "market.bar",
            "symbol": "AAPL",
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 100.0,
            "ts_ms": 1,
        }
    )
    assert result.ok is True
    assert result.diagnostics.get("skipped") == "no_on_event_hook"

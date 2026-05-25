from __future__ import annotations

from runtime.bootstrap.backtest_replay_tick_filter import (
    skip_backtest_replay_tick_for_ingest,
)
from runtime.bootstrap.strategy_bundle_loader import (
    effective_bar_timeframe,
    parse_market_data_feeds,
)
from runtime.integration.market_data_redis_feed import (
    STREAM_AM_1M,
    STREAM_TRADES,
    resolve_stream_names,
)


def test_effective_bar_timeframe_parameters_win() -> None:
    data = {
        "replay_bar_timeframe": "5m",
        "parameters": {"bar_timeframe": "1m"},
    }
    assert effective_bar_timeframe(data) == "1m"


def test_effective_bar_timeframe_root_bar_timeframe() -> None:
    data = {"bar_timeframe": "15m", "parameters": {}}
    assert effective_bar_timeframe(data) == "15m"


def test_effective_bar_timeframe_legacy_replay_key() -> None:
    data = {"replay_bar_timeframe": "5m"}
    assert effective_bar_timeframe(data) == "5m"


def test_parse_market_data_feeds_from_parameters_data_source() -> None:
    data = {
        "parameters": {"data_source": ["quotes", "trades"]},
    }
    assert parse_market_data_feeds(data) == ("quotes", "trades")


def test_parse_market_data_feeds_root_market_data_streams_overrides_parameters() -> (
    None
):
    data = {
        "market_data_streams": ["bars"],
        "parameters": {"data_source": ["trades"]},
    }
    assert parse_market_data_feeds(data) == ("bars",)


def test_skip_backtest_replay_tick_non_bar() -> None:
    assert skip_backtest_replay_tick_for_ingest(
        {"type": "market.tick"},
        expected_bar_timeframe="1m",
    )


def test_skip_backtest_replay_tick_wrong_timeframe() -> None:
    assert skip_backtest_replay_tick_for_ingest(
        {"type": "market.bar", "timeframe": "5m"},
        expected_bar_timeframe="1m",
    )


def test_skip_backtest_replay_tick_bar_allowed() -> None:
    assert not skip_backtest_replay_tick_for_ingest(
        {"type": "market.bar", "timeframe": "1min"},
        expected_bar_timeframe="1m",
    )


def test_skip_backtest_replay_tick_bar_no_timeframe_allowed() -> None:
    assert not skip_backtest_replay_tick_for_ingest(
        {"type": "market.bar", "symbol": "X"},
        expected_bar_timeframe="1m",
    )


def test_resolve_stream_names_drops_bars_when_timeframe_not_1m() -> None:
    assert resolve_stream_names(("bars", "trades"), bar_timeframe="5m") == [
        STREAM_TRADES,
    ]


def test_resolve_stream_names_keeps_bars_for_1m_alias() -> None:
    assert resolve_stream_names(("bars",), bar_timeframe="1min") == [STREAM_AM_1M]

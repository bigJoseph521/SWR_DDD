from __future__ import annotations

from types import SimpleNamespace

import pytest

from runtime.integration.market_data_redis_feed import (
    STREAM_AM_1M,
    STREAM_QUOTES,
    STREAM_TRADES,
    MarketDataRedisKeys,
    bar_message_to_tick,
    expand_market_data_stream_keys,
    market_data_redis_keys_from_settings,
    partitions_for_realtime_subscribe,
    quote_message_to_tick,
    resolve_stream_names,
    sanitize_redis_url_for_log,
    stream_payload_to_tick,
    trade_message_to_tick,
)


def test_sanitize_redis_url_for_log_redacts_password() -> None:
    assert "secret" not in sanitize_redis_url_for_log(
        "redis://user:secret@localhost:6379/0"
    )
    assert "user:***" in sanitize_redis_url_for_log(
        "redis://user:secret@localhost:6379/0"
    )
    assert "***" in sanitize_redis_url_for_log("redis://:onlypass@127.0.0.1:6380/1")


def test_resolve_stream_names_order_and_dedupe() -> None:
    assert resolve_stream_names(("bars", "trades", "bars"), bar_timeframe="1m") == [
        STREAM_AM_1M,
        STREAM_TRADES,
    ]


def test_bar_message_to_tick() -> None:
    tick = bar_message_to_tick(
        {
            "symbol": "AAPL",
            "instrument_id": 42,
            "time_utc": "2026-04-15T12:00:00Z",
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 1000.0,
        },
        stream_id="1-0",
        field="AAPL",
    )
    assert tick is not None
    assert tick["type"] == "market.bar"
    assert tick["symbol"] == "AAPL"
    assert tick["instrument_id"] == "AAPL"
    assert tick["ts_ms"] > 0
    assert tick["close"] == 1.5
    assert tick["timeframe"] == "1m"


def test_bar_message_to_tick_respects_bar_timeframe_label() -> None:
    tick = bar_message_to_tick(
        {
            "symbol": "AAPL",
            "instrument_id": 1,
            "time_utc": "2026-04-15T12:00:00Z",
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
            "volume": 1.0,
        },
        stream_id="1-0",
        field="AAPL",
        bar_timeframe="1m",
    )
    assert tick is not None
    assert tick["timeframe"] == "1m"


def test_trade_message_to_tick() -> None:
    tick = trade_message_to_tick(
        {
            "symbol": "AAPL",
            "instrument_id": 7,
            "time_utc": "2026-04-15T12:00:01Z",
            "price": 10.5,
            "size": 3.0,
        },
        stream_id="2-0",
        field="AAPL",
    )
    assert tick is not None
    assert tick["type"] == "market.tick"
    assert tick["instrument_id"] == "AAPL"
    assert tick["price"] == 10.5


def test_quote_message_to_tick() -> None:
    tick = quote_message_to_tick(
        {
            "symbol": "MSFT",
            "instrument_id": 8,
            "time_utc": "2026-04-15T12:00:02Z",
            "bid_price": 100.0,
            "ask_price": 100.5,
            "bid_size": 10.0,
            "ask_size": 20.0,
        },
        stream_id="3-0",
        field="MSFT",
    )
    assert tick is not None
    assert tick["type"] == "market.quote"
    assert tick["instrument_id"] == "MSFT"
    assert tick["bid"] == 100.0


def test_stream_payload_to_tick_respects_custom_redis_stream_keys() -> None:
    keys = MarketDataRedisKeys(stream_bars_1m="custom:bars:1m")
    bar_json = (
        '{"symbol":"X","instrument_id":1,"time_utc":"2026-01-01T00:00:00Z",'
        '"open":1,"high":1,"low":1,"close":1,"volume":1}'
    )
    t = stream_payload_to_tick(
        "custom:bars:1m",
        "X",
        bar_json,
        message_id="0-1",
        redis_keys=keys,
    )
    assert t is not None and t["type"] == "market.bar"


def test_market_data_redis_keys_from_settings_duck_type() -> None:
    class _S:
        market_data_redis_stream_trades = "s:t"
        market_data_redis_stream_quotes = ""
        market_data_redis_stream_bars_1m = ""

    k = market_data_redis_keys_from_settings(_S())
    assert k.stream_trades == "s:t"
    assert k.stream_quotes == "md:stream:quotes"


def test_expand_market_data_stream_keys_all_partitions() -> None:
    bases = resolve_stream_names(("bars",), bar_timeframe="1m")
    keys = expand_market_data_stream_keys(bases, partition_count=3, symbol_filter=None)
    assert keys == [f"{STREAM_AM_1M}:0", f"{STREAM_AM_1M}:1", f"{STREAM_AM_1M}:2"]


def test_expand_market_data_stream_keys_symbol_filter() -> None:
    bases = [STREAM_AM_1M, STREAM_TRADES]
    keys = expand_market_data_stream_keys(
        bases,
        partition_count=128,
        symbol_filter={"AAPL", "MSFT"},
    )
    pa = partitions_for_realtime_subscribe(
        partition_count=128, symbol_filter={"AAPL", "MSFT"}
    )
    assert len(pa) <= 2
    for base in bases:
        for p in pa:
            assert f"{base}:{p}" in keys
    assert len(keys) == 2 * len(pa)


def test_stream_payload_to_tick_partitioned_stream_name() -> None:
    bar_json = (
        '{"symbol":"AAPL","instrument_id":1,"time_utc":"2026-01-01T00:00:00Z",'
        '"open":1,"high":1,"low":1,"close":1,"volume":1}'
    )
    t = stream_payload_to_tick(f"{STREAM_AM_1M}:92", "AAPL", bar_json, message_id="0-1")
    assert t is not None and t["type"] == "market.bar"


def test_stream_payload_to_tick_dispatches_by_stream() -> None:
    bar_json = (
        '{"symbol":"X","instrument_id":1,"time_utc":"2026-01-01T00:00:00Z",'
        '"open":1,"high":1,"low":1,"close":1,"volume":1}'
    )
    t = stream_payload_to_tick(STREAM_AM_1M, "X", bar_json, message_id="0-1")
    assert t is not None and t["type"] == "market.bar"

    trade_json = (
        '{"symbol":"X","instrument_id":1,"time_utc":"2026-01-01T00:00:01Z",'
        '"price":2,"size":4}'
    )
    t2 = stream_payload_to_tick(STREAM_TRADES, "X", trade_json, message_id="0-2")
    assert t2 is not None and t2["type"] == "market.tick"

    quote_json = (
        '{"symbol":"X","instrument_id":1,"time_utc":"2026-01-01T00:00:02Z",'
        '"bid_price":1,"ask_price":2,"bid_size":1,"ask_size":2}'
    )
    t3 = stream_payload_to_tick(STREAM_QUOTES, "X", quote_json, message_id="0-3")
    assert t3 is not None and t3["type"] == "market.quote"


def test_market_data_redis_keys_normalizes_pubsub_prefixes(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """md:realtime:* is Pub/Sub; XREAD must use md:stream:* (see market-data-service README)."""
    obj = SimpleNamespace(
        market_data_redis_stream_trades="md:realtime:trades",
        market_data_redis_stream_quotes="md:realtime:quotes",
        market_data_redis_stream_bars_1m="md:realtime:bars",
    )
    k = market_data_redis_keys_from_settings(obj)
    assert k.stream_trades == STREAM_TRADES
    assert k.stream_quotes == STREAM_QUOTES
    assert k.stream_bars_1m == STREAM_AM_1M
    out = capsys.readouterr().out
    assert "Pub/Sub" in out
    assert "md:stream" in out or "md:stream:am" in out

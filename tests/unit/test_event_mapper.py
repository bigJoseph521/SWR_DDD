from __future__ import annotations

import pytest
from runtime.bootstrap.event_mapper import (
    EventMapper,
    EventMappingError,
    MarketBarEvent,
    MarketQuoteEvent,
    MarketTickEvent,
    TimerEvent,
)


def test_bar_mapping_accepts_event_type_and_ohlc_aliases() -> None:
    mapper = EventMapper()
    raw = {
        "event_type": "market.bar",
        "symbol": "AAPL",
        "Open": 190.0,
        "High": 191.2,
        "Low": 189.4,
        "Close": 190.8,
        "Volume": 1000,
        "ts_ms": 1710000000000,
    }

    mapped = mapper.map_event(raw)

    assert isinstance(mapped, MarketBarEvent)
    assert mapped.symbol == "AAPL"
    assert mapped.open == 190.0


def test_valid_bar_mapping() -> None:
    mapper = EventMapper()
    raw = {
        "type": "market.bar",
        "symbol": "AAPL",
        "open": 190.0,
        "high": 191.2,
        "low": 189.4,
        "close": 190.8,
        "volume": 1000,
        "ts_ms": 1710000000000,
    }

    mapped = mapper.map_event(raw)

    assert isinstance(mapped, MarketBarEvent)
    assert mapped.symbol == "AAPL"
    assert mapped.volume == 1000.0


def test_valid_quote_mapping_with_price_aliases() -> None:
    mapper = EventMapper()
    raw = {
        "type": "market.quote",
        "symbol": "AAPL",
        "bid_price": 100.0,
        "ask_price": 100.5,
        "bid_size": 10.0,
        "ask_size": 20.0,
        "ts_ms": 1710000000000,
    }
    mapped = mapper.map_event(raw)
    assert isinstance(mapped, MarketQuoteEvent)
    assert mapped.bid == 100.0
    assert mapped.ask == 100.5


def test_valid_tick_mapping_with_last_alias() -> None:
    mapper = EventMapper()
    raw = {
        "type": "market.tick",
        "symbol": "AAPL",
        "last": 100.25,
        "size": 50.0,
        "ts_ms": 1710000000000,
    }
    mapped = mapper.map_event(raw)
    assert isinstance(mapped, MarketTickEvent)
    assert mapped.price == 100.25
    assert mapped.size == 50.0


def test_valid_timer_mapping() -> None:
    mapper = EventMapper()
    raw = {
        "type": "timer",
        "timer_id": "heartbeat",
        "scheduled_at_ms": 1710000000001,
        "metadata": {"scope": "worker"},
    }

    mapped = mapper.map_event(raw)

    assert isinstance(mapped, TimerEvent)
    assert mapped.timer_id == "heartbeat"
    assert mapped.metadata == {"scope": "worker"}


def test_missing_required_field_raises_event_mapping_error() -> None:
    mapper = EventMapper()
    raw = {
        "type": "market.bar",
        "symbol": "AAPL",
        "open": 190.0,
        "high": 191.2,
        "low": 189.4,
        "close": 190.8,
        "volume": 1000,
    }

    with pytest.raises(EventMappingError) as exc_info:
        mapper.map_event(raw)

    assert exc_info.value.reason == "missing_required_field"
    assert exc_info.value.details["field_name"] == "ts_ms"


def test_unsupported_event_type_raises_event_mapping_error() -> None:
    mapper = EventMapper()
    raw = {"type": "market.trade", "symbol": "AAPL"}

    with pytest.raises(EventMappingError) as exc_info:
        mapper.map_event(raw)

    assert exc_info.value.reason == "unsupported_event_type"


def test_mapping_is_deterministic_for_identical_input() -> None:
    mapper = EventMapper()
    raw = {
        "type": "timer",
        "timer_id": "heartbeat",
        "scheduled_at_ms": 1710000000001,
        "metadata": {"scope": "worker"},
    }

    first = mapper.map_event(raw)
    second = mapper.map_event(dict(raw))

    assert first == second

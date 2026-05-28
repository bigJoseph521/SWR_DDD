"""Validate and normalize stdio ``MARKET_DATA_EVENT.event`` payloads for :class:`EventMapper`."""

from __future__ import annotations

from typing import Any, Mapping

_BAR_EVENT_TYPE = "market.bar"


class MarketDataWireError(ValueError):
    """Invalid runner market data payload."""


def validate_market_data_event(event: Mapping[str, Any]) -> None:
    if not isinstance(event, Mapping):
        raise MarketDataWireError("event must be a JSON object")
    if not event:
        raise MarketDataWireError("event must not be empty")
    has_bar = isinstance(event.get("bar"), Mapping)
    has_ohlc = any(k in event for k in ("open", "high", "low", "close"))
    has_nested_bar_ohlc = has_bar and any(
        k in event["bar"]
        for k in ("open", "high", "low", "close")  # type: ignore[index]
    )
    if not (has_ohlc or has_nested_bar_ohlc):
        raise MarketDataWireError(
            "event must include bar OHLC fields (top-level or under bar)"
        )


def normalize_stdio_market_data_event(event: Mapping[str, Any]) -> dict[str, Any]:
    validate_market_data_event(event)
    out: dict[str, Any] = dict(event)

    bar = out.get("bar")
    if isinstance(bar, Mapping):
        for key, value in bar.items():
            if key not in out:
                out[key] = value

    if "type" not in out and "event_type" not in out:
        out["type"] = _BAR_EVENT_TYPE

    symbol = out.get("symbol")
    if not isinstance(symbol, str) or not symbol.strip():
        instrument = out.get("instrument_id")
        if instrument is not None and str(instrument).strip():
            out["symbol"] = str(instrument).strip()

    close = out.get("close")
    if close is not None:
        for field in ("open", "high", "low"):
            if field not in out:
                out[field] = close

    if "volume" not in out:
        out["volume"] = 0.0

    if "ts_ms" not in out:
        out["ts_ms"] = 0

    return out

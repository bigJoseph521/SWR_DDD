from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping

from runtime.application.time_conversion import utc_datetime_to_epoch_millis


class EventMappingError(ValueError):
    def __init__(
        self, *, reason: str, details: Mapping[str, Any] | None = None
    ) -> None:
        super().__init__("Raw event payload could not be mapped.")
        self.reason = reason
        self.details = dict(details or {})


def _require_mapping(value: object, *, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise EventMappingError(
            reason="invalid_field_type",
            details={"field_name": field_name, "expected": "mapping"},
        )
    return value


def _require_str(raw_event: Mapping[str, Any], *, field_name: str) -> str:
    if field_name not in raw_event:
        raise EventMappingError(
            reason="missing_required_field",
            details={"field_name": field_name},
        )
    value = raw_event[field_name]
    if not isinstance(value, str):
        raise EventMappingError(
            reason="invalid_field_type",
            details={"field_name": field_name, "expected": "string"},
        )
    if not value:
        raise EventMappingError(
            reason="invalid_field_value",
            details={"field_name": field_name, "message": "must_not_be_empty"},
        )
    return value


def _require_int(raw_event: Mapping[str, Any], *, field_name: str) -> int:
    if field_name not in raw_event:
        raise EventMappingError(
            reason="missing_required_field",
            details={"field_name": field_name},
        )
    return _coerce_int(raw_event[field_name], field_name=field_name)


def _require_float(raw_event: Mapping[str, Any], *, field_name: str) -> float:
    if field_name not in raw_event:
        raise EventMappingError(
            reason="missing_required_field",
            details={"field_name": field_name},
        )
    return _coerce_float(raw_event[field_name], field_name=field_name)


def _coerce_float(value: object, *, field_name: str) -> float:
    if isinstance(value, bool):
        raise EventMappingError(
            reason="invalid_field_type",
            details={"field_name": field_name, "expected": "number"},
        )
    if isinstance(value, (int, float)):
        return float(value)
    if hasattr(value, "item"):
        try:
            return float(value.item())  # numpy scalar
        except Exception:
            pass
    if isinstance(value, str) and value.strip():
        try:
            return float(value.strip())
        except ValueError:
            pass
    raise EventMappingError(
        reason="invalid_field_type",
        details={"field_name": field_name, "expected": "number"},
    )


def _coerce_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise EventMappingError(
            reason="invalid_field_type",
            details={"field_name": field_name, "expected": "integer"},
        )
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if hasattr(value, "item"):
        try:
            return int(value.item())
        except Exception:
            pass
    if isinstance(value, str) and value.strip():
        try:
            return int(float(value.strip()))
        except ValueError:
            pass
    raise EventMappingError(
        reason="invalid_field_type",
        details={"field_name": field_name, "expected": "integer"},
    )


def _parse_iso_to_ts_ms(value: object) -> int | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return utc_datetime_to_epoch_millis(dt)


def _lower_key_index(payload: Mapping[str, Any]) -> dict[str, str]:
    return {str(k).lower(): str(k) for k in payload}


def _pick_field(payload: dict[str, Any], *candidates: str) -> Any:
    lower = _lower_key_index(payload)
    for name in candidates:
        if name in payload:
            return payload[name]
        key = lower.get(name.lower())
        if key is not None:
            return payload[key]
    return None


@dataclass(frozen=True, slots=True)
class MarketBarEvent:
    event_type: str
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    ts_ms: int


@dataclass(frozen=True, slots=True)
class MarketQuoteEvent:
    event_type: str
    symbol: str
    bid: float
    ask: float
    bid_size: float
    ask_size: float
    ts_ms: int


@dataclass(frozen=True, slots=True)
class MarketTickEvent:
    event_type: str
    symbol: str
    price: float
    size: float
    ts_ms: int


@dataclass(frozen=True, slots=True)
class TimerEvent:
    event_type: str
    timer_id: str
    scheduled_at_ms: int
    metadata: Mapping[str, Any] = field(default_factory=dict)


MappedEvent = MarketBarEvent | MarketQuoteEvent | MarketTickEvent | TimerEvent


class EventMapper:
    _BAR_TYPE = "market.bar"
    _QUOTE_TYPE = "market.quote"
    _TICK_TYPE = "market.tick"
    _TIMER_TYPE = "timer"

    def _normalize_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Align replay/parquet payloads with mapper expectations (no strategy changes)."""
        if "type" not in payload and "event_type" in payload:
            et = payload.get("event_type")
            if isinstance(et, str) and et:
                payload["type"] = et

        event_type = payload.get("type")
        if not isinstance(event_type, str) or not event_type:
            return payload

        if event_type == self._BAR_TYPE:
            return self._normalize_market_bar_payload(payload)
        if event_type == self._QUOTE_TYPE:
            return self._normalize_market_quote_payload(payload)
        if event_type == self._TICK_TYPE:
            return self._normalize_market_tick_payload(payload)
        return payload

    def _normalize_market_bar_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        open_v = _pick_field(payload, "open", "Open", "OPEN")
        high_v = _pick_field(payload, "high", "High", "HIGH")
        low_v = _pick_field(payload, "low", "Low", "LOW")
        close_v = _pick_field(
            payload, "close", "Close", "CLOSE", "Adj Close", "adj_close"
        )
        vol_v = _pick_field(payload, "volume", "Volume", "VOLUME")
        sym_v = _pick_field(payload, "symbol", "Symbol", "ticker", "instrument_id")
        ts_ms_v = _pick_field(payload, "ts_ms", "TsMs")

        if open_v is not None and "open" not in payload:
            payload["open"] = open_v
        if high_v is not None and "high" not in payload:
            payload["high"] = high_v
        if low_v is not None and "low" not in payload:
            payload["low"] = low_v
        if close_v is not None and "close" not in payload:
            payload["close"] = close_v
        if vol_v is not None and "volume" not in payload:
            payload["volume"] = vol_v
        if sym_v is not None and "symbol" not in payload:
            payload["symbol"] = sym_v

        if ts_ms_v is None and "ts_ms" not in payload:
            et = _pick_field(
                payload, "event_time", "timestamp", "datetime", "Date", "date"
            )
            ts_ms_v = _parse_iso_to_ts_ms(et) if isinstance(et, str) else None
            if ts_ms_v is None and et is not None:
                try:
                    ts_ms_v = _coerce_int(et, field_name="ts_ms")
                except EventMappingError:
                    ts_ms_v = None
            if ts_ms_v is not None:
                payload["ts_ms"] = ts_ms_v
        return payload

    def _normalize_market_quote_payload(
        self, payload: dict[str, Any]
    ) -> dict[str, Any]:
        sym_v = _pick_field(payload, "symbol", "Symbol", "ticker", "instrument_id")
        bid_v = _pick_field(payload, "bid", "bid_price", "Bid", "BID")
        ask_v = _pick_field(payload, "ask", "ask_price", "Ask", "ASK")
        bid_sz = _pick_field(payload, "bid_size", "bidSize", "BidSize")
        ask_sz = _pick_field(payload, "ask_size", "askSize", "AskSize")
        ts_ms_v = _pick_field(payload, "ts_ms", "TsMs")

        if sym_v is not None and "symbol" not in payload:
            payload["symbol"] = sym_v
        if bid_v is not None and "bid" not in payload:
            payload["bid"] = bid_v
        if ask_v is not None and "ask" not in payload:
            payload["ask"] = ask_v
        if bid_sz is not None and "bid_size" not in payload:
            payload["bid_size"] = bid_sz
        if ask_sz is not None and "ask_size" not in payload:
            payload["ask_size"] = ask_sz

        if ts_ms_v is None and "ts_ms" not in payload:
            et = _pick_field(payload, "event_time", "timestamp", "datetime")
            ts_ms_v = _parse_iso_to_ts_ms(et) if isinstance(et, str) else None
            if ts_ms_v is not None:
                payload["ts_ms"] = ts_ms_v
        return payload

    def _normalize_market_tick_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        sym_v = _pick_field(payload, "symbol", "Symbol", "ticker", "instrument_id")
        price_v = _pick_field(
            payload, "price", "last", "last_price", "Last", "trade_price"
        )
        size_v = _pick_field(
            payload, "size", "volume", "last_size", "trade_size", "qty"
        )
        ts_ms_v = _pick_field(payload, "ts_ms", "TsMs")

        if sym_v is not None and "symbol" not in payload:
            payload["symbol"] = sym_v
        if price_v is not None and "price" not in payload:
            payload["price"] = price_v
        if size_v is not None and "size" not in payload:
            payload["size"] = size_v

        if ts_ms_v is None and "ts_ms" not in payload:
            et = _pick_field(payload, "event_time", "timestamp", "datetime")
            ts_ms_v = _parse_iso_to_ts_ms(et) if isinstance(et, str) else None
            if ts_ms_v is not None:
                payload["ts_ms"] = ts_ms_v
        return payload

    def map_event(self, raw_event: Mapping[str, Any]) -> MappedEvent:
        payload = dict(_require_mapping(raw_event, field_name="raw_event"))
        payload = self._normalize_payload(payload)
        event_type = _require_str(payload, field_name="type")

        if event_type == self._BAR_TYPE:
            return MarketBarEvent(
                event_type=event_type,
                symbol=_require_str(payload, field_name="symbol"),
                open=_require_float(payload, field_name="open"),
                high=_require_float(payload, field_name="high"),
                low=_require_float(payload, field_name="low"),
                close=_require_float(payload, field_name="close"),
                volume=_require_float(payload, field_name="volume"),
                ts_ms=_require_int(payload, field_name="ts_ms"),
            )

        if event_type == self._QUOTE_TYPE:
            return MarketQuoteEvent(
                event_type=event_type,
                symbol=_require_str(payload, field_name="symbol"),
                bid=_require_float(payload, field_name="bid"),
                ask=_require_float(payload, field_name="ask"),
                bid_size=_require_float(payload, field_name="bid_size"),
                ask_size=_require_float(payload, field_name="ask_size"),
                ts_ms=_require_int(payload, field_name="ts_ms"),
            )

        if event_type == self._TICK_TYPE:
            return MarketTickEvent(
                event_type=event_type,
                symbol=_require_str(payload, field_name="symbol"),
                price=_require_float(payload, field_name="price"),
                size=_require_float(payload, field_name="size"),
                ts_ms=_require_int(payload, field_name="ts_ms"),
            )

        if event_type == self._TIMER_TYPE:
            metadata_value = payload.get("metadata")
            metadata: Mapping[str, Any]
            if metadata_value is None:
                metadata = {}
            else:
                metadata = _require_mapping(metadata_value, field_name="metadata")

            return TimerEvent(
                event_type=event_type,
                timer_id=_require_str(payload, field_name="timer_id"),
                scheduled_at_ms=_require_int(payload, field_name="scheduled_at_ms"),
                metadata=dict(metadata),
            )

        raise EventMappingError(
            reason="unsupported_event_type",
            details={"event_type": event_type},
        )

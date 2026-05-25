from __future__ import annotations

import dataclasses
from datetime import datetime
from typing import Any, Sequence, cast

import pandas as pd
from alphovex_sdk.context.data_context import DataContext
from alphovex_sdk.models import Bar, Quote, Tick
from alphovex_sdk.typedefs import Symbol, Timeframe, TimestampLike
from pandas import DataFrame


def _as_datetime(value: TimestampLike | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).replace("Z", "+00:00")
    return datetime.fromisoformat(text)


def _bar_ts(bar: object) -> datetime | None:
    ts = getattr(bar, "ts_event", None) or getattr(bar, "timestamp", None)
    return ts if isinstance(ts, datetime) else None


def _obj_to_dict(obj: object) -> dict[str, Any]:
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return dataclasses.asdict(obj)
    model_dump = getattr(obj, "model_dump", None)
    if callable(model_dump):
        return model_dump()
    if hasattr(obj, "__dict__"):
        return {k: v for k, v in vars(obj).items() if not k.startswith("_")}
    raise TypeError(f"cannot convert {type(obj)!r} to dict")


def _bars_to_dataframe(bars: Sequence[object]) -> DataFrame:
    if not bars:
        return pd.DataFrame()
    rows = [_obj_to_dict(cast(object, b)) for b in bars]
    return pd.DataFrame(rows)


class RuntimeDataContext(DataContext):
    """Thin :class:`DataContext` over replay :class:`InMemoryReplayDataService`."""

    __slots__ = ("_data",)

    def __init__(self, *, data_service: object) -> None:
        self._data = data_service

    def _instrument_id(self, symbol: Symbol) -> str:
        return str(symbol)

    def latest_bar(self, symbol: Symbol, timeframe: Timeframe = "1m") -> Bar | None:
        fn = getattr(self._data, "latest_bar", None)
        if not callable(fn):
            return None
        return cast(Bar | None, fn(self._instrument_id(symbol), timeframe))

    def latest_bar_as_dict(
        self, symbol: Symbol, timeframe: Timeframe = "1m"
    ) -> dict | None:
        bar = self.latest_bar(symbol, timeframe)
        return None if bar is None else _obj_to_dict(bar)

    def latest_quote(self, symbol: Symbol) -> Quote | None:
        fn = getattr(self._data, "latest_quote", None)
        if not callable(fn):
            return None
        return cast(Quote | None, fn(self._instrument_id(symbol)))

    def latest_quote_as_dict(self, symbol: Symbol) -> dict | None:
        quote = self.latest_quote(symbol)
        return None if quote is None else _obj_to_dict(quote)

    def latest_tick(self, symbol: Symbol) -> Tick | None:
        fn = getattr(self._data, "latest_tick", None)
        if not callable(fn):
            return None
        return cast(Tick | None, fn(self._instrument_id(symbol)))

    def latest_tick_as_dict(self, symbol: Symbol) -> dict | None:
        tick = self.latest_tick(symbol)
        return None if tick is None else _obj_to_dict(tick)

    def _filter_series(
        self,
        series: Sequence[object],
        *,
        start: datetime | None,
        end: datetime | None,
        limit: int | None,
    ) -> list[object]:
        out: list[object] = []
        for item in series:
            ts = _bar_ts(item)
            if ts is None:
                continue
            if start is not None and ts < start:
                continue
            if end is not None and ts > end:
                continue
            out.append(item)
        if limit is not None and limit >= 0:
            return out[-limit:] if limit else []
        return out

    def get_bars(
        self,
        symbol: Symbol,
        timeframe: Timeframe = "1m",
        *,
        start: TimestampLike | None = None,
        end: TimestampLike | None = None,
        limit: int | None = None,
    ) -> Sequence[Bar]:
        bars_fn = getattr(self._data, "bars", None)
        if not callable(bars_fn):
            return ()
        hist = bars_fn(self._instrument_id(symbol), timeframe)
        raw = tuple(getattr(hist, "bars", ()) or ())
        filtered = self._filter_series(
            raw,
            start=_as_datetime(start),
            end=_as_datetime(end),
            limit=limit,
        )
        return cast(Sequence[Bar], tuple(filtered))

    def get_bars_as_dataframe(
        self,
        symbol: Symbol,
        timeframe: Timeframe = "1m",
        *,
        start: TimestampLike | None = None,
        end: TimestampLike | None = None,
        limit: int | None = None,
    ) -> DataFrame:
        bars = self.get_bars(symbol, timeframe, start=start, end=end, limit=limit)
        return _bars_to_dataframe(cast(Sequence[object], bars))

    def get_ticks(
        self,
        symbol: Symbol,
        *,
        start: TimestampLike | None = None,
        end: TimestampLike | None = None,
        limit: int | None = None,
    ) -> Sequence[Tick]:
        tick = self.latest_tick(symbol)
        if tick is None:
            return ()
        ts = _bar_ts(tick)
        if ts is None:
            return ()
        s0 = _as_datetime(start)
        e0 = _as_datetime(end)
        if s0 is not None and ts < s0:
            return ()
        if e0 is not None and ts > e0:
            return ()
        if limit == 0:
            return ()
        return (tick,)

    def get_ticks_as_dataframe(
        self,
        symbol: Symbol,
        *,
        start: TimestampLike | None = None,
        end: TimestampLike | None = None,
        limit: int | None = None,
    ) -> DataFrame:
        ticks = self.get_ticks(symbol, start=start, end=end, limit=limit)
        return _bars_to_dataframe(cast(Sequence[object], ticks))

    def get_quotes(
        self,
        symbol: Symbol,
        *,
        start: TimestampLike | None = None,
        end: TimestampLike | None = None,
        limit: int | None = None,
    ) -> Sequence[Quote]:
        quote = self.latest_quote(symbol)
        if quote is None:
            return ()
        ts = _bar_ts(quote)
        if ts is None:
            return ()
        s0 = _as_datetime(start)
        e0 = _as_datetime(end)
        if s0 is not None and ts < s0:
            return ()
        if e0 is not None and ts > e0:
            return ()
        if limit == 0:
            return ()
        return (quote,)

    def get_quotes_as_dataframe(
        self,
        symbol: Symbol,
        *,
        start: TimestampLike | None = None,
        end: TimestampLike | None = None,
        limit: int | None = None,
    ) -> DataFrame:
        quotes = self.get_quotes(symbol, start=start, end=end, limit=limit)
        return _bars_to_dataframe(cast(Sequence[object], quotes))

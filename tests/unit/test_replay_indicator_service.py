from __future__ import annotations

from datetime import datetime, timezone

import pytest
from alphovex_sdk.context.indicator_context import DataSourceEnum
from alphovex_sdk.indicators.trend import SMA
from alphovex_sdk.models.market_data import Bar
from runtime.bootstrap.replay_runtime_support import IndicatorService


def _bar(*, close: float) -> Bar:
    ts = datetime(2020, 1, 1, tzinfo=timezone.utc)
    return Bar(
        symbol="AAPL",
        timeframe="1d",
        open=close,
        high=close,
        low=close,
        close=close,
        volume=1.0,
        timestamp=ts,
        instrument_id="AAPL",
    )


def test_indicator_service_advances_sma_before_value_readable() -> None:
    svc = IndicatorService(data=object(), timeframe="1d")
    svc.register_indicator("s2", SMA(2), DataSourceEnum.BAR)

    assert svc.get_indicator_value("s2") is None

    svc.advance_on_bar(_bar(close=10.0))
    assert svc.get_indicator_value("s2") is None

    svc.advance_on_bar(_bar(close=20.0))
    assert svc.get_indicator_value("s2") == pytest.approx(15.0)

    svc.advance_on_bar(_bar(close=30.0))
    assert svc.get_indicator_value("s2") == pytest.approx(25.0)


def test_unknown_indicator_id_returns_none() -> None:
    svc = IndicatorService(data=object(), timeframe="1d")
    assert svc.get_indicator_value("missing") is None


def test_bar_source_only_updates_bar_indicators() -> None:
    svc = IndicatorService(data=object(), timeframe="1d")
    svc.register_indicator("s1", SMA(1), DataSourceEnum.BAR)
    b = _bar(close=5.0)
    svc.advance_on_bar(b)
    assert svc.get_indicator_value("s1") == pytest.approx(5.0)

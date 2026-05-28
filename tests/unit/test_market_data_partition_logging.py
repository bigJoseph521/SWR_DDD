from __future__ import annotations

import json
import sys
import threading
import types
from typing import Any
import pytest

from runtime.infrastructure.redis.market_data_redis_feed import (
    STREAM_AM_1M,
    run_market_data_redis_loop,
)


def _install_minimal_redis_module(monkeypatch: pytest.MonkeyPatch) -> None:
    m = types.ModuleType("redis")

    class ResponseError(Exception):
        pass

    m.ResponseError = ResponseError
    m.ConnectionError = ConnectionError
    m.TimeoutError = TimeoutError

    class Redis:
        @staticmethod
        def from_url(*_a: Any, **_k: Any) -> Any:
            raise RuntimeError("Redis.from_url should be patched in tests")

    m.Redis = Redis
    monkeypatch.setitem(sys.modules, "redis", m)


class _FakeRedis:
    def __init__(self, *, payloads: list[tuple[str, str, str, str]]) -> None:
        self._q = list(payloads)
        self.closed = False

    def close(self) -> None:
        self.closed = True

    def xread(self, streams: Any, count: Any = None, block: Any = None) -> Any:
        if not self._q:
            return []
        sk, mid, field, body = self._q.pop(0)
        return [(sk, [(mid, {field: body})])]


def test_partition_skips_non_strategy_symbol_log_and_dispatch(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _install_minimal_redis_module(monkeypatch)
    bar_aapl = json.dumps(
        {
            "symbol": "AAPL",
            "time_utc": "2026-01-01T00:00:00Z",
            "open": 1,
            "high": 1,
            "low": 1,
            "close": 1,
            "volume": 1,
        }
    )
    bar_msft = json.dumps(
        {
            "symbol": "MSFT",
            "time_utc": "2026-01-01T00:01:00Z",
            "open": 2,
            "high": 2,
            "low": 2,
            "close": 2,
            "volume": 2,
        }
    )
    fake = _FakeRedis(
        payloads=[
            (f"{STREAM_AM_1M}:7", "1-0", "MSFT", bar_msft),
            (f"{STREAM_AM_1M}:7", "2-0", "AAPL", bar_aapl),
        ],
    )
    monkeypatch.setattr(
        "runtime.infrastructure.redis.market_data_redis_feed._open_redis_client",
        lambda _url: fake,
    )
    stop = threading.Event()
    dispatched: list[str] = []

    def on_tick(t: dict[str, Any]) -> None:
        dispatched.append(str(t.get("symbol") or ""))
        stop.set()

    run_market_data_redis_loop(
        redis_url="redis://localhost:6379/0",
        stream_names=[f"{STREAM_AM_1M}:7"],
        stream_start_id="$",
        block_ms=0,
        count=10,
        strategy_symbol="AAPL",
        on_tick=on_tick,
        should_stop=stop,
    )
    out = capsys.readouterr().out
    assert "partition_stream_entry" in out
    assert '"symbol": "AAPL"' in out or '"symbol":"AAPL"' in out
    assert "MSFT" not in out
    assert (
        '"matches_strategy_symbol": true' in out
        or '"matches_strategy_symbol":true' in out
    )
    assert dispatched == ["AAPL"]

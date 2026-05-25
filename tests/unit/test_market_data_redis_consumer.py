from __future__ import annotations

import json
import sys
import threading
import types
from typing import Any
from unittest.mock import MagicMock

import pytest

from runtime.infrastructure.redis.market_data_redis_feed import (
    STREAM_AM_1M,
    build_market_data_consumer_group_name,
    classify_stream_payload_failure,
    run_market_data_redis_loop,
    sanitize_redis_stream_consumer_token,
)


def _install_minimal_redis_module(monkeypatch: pytest.MonkeyPatch) -> None:
    """``run_market_data_redis_loop`` imports ``redis`` lazily; stub types for tests without redis installed."""
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


def test_sanitize_redis_stream_consumer_token() -> None:
    assert sanitize_redis_stream_consumer_token("a:b@c") == "a_b_c"
    assert sanitize_redis_stream_consumer_token("") == "swr"


def test_build_market_data_consumer_group_name_prefers_deployment() -> None:
    g = build_market_data_consumer_group_name(
        group_prefix="pfx",
        deployment_id="dep-1",
        runtime_id="rt-1",
    )
    assert g.startswith("pfx:")
    assert "dep-1" in g


def test_build_market_data_consumer_group_name_falls_back_to_runtime() -> None:
    g = build_market_data_consumer_group_name(
        group_prefix="pfx",
        deployment_id=None,
        runtime_id="rt-2",
    )
    assert "rt-2" in g


def test_classify_stream_payload_failure_json() -> None:
    r = classify_stream_payload_failure(
        f"{STREAM_AM_1M}:0",
        "AAPL",
        "not-json",
        message_id="1-0",
    )
    assert r == "json_decode_error"


class _FakeRedis:
    """Minimal fake for XREAD / XREADGROUP / XACK paths in ``run_market_data_redis_loop``."""

    def __init__(self, *, payloads: list[tuple[str, str, str, str]]) -> None:
        """
        payloads: list of (stream_key, msg_id, field_sym, json_payload) then stop.
        """
        self._q = list(payloads)
        self.xgroup_create = MagicMock()
        self.xack = MagicMock()
        self.closed = False
        self.xread_calls: list[dict[str, Any]] = []

    def close(self) -> None:
        self.closed = True

    def xread(self, streams: Any, count: Any = None, block: Any = None) -> Any:
        self.xread_calls.append({"streams": streams, "count": count, "block": block})
        if not self._q:
            return []
        sk, mid, field, body = self._q.pop(0)
        if not isinstance(streams, dict) or sk not in streams:
            raise AssertionError((streams, sk))
        return [(sk, [(mid, {field: body})])]

    def xreadgroup(
        self, **kwargs: Any
    ) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
        streams = kwargs.get("streams") or {}
        if not self._q:
            return []
        sk, mid, field, body = self._q.pop(0)
        assert sk in streams
        return [(sk, [(mid, {field: body})])]


def test_run_market_data_xread_uses_block_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Matches ``redis-cli XREAD BLOCK 0 STREAMS md:stream:am:{p} $`` (infinite block)."""
    _install_minimal_redis_module(monkeypatch)
    bar = json.dumps(
        {
            "symbol": "AAPL",
            "instrument_id": 1,
            "time_utc": "2026-01-01T00:00:00Z",
            "open": 1,
            "high": 1,
            "low": 1,
            "close": 1,
            "volume": 1,
        }
    )
    fake = _FakeRedis(payloads=[(f"{STREAM_AM_1M}:7", "1-0", "AAPL", bar)])
    monkeypatch.setattr(
        "runtime.infrastructure.redis.market_data_redis_feed._open_redis_client",
        lambda _url: fake,
    )
    stop = threading.Event()
    captured: list[dict[str, Any]] = []

    def on_tick(t: dict[str, Any]) -> None:
        captured.append(dict(t))
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
        use_consumer_group=False,
    )
    assert captured and captured[0].get("symbol") == "AAPL"
    assert fake.xread_calls, "expected XREAD path"
    assert fake.xread_calls[0]["block"] == 0


def test_run_market_data_redis_consumer_group_invokes_on_tick_and_xacks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_minimal_redis_module(monkeypatch)
    captured: list[dict[str, Any]] = []
    bar = json.dumps(
        {
            "symbol": "AAPL",
            "instrument_id": 1,
            "time_utc": "2026-01-01T00:00:00Z",
            "open": 1,
            "high": 1,
            "low": 1,
            "close": 1,
            "volume": 1,
        }
    )
    fake = _FakeRedis(
        payloads=[(f"{STREAM_AM_1M}:0", "99-0", "AAPL", bar)],
    )

    monkeypatch.setattr(
        "runtime.infrastructure.redis.market_data_redis_feed._open_redis_client",
        lambda _url: fake,
    )

    stop = threading.Event()

    def on_tick(t: dict[str, Any]) -> None:
        captured.append(dict(t))
        stop.set()

    run_market_data_redis_loop(
        redis_url="redis://localhost:6379/0",
        stream_names=[f"{STREAM_AM_1M}:0"],
        stream_start_id="$",
        block_ms=1,
        count=10,
        strategy_symbol="AAPL",
        on_tick=on_tick,
        should_stop=stop,
        use_consumer_group=True,
        consumer_group_name="g-test",
        consumer_name="c-test",
    )
    assert captured and captured[0].get("symbol") == "AAPL"
    fake.xgroup_create.assert_called()
    fake.xack.assert_called()


def test_run_market_data_redis_malformed_does_not_crash_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_minimal_redis_module(monkeypatch)
    fake = _FakeRedis(
        payloads=[
            (f"{STREAM_AM_1M}:0", "1-0", "AAPL", "not-json"),
            (
                f"{STREAM_AM_1M}:0",
                "2-0",
                "AAPL",
                json.dumps(
                    {
                        "symbol": "AAPL",
                        "instrument_id": 1,
                        "time_utc": "2026-01-01T00:00:00Z",
                        "open": 1,
                        "high": 1,
                        "low": 1,
                        "close": 1,
                        "volume": 1,
                    }
                ),
            ),
        ],
    )

    monkeypatch.setattr(
        "runtime.infrastructure.redis.market_data_redis_feed._open_redis_client",
        lambda _url: fake,
    )
    stop = threading.Event()
    ticks: list[dict[str, Any]] = []

    def on_tick(t: dict[str, Any]) -> None:
        ticks.append(t)
        stop.set()

    run_market_data_redis_loop(
        redis_url="redis://localhost:6379/0",
        stream_names=[f"{STREAM_AM_1M}:0"],
        stream_start_id="$",
        block_ms=1,
        count=10,
        strategy_symbol="AAPL",
        on_tick=on_tick,
        should_stop=stop,
        use_consumer_group=True,
        consumer_group_name="g-mal",
        consumer_name="c-mal",
    )
    assert len(ticks) == 1


def test_run_market_data_redis_on_tick_exception_still_xacks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_minimal_redis_module(monkeypatch)
    bar = json.dumps(
        {
            "symbol": "AAPL",
            "instrument_id": 1,
            "time_utc": "2026-01-01T00:00:00Z",
            "open": 1,
            "high": 1,
            "low": 1,
            "close": 1,
            "volume": 1,
        }
    )
    fake = _FakeRedis(payloads=[(f"{STREAM_AM_1M}:0", "1-0", "AAPL", bar)])
    monkeypatch.setattr(
        "runtime.infrastructure.redis.market_data_redis_feed._open_redis_client",
        lambda _url: fake,
    )
    stop = threading.Event()

    def on_tick(_t: dict[str, Any]) -> None:
        raise RuntimeError("boom")

    threading.Timer(0.25, stop.set).start()
    run_market_data_redis_loop(
        redis_url="redis://localhost:6379/0",
        stream_names=[f"{STREAM_AM_1M}:0"],
        stream_start_id="$",
        block_ms=50,
        count=10,
        strategy_symbol="AAPL",
        on_tick=on_tick,
        should_stop=stop,
        use_consumer_group=True,
        consumer_group_name="g-exc",
        consumer_name="c-exc",
    )
    fake.xack.assert_called()

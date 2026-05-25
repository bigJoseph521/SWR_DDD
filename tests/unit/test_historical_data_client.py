from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import grpc
import pytest
from runtime.transport.grpc.historical_data_client import (
    HistoricalDataGrpcClient,
    build_historical_bar_replay_tick,
    build_query_historical_request,
    launch_payload_backtest_symbol,
    print_historical_data_service_connectivity_at_startup,
)


def _historical_pb2():
    root = Path(__file__).resolve().parents[2] / "protos" / "generated"
    sys.path.insert(0, str(root))
    import importlib

    return importlib.import_module("historical_data_pb2")


def test_launch_payload_backtest_symbol() -> None:
    assert (
        launch_payload_backtest_symbol({"parameters": {"symbol": " AAPL "}}) == "AAPL"
    )
    assert launch_payload_backtest_symbol({}) is None


def test_build_query_sets_warmup_only_without_cursor() -> None:
    h = _historical_pb2()
    req = build_query_historical_request(
        symbol="SPY",
        timeframe="1d",
        from_utc="2020-01-01T00:00:00Z",
        to_utc="2020-12-31T00:00:00Z",
        limit=1000,
        cursor=None,
        warmup_bars=25,
        historical_data_pb2=h,
    )
    assert req.symbol == "SPY"
    assert req.limit == 1000
    assert req.warmup_bars == 25
    assert not req.HasField("cursor")


def test_build_query_omits_warmup_when_cursor_set() -> None:
    h = _historical_pb2()
    req = build_query_historical_request(
        symbol="SPY",
        timeframe="1d",
        from_utc="2020-01-01T00:00:00Z",
        to_utc="2020-12-31T00:00:00Z",
        limit=1000,
        cursor="2020-06-01 00:00:00+00",
        warmup_bars=99,
        historical_data_pb2=h,
    )
    assert req.cursor == "2020-06-01 00:00:00+00"
    assert not req.HasField("warmup_bars")


def test_build_historical_bar_replay_tick() -> None:
    h = _historical_pb2()
    bar = h.HistoricalBar(
        instrument_id=123,
        time_utc="2020-03-15 14:30:00+00",
        open=1.0,
        high=2.0,
        low=0.5,
        close=1.5,
        volume=1000,
    )
    tick = build_historical_bar_replay_tick(
        bar=bar, symbol="SPY", timeframe="1d", sequence=0
    )
    assert tick["type"] == "market.bar"
    assert tick["symbol"] == "SPY"
    assert tick["timeframe"] == "1d"
    assert tick["instrument_id"] == "SPY"
    assert tick["ts_ms"] > 0


def test_health_check_returns_status() -> None:
    h = _historical_pb2()
    stub = MagicMock()
    stub.HealthCheck.return_value = h.HealthCheckResponse(status="ok")
    client = HistoricalDataGrpcClient(MagicMock(), stub, timeout_seconds=30.0)
    assert client.health_check(timeout_seconds=1.0) == "ok"
    stub.HealthCheck.assert_called_once()
    assert stub.HealthCheck.call_args.kwargs.get("timeout") == pytest.approx(1.0)


def test_print_connectivity_empty_target(capsys: pytest.CaptureFixture[str]) -> None:
    print_historical_data_service_connectivity_at_startup("", None)
    out = capsys.readouterr().out
    assert "not configured" in out
    assert "historical_data_grpc_target is empty" in out


def test_print_connectivity_success(capsys: pytest.CaptureFixture[str]) -> None:
    h = _historical_pb2()
    stub = MagicMock()
    stub.HealthCheck.return_value = h.HealthCheckResponse(status="ok")
    client = HistoricalDataGrpcClient(MagicMock(), stub, timeout_seconds=30.0)
    print_historical_data_service_connectivity_at_startup("127.0.0.1:50051", client)
    out = capsys.readouterr().out
    assert "connected" in out
    assert "127.0.0.1:50051" in out


class _FakeRpcError(grpc.RpcError):
    def code(self) -> grpc.StatusCode:
        return grpc.StatusCode.UNAVAILABLE

    def details(self) -> str:
        return "connection refused"


def test_print_connectivity_rpc_error(capsys: pytest.CaptureFixture[str]) -> None:
    stub = MagicMock()
    stub.HealthCheck.side_effect = _FakeRpcError()
    client = HistoricalDataGrpcClient(MagicMock(), stub, timeout_seconds=30.0)
    print_historical_data_service_connectivity_at_startup("127.0.0.1:50051", client)
    out = capsys.readouterr().out
    assert "NOT connected" in out
    assert "UNAVAILABLE" in out


def test_build_historical_bar_replay_tick_rejects_bad_time() -> None:
    h = _historical_pb2()
    bar = h.HistoricalBar(
        instrument_id=1,
        time_utc="not-a-time",
        open=1.0,
        high=1.0,
        low=1.0,
        close=1.0,
        volume=1,
    )
    with pytest.raises(ValueError):
        build_historical_bar_replay_tick(
            bar=bar, symbol="X", timeframe="1d", sequence=0
        )

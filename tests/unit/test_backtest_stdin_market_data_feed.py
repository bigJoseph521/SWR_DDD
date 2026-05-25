"""Unit tests for BACKTEST stdin market-data feed skeleton and guard rails."""

from __future__ import annotations

import io
import json
from pathlib import Path

from runtime.interface.stdio.backtest_stdin_market_data_feed import (
    BacktestStdinMarketDataFeed,
)
from runtime.interface.stdio.stdin_protocol import (
    ALLOWED_STDIN_MESSAGE_TYPES,
    StdinMessageType,
)


def test_backtest_stdin_feed_skeleton_exists() -> None:
    stream = io.StringIO()
    feed = BacktestStdinMarketDataFeed(input_stream=stream)
    assert feed is not None
    assert feed._input is stream


def test_backtest_stdin_feed_does_not_support_open_orders_snapshot() -> None:
    assert "OPEN_ORDERS_SNAPSHOT" not in ALLOWED_STDIN_MESSAGE_TYPES
    assert not any(
        "OPEN_ORDERS" in member.value for member in StdinMessageType
    )


def test_backtest_stdin_feed_parses_minimal_market_data_line() -> None:
    stream = io.StringIO(
        json.dumps(
            {
                "type": "MARKET_DATA",
                "payload": {"symbol": "SPY", "type": "market.bar", "ts_ms": 1},
            }
        )
        + "\n"
    )
    feed = BacktestStdinMarketDataFeed(input_stream=stream)
    received: list[dict[str, object]] = []
    feed.start(lambda tick: received.append(dict(tick)))
    feed.stop()
    assert len(received) == 1
    assert received[0]["symbol"] == "SPY"


def test_backtest_stdin_feed_end_of_stream_invokes_callback() -> None:
    stream = io.StringIO(json.dumps({"type": "END_OF_STREAM"}) + "\n")
    feed = BacktestStdinMarketDataFeed(input_stream=stream)
    seen: list[str] = []
    feed.start(lambda _tick: None, on_end_of_stream=lambda: seen.append("eos"))
    feed.stop()
    assert seen == ["eos"]


def test_runtime_has_no_replay_ingress_grpc_wiring() -> None:
    from pathlib import Path

    repo_root = Path(__file__).resolve().parents[2]
    assert not (repo_root / "runtime/bootstrap/replay_ingress_wiring.py").exists()
    entrypoint = (repo_root / "runtime/bootstrap/runtime_entrypoint.py").read_text(
        encoding="utf-8"
    )
    assert "replay_ingress_wiring" not in entrypoint
    assert "replay_ingress_grpc" not in entrypoint


def test_strategy_worker_runtime_has_no_backtest_service_dependency() -> None:
    from pathlib import Path

    repo_root = Path(__file__).resolve().parents[2]
    runtime_root = repo_root / "runtime"
    forbidden = (
        "backtest_service",
        "BacktestService",
        "BACKTEST_SERVICE",
        "backtest-service",
    )
    violations: list[str] = []
    for path in sorted(runtime_root.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        text = path.read_text(encoding="utf-8")
        for token in forbidden:
            if token in text:
                violations.append(f"{path.relative_to(repo_root)}: {token}")
    assert violations == []

"""
Subprocess integration: ``python -m runtime.main`` with CLI bootstrap (backtest-runner contract).
"""

from __future__ import annotations

import json
import os
import select
import subprocess
import sys
from pathlib import Path
from typing import Any, TextIO

import pytest

_SWR_ROOT = Path(__file__).resolve().parents[2]
_FIXTURES = _SWR_ROOT / "tests" / "fixtures" / "runtime_backtest_stdio"
_INTEGRATION_STRATEGY_DIR = _FIXTURES / "integration_protocol_strategy"
_SUBPROCESS_TIMEOUT_SEC = 30.0
_READLINE_TIMEOUT_SEC = 10.0


def _pythonpath() -> str:
    generated = _SWR_ROOT / "protos" / "generated"
    return os.pathsep.join([str(_SWR_ROOT), str(generated)])


def _subprocess_env() -> dict[str, str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = _pythonpath()
    return env


def _start_stdio_worker(
    *, artifact_dir: Path, entrypoint: str
) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [
            sys.executable,
            "-u",
            "-m",
            "runtime.main",
            "--mode",
            "BACKTEST",
            "--transport",
            "stdio-jsonl",
            "--artifact-path",
            str(artifact_dir.resolve()),
            "--entrypoint",
            entrypoint,
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        cwd=str(_SWR_ROOT),
        env=_subprocess_env(),
    )


def _readline_with_timeout(stream: TextIO, timeout_sec: float) -> str:
    fileno = stream.fileno()
    ready, _, _ = select.select([fileno], [], [], timeout_sec)
    if not ready:
        raise TimeoutError(f"no stdout line within {timeout_sec}s")
    line = stream.readline()
    if line == "":
        raise EOFError("stdout closed before response line")
    return line


def _parse_protocol_line(line: str, *, context: str) -> dict[str, Any]:
    stripped = line.strip()
    if not stripped:
        pytest.fail(f"{context}: empty stdout line (expected JSONL protocol frame)")
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError as exc:
        pytest.fail(f"{context}: stdout is not valid JSON ({stripped!r}): {exc}")
    if not isinstance(parsed, dict) or not isinstance(parsed.get("type"), str):
        pytest.fail(f"{context}: protocol frame must be object with string type")
    return parsed


def _exchange(
    proc: subprocess.Popen[str],
    payload: dict[str, Any],
    *,
    context: str,
) -> dict[str, Any]:
    assert proc.stdin is not None
    assert proc.stdout is not None
    proc.stdin.write(json.dumps(payload, separators=(",", ":")) + "\n")
    proc.stdin.flush()
    line = _readline_with_timeout(proc.stdout, _READLINE_TIMEOUT_SEC)
    return _parse_protocol_line(line, context=context)


def _runner_envelope(
    sequence: int,
    *,
    message_id: str,
    backtest_job_id: str = "job-integration-1",
    runtime_id: str = "runtime-integration-1",
) -> dict[str, Any]:
    return {
        "message_id": message_id,
        "sequence": sequence,
        "backtest_job_id": backtest_job_id,
        "runtime_id": runtime_id,
    }


def _sample_portfolio_payload() -> dict[str, object]:
    return {
        "currency": "USD",
        "cash_balance": "100000",
        "equity": "100000",
        "positions": [],
    }


def _bar_payload() -> dict[str, object]:
    return {
        "instrument_id": "AAPL",
        "symbol": "AAPL",
        "bar": {
            "open": "100",
            "high": "101",
            "low": "99",
            "close": "100.5",
            "volume": "1000",
        },
    }


def _finish_worker(proc: subprocess.Popen[str]) -> tuple[int, str]:
    if proc.stdin is not None and not proc.stdin.closed:
        proc.stdin.close()
    try:
        proc.wait(timeout=_SUBPROCESS_TIMEOUT_SEC)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)
        stderr_text = proc.stderr.read() if proc.stderr is not None else ""
        pytest.fail(f"subprocess did not exit in time\nstderr:\n{stderr_text}")
    stdout_rest = proc.stdout.read() if proc.stdout is not None else ""
    stderr_text = proc.stderr.read() if proc.stderr is not None else ""
    if stdout_rest.strip():
        pytest.fail(
            "unexpected extra stdout after protocol sequence "
            f"(must be JSONL-only):\n{stdout_rest!r}"
        )
    return proc.returncode if proc.returncode is not None else -1, stderr_text


@pytest.mark.integration
def test_backtest_runner_wire_happy_path() -> None:
    artifact_dir = _INTEGRATION_STRATEGY_DIR
    entrypoint = "stdio_integration:IntegrationProtocolStrategy"
    proc = _start_stdio_worker(artifact_dir=artifact_dir, entrypoint=entrypoint)
    try:
        health = _exchange(
            proc,
            {
                "type": "HEALTH_CHECK",
                **_runner_envelope(1, message_id="hc-1"),
                "payload": {"nonce": "n1"},
            },
            context="HEALTH_CHECK",
        )
        assert health["type"] == "WORKER_HEALTH"
        assert health["payload"]["status"] == "UP"

        ack_portfolio = _exchange(
            proc,
            {
                "type": "PORTFOLIO_SNAPSHOT",
                **_runner_envelope(2, message_id="ps-1"),
                "payload": _sample_portfolio_payload(),
            },
            context="PORTFOLIO_SNAPSHOT",
        )
        assert ack_portfolio["type"] == "NO_OP"

        ack_orders = _exchange(
            proc,
            {
                "type": "OPEN_ORDERS_SNAPSHOT",
                **_runner_envelope(3, message_id="oo-1"),
                "payload": {"orders": []},
            },
            context="OPEN_ORDERS_SNAPSHOT",
        )
        assert ack_orders["type"] == "NO_OP"

        market = _exchange(
            proc,
            {
                "type": "MARKET_DATA_EVENT",
                **_runner_envelope(4, message_id="md-1"),
                "payload": _bar_payload(),
            },
            context="MARKET_DATA_EVENT",
        )
        assert market["type"] in ("ORDER_INTENTS", "NO_OP")
        if market["type"] == "ORDER_INTENTS":
            assert isinstance(market.get("intents"), list)
            assert len(market["intents"]) >= 1

        shutdown = _exchange(
            proc,
            {
                "type": "SHUTDOWN",
                **_runner_envelope(5, message_id="sd-1"),
                "payload": {"reason": "test"},
            },
            context="SHUTDOWN",
        )
        assert shutdown["type"] == "SHUTDOWN_ACK"

        exit_code, stderr_text = _finish_worker(proc)
        assert exit_code == 0, f"expected clean exit 0; stderr:\n{stderr_text}"
        assert (
            "backtest-runner stdio loop" in stderr_text
            or "stdio-jsonl host" in stderr_text
        )
    finally:
        if proc.poll() is None:
            proc.kill()

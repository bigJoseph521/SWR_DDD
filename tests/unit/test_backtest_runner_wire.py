"""Unit tests for backtest-runner JSONL wire codec."""

from __future__ import annotations

import json

from runtime.interface.stdio.backtest_runner.wire.runner_wire import (
    decode_runner_line,
    encode_health_response,
)
from runtime.interface.stdio.backtest_runner.wire.messages import WireEnvelope


def test_health_check_response_shape() -> None:
    frame = decode_runner_line(
        '{"type":"HEALTH_CHECK","message_id":"hc","sequence":1,'
        '"backtest_job_id":"job-1","runtime_id":"rt-1","payload":{}}'
    )
    assert frame.kind == "HEALTH_CHECK"
    line = encode_health_response(frame.envelope)
    parsed = json.loads(line.strip())
    assert parsed["type"] == "WORKER_HEALTH"
    assert parsed["payload"]["status"] == "UP"
    assert parsed["correlation_id"] == "hc"


def test_wire_envelope_from_dict() -> None:
    env = WireEnvelope.from_dict(
        {
            "sequence": 42,
            "backtest_job_id": "bt",
            "runtime_id": "rt",
            "message_id": "m1",
        }
    )
    assert env.sequence == 42
    assert env.message_id == "m1"

"""BACKTEST stdout JSONL order-intent egress."""

from __future__ import annotations

import io
import json

from runtime.interface.stdio.backtest_stdout_order_intent_writer import (
    emit_backtest_order_intent_jsonl,
)
from runtime.interface.stdio.stdout_protocol import StdoutMessageType


def test_emit_backtest_order_intent_jsonl_writes_single_line_envelope() -> None:
    buf = io.StringIO()
    emit_backtest_order_intent_jsonl(
        {"instrument_id": "AAPL", "quantity": "1", "side": "BUY"},
        output=buf,
    )
    line = buf.getvalue().strip()
    parsed = json.loads(line)
    assert parsed["type"] == StdoutMessageType.ORDER_INTENT.value
    assert parsed["payload"]["instrument_id"] == "AAPL"
    assert parsed["payload"]["quantity"] == "1"
    assert parsed["payload"]["side"] == "BUY"
    assert "\n" not in line

"""Emit BACKTEST order intents as JSONL on stdout for upstream runner consumption."""

from __future__ import annotations

import json
import sys
from collections.abc import Mapping
from typing import Any, TextIO

from runtime.interface.stdio.stdout_protocol import StdoutMessageType


def emit_backtest_order_intent_jsonl(
    payload: Mapping[str, Any],
    *,
    output: TextIO | None = None,
) -> None:
    """Write one ``ORDER_INTENT`` JSONL line to stdout (machine-readable)."""
    stream = output if output is not None else sys.stdout
    envelope = {
        "type": StdoutMessageType.ORDER_INTENT.value,
        "payload": dict(payload),
    }
    print(
        json.dumps(envelope, separators=(",", ":"), ensure_ascii=True),
        file=stream,
        flush=True,
    )

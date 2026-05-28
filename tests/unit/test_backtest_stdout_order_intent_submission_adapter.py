"""BACKTEST stdout order-intent submission adapter."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import patch

from decimal import Decimal

from runtime.domain.enums import OrderIntentSide, OrderIntentType, WorkerMode
from runtime.infrastructure.backtest.backtest_stdout_order_intent_submission_adapter import (
    BacktestStdoutOrderIntentSubmissionAdapter,
)
from runtime.domain.launch_spec import LaunchSpec
from runtime.domain.model.strategy_order_intent import StrategyOrderIntent
from runtime.infrastructure.grpc.risk_order_intent_wire_mapper import (
    OrderSubmissionContext,
)
from runtime.interface.stdio.stdout_protocol import StdoutMessageType


def _bt_spec() -> LaunchSpec:
    return LaunchSpec.from_payload(
        {
            "runtime_id": "rt-1",
            "tenant_id": "t1",
            "strategy_version_id": "sv1",
            "mode": "BACKTEST",
            "launch_attempt": 1,
            "artifact_uri": "file:///x",
            "artifact_digest": "sha256:aa",
            "entrypoint": "m:s",
            "account_id": "acct-bt-1",
            "job_id": "bt1",
            "correlation_id": "corr-bt-1",
            "ts_start": "2025-01-01T00:00:00Z",
            "ts_end": "2025-01-02T00:00:00Z",
        }
    )


def test_backtest_stdout_adapter_emits_order_intent_jsonl() -> None:
    spec = _bt_spec()
    context = OrderSubmissionContext(
        launch_spec=spec,
        correlation_id_fallback="corr-bt-1",
        allocate_order_intent_id=lambda: "oi-1",
    )
    adapter = BacktestStdoutOrderIntentSubmissionAdapter(submission_context=context)
    created = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    intent = StrategyOrderIntent(
        instrument_id="AAPL",
        side=OrderIntentSide.BUY,
        order_type=OrderIntentType.MARKET,
        quantity=Decimal("1"),
        created_at=created,
    )
    captured: list[dict[str, object]] = []

    def _capture(payload: dict[str, object], *, output=None) -> None:
        captured.append(dict(payload))

    with patch(
        "runtime.infrastructure.backtest.backtest_stdout_order_intent_submission_adapter.emit_backtest_order_intent_jsonl",
        side_effect=_capture,
    ):
        result = adapter.submit_order_intent(intent)

    assert result["accepted"] is True
    assert result["egress"] == "stdout_jsonl"
    assert len(captured) == 1
    assert captured[0]["instrument_id"] == "AAPL"
    assert captured[0]["correlation_id"] == "corr-bt-1"
    assert captured[0]["order_intent_id"] == "oi-1"
    assert captured[0]["mode"] == WorkerMode.BACKTEST.value


def test_backtest_stdout_writer_envelope_shape() -> None:
    """Integration: adapter → writer produces ORDER_INTENT JSONL envelope."""
    import io

    from runtime.interface.stdio.backtest_stdout_order_intent_writer import (
        emit_backtest_order_intent_jsonl,
    )

    buf = io.StringIO()
    emit_backtest_order_intent_jsonl({"instrument_id": "AAPL"}, output=buf)
    line = json.loads(buf.getvalue().strip())
    assert line["type"] == StdoutMessageType.ORDER_INTENT.value

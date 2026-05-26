from __future__ import annotations

import logging
from typing import Any, Mapping

from runtime.domain.errors import OrderIntentWireMappingError
from runtime.domain.model.strategy_order_intent import StrategyOrderIntent
from runtime.infrastructure.grpc.risk_order_intent_client import (
    order_intent_wire_dict_for_console,
)
from runtime.infrastructure.grpc.risk_order_intent_wire_mapper import (
    OrderSubmissionContext,
    build_risk_order_intent_wire_payload,
)
from runtime.interface.stdio.backtest_stdout_order_intent_writer import (
    emit_backtest_order_intent_jsonl,
)

_LOG = logging.getLogger(__name__)


class BacktestStdoutOrderIntentSubmissionAdapter:
    """Infrastructure adapter: BACKTEST order intents → stdout JSONL (no Risk gRPC)."""

    def __init__(
        self,
        *,
        submission_context: OrderSubmissionContext,
    ) -> None:
        self._submission_context = submission_context
        self.last_wire_payload: dict[str, Any] | None = None

    def submit_order_intent(self, intent: StrategyOrderIntent) -> Mapping[str, Any]:
        try:
            wire = build_risk_order_intent_wire_payload(
                intent,
                context=self._submission_context,
                created_at=intent.created_at,
            )
        except OrderIntentWireMappingError as exc:
            _LOG.error(
                "backtest_order_intent_wire_mapping_failed",
                extra={
                    "reason_code": exc.reason_code,
                    **exc.diagnostics,
                },
            )
            raise
        self.last_wire_payload = wire
        console_payload = order_intent_wire_dict_for_console(wire)
        emit_backtest_order_intent_jsonl(console_payload)
        return {"accepted": True, "egress": "stdout_jsonl"}

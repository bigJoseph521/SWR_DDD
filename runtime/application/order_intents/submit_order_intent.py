from __future__ import annotations

import logging
from typing import Any, Callable, Mapping

from runtime.application.order_intents.order_intent_result import OrderIntentResult
from runtime.application.ports.risk_order_intent_submission_port import (
    RiskOrderIntentSubmissionPort,
)
from runtime.domain.errors import (
    ORDER_INTENT_SUBMISSION_INTERNAL_ERROR,
    OrderIntentSubmissionError,
    OrderIntentWireMappingError,
)
from runtime.domain.model.strategy_order_intent import StrategyOrderIntent

_LOG = logging.getLogger(__name__)


def _intent_log_context(intent: StrategyOrderIntent) -> dict[str, Any]:
    ctx: dict[str, Any] = {"instrument_id": intent.instrument_id}
    if intent.client_order_id:
        ctx["client_order_id"] = intent.client_order_id
    return ctx


def _risk_rejection_result(response: Mapping[str, Any]) -> OrderIntentResult | None:
    if response.get("accepted") is not False:
        return None
    reason_code = str(response.get("reason_code") or "").strip()
    error_code = str(response.get("error_code") or "").strip()
    return OrderIntentResult(
        ok=False,
        response=response,
        error_code=error_code or "order_intent_submission_failed",
        reason_code=reason_code or "RISK_ORDER_INTENT_REJECTED",
    )


class SubmitOrderIntent:
    """
    Application use case for order intent submission to Risk Service.

    Accepts :class:`StrategyOrderIntent` only; wire enrichment happens in infrastructure.
    """

    def __init__(self, *, submission_port: RiskOrderIntentSubmissionPort) -> None:
        self._submission_port = submission_port

    def execute(
        self,
        intent: StrategyOrderIntent,
        *,
        on_journal: Callable[[dict[str, object]], None] | None = None,
    ) -> OrderIntentResult:
        log_ctx = _intent_log_context(intent)
        try:
            response = dict(self._submission_port.submit_order_intent(intent))
        except OrderIntentWireMappingError as exc:
            _LOG.error(
                "order_intent_wire_mapping_failed",
                extra={**log_ctx, **exc.diagnostics, "reason_code": exc.reason_code},
                exc_info=True,
            )
            return OrderIntentResult(
                ok=False,
                error_code="order_intent_wire_mapping_failed",
                reason_code=exc.reason_code,
            )
        except OrderIntentSubmissionError as exc:
            _LOG.error(
                "order_intent_submission_failed",
                extra={
                    **log_ctx,
                    **exc.diagnostics,
                    "error_code": exc.error_code,
                    "reason_code": exc.reason_code,
                    "retryable": exc.retryable,
                },
                exc_info=True,
            )
            return OrderIntentResult(
                ok=False,
                error_code=exc.error_code,
                reason_code=exc.reason_code,
            )
        except Exception:
            _LOG.exception(
                "order_intent_submission_internal_error",
                extra=log_ctx,
            )
            return OrderIntentResult(
                ok=False,
                error_code="order_intent_submission_failed",
                reason_code=ORDER_INTENT_SUBMISSION_INTERNAL_ERROR,
            )

        rejection = _risk_rejection_result(response)
        if rejection is not None:
            if on_journal is not None:
                on_journal({"intent": intent, "response": response})
            return rejection

        if on_journal is not None:
            on_journal({"intent": intent, "response": response})
        return OrderIntentResult(ok=True, response=response)

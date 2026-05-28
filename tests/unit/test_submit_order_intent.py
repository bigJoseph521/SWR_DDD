from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping

import pytest
from runtime.application.order_intents.submit_order_intent import SubmitOrderIntent
from runtime.domain.enums import OrderIntentSide, OrderIntentType
from runtime.domain.errors import (
    ORDER_INTENT_SUBMISSION_INTERNAL_ERROR,
    ORDER_INTENT_WIRE_MAPPING_MISSING_CORRELATION_ID,
    RISK_SERVICE_TIMEOUT,
    RISK_SERVICE_UNAVAILABLE,
    OrderIntentSubmissionError,
    OrderIntentWireMappingError,
)
from runtime.domain.model.strategy_order_intent import StrategyOrderIntent


class _FakeRiskOrderIntentSubmissionPort:
    def __init__(self) -> None:
        self.submitted: list[StrategyOrderIntent] = []

    def submit_order_intent(self, intent: StrategyOrderIntent) -> Mapping[str, Any]:
        self.submitted.append(intent)
        return {"accepted": True, "idempotency_key": intent.client_order_id}


def test_submit_order_intent_uses_port_without_infrastructure() -> None:
    port = _FakeRiskOrderIntentSubmissionPort()
    use_case = SubmitOrderIntent(submission_port=port)
    intent = StrategyOrderIntent(
        instrument_id="AAPL",
        side=OrderIntentSide.BUY,
        order_type=OrderIntentType.MARKET,
        quantity=Decimal("2"),
        client_order_id="client-1",
    )

    outcome = use_case.execute(intent)

    assert outcome.ok is True
    assert outcome.response == {"accepted": True, "idempotency_key": "client-1"}
    assert len(port.submitted) == 1
    assert port.submitted[0].instrument_id == "AAPL"


def test_submit_order_intent_preserves_normal_risk_rejection_response() -> None:
    class _RejectingPort:
        def submit_order_intent(self, intent: StrategyOrderIntent) -> Mapping[str, Any]:
            return {
                "accepted": False,
                "reason_code": "RISK_LIMIT_BREACH",
                "error_code": "POLICY_VIOLATION",
            }

    use_case = SubmitOrderIntent(submission_port=_RejectingPort())
    intent = StrategyOrderIntent(
        instrument_id="AAPL",
        side=OrderIntentSide.BUY,
        order_type=OrderIntentType.MARKET,
        quantity=Decimal("1"),
    )

    outcome = use_case.execute(intent)

    assert outcome.ok is False
    assert outcome.response == {
        "accepted": False,
        "reason_code": "RISK_LIMIT_BREACH",
        "error_code": "POLICY_VIOLATION",
    }
    assert outcome.reason_code == "RISK_LIMIT_BREACH"
    assert outcome.error_code == "POLICY_VIOLATION"


def test_submit_order_intent_maps_wire_mapping_error_to_stable_reason_code() -> None:
    class _WireMappingFailingPort:
        def submit_order_intent(self, intent: StrategyOrderIntent) -> Mapping[str, Any]:
            raise OrderIntentWireMappingError(
                reason_code=ORDER_INTENT_WIRE_MAPPING_MISSING_CORRELATION_ID,
                diagnostics={"runtime_id": "rt-1"},
            )

    use_case = SubmitOrderIntent(submission_port=_WireMappingFailingPort())
    intent = StrategyOrderIntent(
        instrument_id="AAPL",
        side=OrderIntentSide.BUY,
        order_type=OrderIntentType.MARKET,
        quantity=Decimal("1"),
    )

    outcome = use_case.execute(intent)

    assert outcome.ok is False
    assert outcome.error_code == "order_intent_wire_mapping_failed"
    assert outcome.reason_code == ORDER_INTENT_WIRE_MAPPING_MISSING_CORRELATION_ID
    assert "rt-1" not in (outcome.reason_code or "")


def test_submit_order_intent_maps_unavailable_error_to_stable_reason_code() -> None:
    class _UnavailablePort:
        def submit_order_intent(self, intent: StrategyOrderIntent) -> Mapping[str, Any]:
            raise OrderIntentSubmissionError(
                error_code="order_intent_submission_failed",
                reason_code=RISK_SERVICE_UNAVAILABLE,
                diagnostics={"runtime_id": "rt-1"},
                retryable=True,
            )

    outcome = SubmitOrderIntent(submission_port=_UnavailablePort()).execute(
        StrategyOrderIntent(
            instrument_id="AAPL",
            side=OrderIntentSide.BUY,
            order_type=OrderIntentType.MARKET,
            quantity=Decimal("1"),
        )
    )

    assert outcome.ok is False
    assert outcome.reason_code == RISK_SERVICE_UNAVAILABLE
    assert outcome.error_code == "order_intent_submission_failed"


def test_submit_order_intent_maps_timeout_error_to_stable_reason_code() -> None:
    class _TimeoutPort:
        def submit_order_intent(self, intent: StrategyOrderIntent) -> Mapping[str, Any]:
            raise OrderIntentSubmissionError(
                error_code="order_intent_submission_failed",
                reason_code=RISK_SERVICE_TIMEOUT,
                retryable=True,
            )

    outcome = SubmitOrderIntent(submission_port=_TimeoutPort()).execute(
        StrategyOrderIntent(
            instrument_id="AAPL",
            side=OrderIntentSide.BUY,
            order_type=OrderIntentType.MARKET,
            quantity=Decimal("1"),
        )
    )

    assert outcome.ok is False
    assert outcome.reason_code == RISK_SERVICE_TIMEOUT


def test_submit_order_intent_maps_generic_failure_without_raw_exception_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    emitted: list[dict[str, object]] = []

    def _capture_stdout_event(**kwargs: object) -> None:
        emitted.append(kwargs)

    monkeypatch.setattr(
        "runtime.application.order_intents.submit_order_intent.write_stdout_event",
        _capture_stdout_event,
    )

    class _FailingPort:
        def submit_order_intent(self, intent: StrategyOrderIntent) -> Mapping[str, Any]:
            raise RuntimeError("secret internal grpc stack trace detail")

    use_case = SubmitOrderIntent(submission_port=_FailingPort())
    intent = StrategyOrderIntent(
        instrument_id="AAPL",
        side=OrderIntentSide.BUY,
        order_type=OrderIntentType.MARKET,
        quantity=Decimal("1"),
    )

    outcome = use_case.execute(intent)

    assert outcome.ok is False
    assert outcome.error_code == "order_intent_submission_failed"
    assert outcome.reason_code == ORDER_INTENT_SUBMISSION_INTERNAL_ERROR
    assert "secret internal" not in (outcome.reason_code or "")
    assert len(emitted) == 1
    assert emitted[0]["event_name"] == "worker.order_intent.submission_failed"
    assert emitted[0]["reason_code"] == ORDER_INTENT_SUBMISSION_INTERNAL_ERROR
    assert "secret internal" not in str(emitted[0])

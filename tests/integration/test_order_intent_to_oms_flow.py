from __future__ import annotations

from decimal import Decimal

import pytest
from runtime.domain.enums import (
    OrderIntentSide,
    OrderIntentType,
    RuntimeMode,
)
from runtime.domain.order_intent import OrderIntent
from runtime.integration.oms_gateway import OmsGateway
from runtime.runtime.mode_policy import get_mode_policy


class _FakeOmsClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.raw_submit_calls = 0
        self.mark_filled_calls = 0

    def submit_order_intent(self, payload: dict[str, object]) -> dict[str, object]:
        self.calls.append(payload)
        return {"accepted": True, "id": payload["idempotency_key"]}

    def submit_order(self, payload: dict[str, object]) -> dict[str, object]:
        self.raw_submit_calls += 1
        return {"accepted": False, "id": payload.get("id")}

    def mark_filled(self, payload: dict[str, object]) -> dict[str, object]:
        self.mark_filled_calls += 1
        return {"accepted": False, "id": payload.get("id")}


def _intent_for_mode(mode: RuntimeMode) -> OrderIntent:
    return OrderIntent(
        idempotency_key=f"intent-{mode.value.lower()}",
        runtime_id="rt-oms-1",
        strategy_version_id="sv-1",
        mode=mode,
        instrument_id="BTC-USD",
        side=OrderIntentSide.BUY,
        order_type=OrderIntentType.LIMIT,
        quantity=Decimal("1.5"),
        limit_price=Decimal("50000"),
    )


@pytest.mark.parametrize("mode", [RuntimeMode.PAPER, RuntimeMode.LIVE])
def test_paper_live_intent_egress_calls_oms_with_normalized_intent(
    mode: RuntimeMode,
) -> None:
    client = _FakeOmsClient()
    gateway = OmsGateway(get_mode_policy(mode), client)
    intent = _intent_for_mode(mode)

    ack = gateway.submit_order_intent(intent)

    assert ack == {"accepted": True, "id": intent.idempotency_key}
    assert client.calls == [intent.to_dict()]
    assert client.raw_submit_calls == 0
    assert client.mark_filled_calls == 0


def test_backtest_can_construct_oms_gateway_for_risk_wire() -> None:
    """BACKTEST allows :class:`OmsGateway` (risk-service ``OrderIntent`` gRPC egress)."""
    gateway = OmsGateway(get_mode_policy(RuntimeMode.BACKTEST), _FakeOmsClient())
    intent = _intent_for_mode(RuntimeMode.BACKTEST)
    ack = gateway.submit_order_intent(intent)
    assert ack == {"accepted": True, "id": intent.idempotency_key}


def test_gateway_exposes_intent_only_method_semantics() -> None:
    gateway = OmsGateway(get_mode_policy(RuntimeMode.PAPER), _FakeOmsClient())
    assert callable(getattr(gateway, "submit_order_intent"))
    assert not hasattr(gateway, "submit_order")
    assert not hasattr(gateway, "mark_filled")


def test_oms_gateway_invokes_order_intent_result_callback() -> None:
    seen: list[tuple[str, dict[str, object], dict[str, object]]] = []

    def _cb(source: str, payload: dict[str, object], result: dict[str, object]) -> None:
        seen.append((source, payload, result))

    client = _FakeOmsClient()
    gateway = OmsGateway(
        get_mode_policy(RuntimeMode.PAPER), client, on_order_intent_result=_cb
    )
    intent = _intent_for_mode(RuntimeMode.PAPER)

    gateway.submit_order_intent(intent)

    assert len(seen) == 1
    assert seen[0][0] == "oms"
    assert seen[0][1] == intent.to_dict()
    assert seen[0][2] == {"accepted": True, "id": intent.idempotency_key}

    gateway.submit_order_intent_payload(
        {"runtime_id": "r1", "instrument_id": "X", "idempotency_key": "c1"}
    )

    assert len(seen) == 2
    assert seen[1][0] == "oms"
    assert seen[1][1]["runtime_id"] == "r1"

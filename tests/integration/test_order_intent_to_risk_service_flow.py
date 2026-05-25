from __future__ import annotations

from decimal import Decimal

import pytest
from runtime.domain.launch_spec import LaunchSpec
from runtime.domain.enums import (
    OrderIntentSide,
    OrderIntentType,
    WorkerMode,
)
from runtime.domain.model.strategy_order_intent import StrategyOrderIntent
from runtime.infrastructure.grpc.risk_order_intent_gateway import RiskOrderIntentGateway
from runtime.infrastructure.grpc.risk_order_intent_wire_mapper import (
    OrderSubmissionContext,
    build_risk_order_intent_wire_payload,
)
from runtime.domain.policies.mode_policy import get_mode_policy


class _FakeRiskOrderIntentClient:
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


def _launch_spec(*, mode: WorkerMode) -> LaunchSpec:
    payload: dict[str, object] = {
        "runtime_id": "rt-risk-1",
        "tenant_id": "t1",
        "strategy_version_id": "sv-1",
        "mode": mode.value,
        "launch_attempt": 1,
        "artifact_uri": "file:///x",
        "artifact_digest": "sha256:aa",
        "entrypoint": "m:s",
        "account_id": "acct-1",
        "correlation_id": "corr-risk-flow",
    }
    if mode is WorkerMode.BACKTEST:
        payload["job_id"] = "bt-1"
        payload["ts_start"] = "2025-01-01T00:00:00Z"
        payload["ts_end"] = "2025-01-02T00:00:00Z"
    return LaunchSpec.from_payload(payload)


def _wire_for_mode(mode: WorkerMode) -> dict[str, object]:
    intent = StrategyOrderIntent(
        instrument_id="BTC-USD",
        side=OrderIntentSide.BUY,
        order_type=OrderIntentType.LIMIT,
        quantity=Decimal("1.5"),
        limit_price=Decimal("50000"),
        client_order_id=f"intent-{mode.value.lower()}",
    )
    return build_risk_order_intent_wire_payload(
        intent,
        context=OrderSubmissionContext(
            launch_spec=_launch_spec(mode=mode),
            allocate_order_intent_id=lambda: f"oi-{mode.value.lower()}",
        ),
    )


@pytest.mark.parametrize("mode", [WorkerMode.PAPER, WorkerMode.LIVE])
def test_paper_live_intent_egress_calls_risk_service_with_normalized_intent(
    mode: WorkerMode,
) -> None:
    client = _FakeRiskOrderIntentClient()
    gateway = RiskOrderIntentGateway(get_mode_policy(mode), client)
    wire = _wire_for_mode(mode)

    ack = gateway.submit_order_intent_wire(wire)

    assert ack == {"accepted": True, "id": wire["idempotency_key"]}
    assert client.calls == [wire]
    assert client.raw_submit_calls == 0
    assert client.mark_filled_calls == 0


def test_backtest_can_construct_risk_order_intent_gateway() -> None:
    """BACKTEST allows Risk Service ``OrderIntent`` gRPC egress."""
    gateway = RiskOrderIntentGateway(
        get_mode_policy(WorkerMode.BACKTEST), _FakeRiskOrderIntentClient()
    )
    wire = _wire_for_mode(WorkerMode.BACKTEST)
    ack = gateway.submit_order_intent_wire(wire)
    assert ack == {"accepted": True, "id": wire["idempotency_key"]}


def test_gateway_exposes_intent_only_method_semantics() -> None:
    gateway = RiskOrderIntentGateway(
        get_mode_policy(WorkerMode.PAPER), _FakeRiskOrderIntentClient()
    )
    assert callable(getattr(gateway, "submit_order_intent_wire"))
    assert not hasattr(gateway, "submit_order")
    assert not hasattr(gateway, "mark_filled")


def test_risk_gateway_invokes_order_intent_result_callback() -> None:
    seen: list[tuple[str, dict[str, object], dict[str, object]]] = []

    def on_result(
        source: str, payload: dict[str, object], result: dict[str, object]
    ) -> None:
        seen.append((source, payload, result))

    gateway = RiskOrderIntentGateway(
        get_mode_policy(WorkerMode.PAPER),
        _FakeRiskOrderIntentClient(),
        on_order_intent_result=on_result,
    )
    wire = _wire_for_mode(WorkerMode.PAPER)
    gateway.submit_order_intent_wire(wire)

    assert len(seen) == 1
    assert seen[0][0] == "risk"

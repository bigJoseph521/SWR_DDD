from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from runtime.infrastructure.grpc.risk_order_intent_client import (
    RiskOrderIntentGrpcClient,
)
from runtime.infrastructure.grpc.serializers import risk_worker_pb2


def test_submit_cancel_order_intent_requires_core_fields() -> None:
    stub = MagicMock()
    client = RiskOrderIntentGrpcClient(stub, timeout_seconds=1.0)
    with pytest.raises(ValueError, match="correlation_id"):
        client.submit_cancel_order_intent(
            {
                "job_id": "j1",
                "account_id": "a1",
                "target_oms_order_id": "oms-1",
                "idempotency_key": "i1",
            }
        )
    with pytest.raises(ValueError, match="order_intent_id"):
        client.submit_cancel_order_intent(
            {
                "job_id": "j1",
                "account_id": "a1",
                "target_oms_order_id": "oms-1",
                "idempotency_key": "i1",
                "correlation_id": "c1",
            }
        )


def test_submit_cancel_order_intent_invokes_grpc_stub() -> None:
    captured: list[object] = []

    class _Stub:
        def SubmitCancelOrderIntent(self, request, timeout=None):
            captured.append((request, timeout))
            return risk_worker_pb2.OrderIntentAck(
                accepted=True,
                status=risk_worker_pb2.ACCEPTED,
                order_id="oms-1",
            )

    client = RiskOrderIntentGrpcClient(_Stub(), timeout_seconds=2.5)
    out = client.submit_cancel_order_intent(
        {
            "job_id": "job-1",
            "account_id": "acct-1",
            "target_oms_order_id": "oms-xyz",
            "idempotency_key": "idem-cancel-1",
            "correlation_id": "corr-1",
            "order_intent_id": "intent-9",
            "mode": "PAPER",
        }
    )
    assert out["accepted"] is True
    assert len(captured) == 1
    req, timeout = captured[0]
    assert timeout == 2.5
    assert isinstance(req, risk_worker_pb2.CancelOrderIntent)
    assert req.job_id == "job-1"
    assert req.account_id == "acct-1"
    assert req.target_oms_order_id == "oms-xyz"
    assert req.idempotency_key == "idem-cancel-1"
    assert req.correlation_id == "corr-1"
    assert req.order_intent_id == "intent-9"

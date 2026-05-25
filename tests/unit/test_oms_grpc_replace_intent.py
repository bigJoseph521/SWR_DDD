from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from runtime.transport.grpc.oms_client import OmsGrpcClient
from runtime.transport.grpc.serializers import risk_worker_pb2


def test_submit_replace_order_intent_requires_core_fields() -> None:
    stub = MagicMock()
    client = OmsGrpcClient(stub, timeout_seconds=1.0)
    with pytest.raises(ValueError, match="job_id"):
        client.submit_replace_order_intent(
            {
                "account_id": "a1",
                "target_oms_order_id": "oms-1",
                "idempotency_key": "i1",
                "order_intent_id": "1",
                "quantity": "1",
                "order_type": "",
            }
        )


def test_submit_replace_order_intent_rejects_patch_rules() -> None:
    stub = MagicMock()
    client = OmsGrpcClient(stub, timeout_seconds=1.0)
    with pytest.raises(ValueError, match="exactly one of quantity or order_type"):
        client.submit_replace_order_intent(
            {
                "job_id": "j1",
                "account_id": "a1",
                "target_oms_order_id": "oms-1",
                "idempotency_key": "i1",
                "order_intent_id": "1",
                "quantity": "",
                "order_type": "",
            }
        )
    with pytest.raises(ValueError, match="exactly one of quantity or order_type"):
        client.submit_replace_order_intent(
            {
                "job_id": "j1",
                "account_id": "a1",
                "target_oms_order_id": "oms-1",
                "idempotency_key": "i1",
                "order_intent_id": "1",
                "quantity": "1",
                "order_type": "limit",
            }
        )


def test_submit_replace_order_intent_invokes_grpc_stub() -> None:
    captured: list[object] = []

    class _Stub:
        def SubmitReplaceOrderIntent(self, request, timeout=None):
            captured.append((request, timeout))
            return risk_worker_pb2.OrderIntentAck(
                accepted=True,
                status=risk_worker_pb2.ACCEPTED,
                order_id="oms-1",
            )

    client = OmsGrpcClient(_Stub(), timeout_seconds=2.5)
    out = client.submit_replace_order_intent(
        {
            "job_id": "job-1",
            "account_id": "acct-1",
            "target_oms_order_id": "oms-xyz",
            "idempotency_key": "idem-rep-1",
            "correlation_id": "corr-1",
            "order_intent_id": "intent-9",
            "quantity": "2",
            "order_type": "",
            "mode": "PAPER",
        }
    )
    assert out["accepted"] is True
    assert len(captured) == 1
    req, timeout = captured[0]
    assert timeout == 2.5
    assert isinstance(req, risk_worker_pb2.ReplaceOrderIntent)
    assert req.job_id == "job-1"
    assert req.account_id == "acct-1"
    assert req.target_oms_order_id == "oms-xyz"
    assert req.idempotency_key == "idem-rep-1"
    assert req.correlation_id == "corr-1"
    assert req.order_intent_id == "intent-9"
    assert req.quantity == "2"
    assert req.order_type == ""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest
from runtime.domain.enums import (
    OrderIntentSide,
    OrderIntentType,
    RuntimeMode,
    WorkerPhase,
)
from runtime.domain.errors import (
    InvalidLaunchContextError,
    OrderIntentValidationError,
    WorkerErrorCode,
    WorkerInternalError,
    get_machine_error_code,
)
from runtime.domain.launch_context import LaunchContext
from runtime.domain.models import RuntimeStateSnapshot
from runtime.domain.order_intent import OrderIntent


def _valid_launch_context_kwargs() -> dict[str, Any]:
    return {
        "runtime_id": "rt-101",
        "tenant_id": "tenant-3",
        "account_id": "acct-9",
        "strategy_version_id": "sv-2026-03-20",
        "mode": RuntimeMode.PAPER,
        "parameter_hash": "sha256:paramhash",
        "artifact_uri": "registry://strategies/sv-2026-03-20",
        "artifact_reference": "registry://strategies/sv-2026-03-20",
        "artifact_digest": "sha256:artifacthash",
        "entrypoint": "strategy.main:run",
        "launch_attempt": 2,
        "correlation_id": "corr-10",
    }


def _valid_intent_kwargs() -> dict[str, Any]:
    return {
        "idempotency_key": "intent-1",
        "runtime_id": "rt-101",
        "strategy_version_id": "sv-2026-03-20",
        "mode": RuntimeMode.PAPER,
        "instrument_id": "BTC-USD",
        "side": OrderIntentSide.BUY,
        "order_type": OrderIntentType.MARKET,
        "quantity": Decimal("1.25"),
    }


def test_launch_context_validation_happy_path() -> None:
    context = LaunchContext(**_valid_launch_context_kwargs())
    assert context.runtime_id == "rt-101"
    assert context.mode is RuntimeMode.PAPER


def test_launch_context_accepts_blank_tenant_id() -> None:
    kwargs = _valid_launch_context_kwargs()
    kwargs["tenant_id"] = "  "
    context = LaunchContext(**kwargs)
    assert context.tenant_id == ""
    assert context.to_worker_identity().tenant_id == ""


def test_launch_context_to_worker_identity_conversion() -> None:
    context = LaunchContext(**_valid_launch_context_kwargs())
    identity = context.to_worker_identity()
    assert identity.runtime_id == context.runtime_id
    assert identity.account_id == context.account_id
    assert identity.parameter_hash == context.parameter_hash
    assert identity.mode is context.mode


def test_launch_context_rejects_job_id_for_paper_live_modes() -> None:
    with pytest.raises(InvalidLaunchContextError):
        LaunchContext(**_valid_launch_context_kwargs(), job_id="job-1")


def test_serialization_stability_for_models() -> None:
    context = LaunchContext(**_valid_launch_context_kwargs())
    identity = context.to_worker_identity()
    intent = OrderIntent(**_valid_intent_kwargs())

    now = datetime.now(timezone.utc)
    snapshot = RuntimeStateSnapshot(
        identity=identity,
        phase=WorkerPhase.RUNNING,
        reason_code=None,
        created_at=now,
        updated_at=now + timedelta(seconds=1),
        last_heartbeat_at=now + timedelta(seconds=1),
    )

    context_dict = context.to_dict()
    identity_dict = identity.to_dict()
    intent_dict = intent.to_dict()
    snapshot_dict = snapshot.to_dict()

    assert context_dict["runtime_id"] == "rt-101"
    assert identity_dict["runtime_id"] == "rt-101"
    assert intent_dict["quantity"] == "1.25"
    assert snapshot_dict["phase"] == "RUNNING"
    assert snapshot_dict["identity"] == identity_dict


def test_shared_vs_local_error_mapping_behavior() -> None:
    local = OrderIntentValidationError(field_name="quantity", reason="must_be_positive")
    shared_style = WorkerInternalError(reason="unexpected")
    assert get_machine_error_code(local) == "WORKER_DOMAIN_ORDER_INTENT_VALIDATION"
    assert get_machine_error_code(shared_style) == WorkerErrorCode.INTERNAL_ERROR.value


def test_valid_market_order_intent() -> None:
    intent = OrderIntent(**_valid_intent_kwargs())
    assert intent.order_type is OrderIntentType.MARKET
    assert intent.limit_price is None
    assert intent.stop_price is None


def test_valid_limit_order_intent() -> None:
    kwargs = _valid_intent_kwargs()
    kwargs["order_type"] = OrderIntentType.LIMIT
    kwargs["limit_price"] = Decimal("100.50")
    intent = OrderIntent(**kwargs)
    assert intent.limit_price == Decimal("100.50")


@pytest.mark.parametrize("qty", [Decimal("0"), Decimal("-1")])
def test_invalid_non_positive_quantity(qty: Decimal) -> None:
    kwargs = _valid_intent_kwargs()
    kwargs["quantity"] = qty
    with pytest.raises(OrderIntentValidationError):
        OrderIntent(**kwargs)


def test_invalid_missing_limit_price_for_limit_order() -> None:
    kwargs = _valid_intent_kwargs()
    kwargs["order_type"] = OrderIntentType.LIMIT
    with pytest.raises(OrderIntentValidationError):
        OrderIntent(**kwargs)


def test_invalid_missing_stop_price_for_stop_order() -> None:
    kwargs = _valid_intent_kwargs()
    kwargs["order_type"] = OrderIntentType.STOP
    with pytest.raises(OrderIntentValidationError):
        OrderIntent(**kwargs)


def test_invalid_missing_prices_for_stop_limit_order() -> None:
    kwargs = _valid_intent_kwargs()
    kwargs["order_type"] = OrderIntentType.STOP_LIMIT
    with pytest.raises(OrderIntentValidationError):
        OrderIntent(**kwargs)


def test_backtest_mode_allowed_for_risk_wire_path() -> None:
    kwargs = _valid_intent_kwargs()
    kwargs["mode"] = RuntimeMode.BACKTEST
    intent = OrderIntent(**kwargs)
    intent.validate_for_oms_submission()

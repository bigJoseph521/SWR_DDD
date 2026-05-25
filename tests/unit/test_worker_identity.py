from __future__ import annotations

from dataclasses import FrozenInstanceError
from typing import Any

import pytest
from runtime.domain.enums import RuntimeMode
from runtime.domain.errors import (
    MalformedWorkerIdentityError,
    SingleAssignmentViolationError,
)
from runtime.domain.worker_identity import WorkerIdentity


def _base_identity_kwargs() -> dict[str, Any]:
    return {
        "runtime_id": "rt-100",
        "tenant_id": "tenant-1",
        "strategy_version_id": "strategy-v5",
        "mode": RuntimeMode.PAPER,
        "validated_parameter_identity": "vp-123",
        "artifact_reference": "registry://strategy/v5",
        "artifact_digest": "sha256:abcdef",
        "entrypoint": "strategy.main:run",
        "launch_attempt": 1,
    }


def test_valid_identity_with_trader_id() -> None:
    identity = WorkerIdentity(**_base_identity_kwargs(), trader_id="trader-1")
    assert identity.trader_id == "trader-1"
    assert identity.account_id is None


def test_accepts_blank_tenant_id_when_user_scoped() -> None:
    kwargs = _base_identity_kwargs()
    kwargs["tenant_id"] = "   "
    identity = WorkerIdentity(**kwargs, trader_id="trader-1")
    assert identity.tenant_id == ""


def test_valid_identity_with_account_id() -> None:
    identity = WorkerIdentity(**_base_identity_kwargs(), account_id="account-1")
    assert identity.account_id == "account-1"
    assert identity.trader_id is None


def test_invalid_when_both_scope_fields_present() -> None:
    with pytest.raises(SingleAssignmentViolationError):
        WorkerIdentity(
            **_base_identity_kwargs(),
            trader_id="trader-1",
            account_id="account-1",
        )


def test_invalid_when_no_scope_field_present() -> None:
    with pytest.raises(SingleAssignmentViolationError):
        WorkerIdentity(**_base_identity_kwargs())


def test_valid_when_validated_parameter_identity_omitted() -> None:
    kwargs = _base_identity_kwargs()
    kwargs.pop("validated_parameter_identity")
    identity = WorkerIdentity(**kwargs, trader_id="trader-1")
    assert identity.validated_parameter_identity is None


@pytest.mark.parametrize("field_name", ["runtime_id", "strategy_version_id"])
def test_invalid_when_required_identity_field_blank(field_name: str) -> None:
    kwargs = _base_identity_kwargs()
    kwargs[field_name] = "  "
    with pytest.raises(MalformedWorkerIdentityError):
        WorkerIdentity(**kwargs, trader_id="trader-1")


@pytest.mark.parametrize("launch_attempt", [0, -1])
def test_invalid_launch_attempt(launch_attempt: int) -> None:
    kwargs = _base_identity_kwargs()
    kwargs["launch_attempt"] = launch_attempt
    with pytest.raises(MalformedWorkerIdentityError):
        WorkerIdentity(
            **kwargs,
            trader_id="trader-1",
        )


def test_identity_is_immutable() -> None:
    identity = WorkerIdentity(**_base_identity_kwargs(), trader_id="trader-1")
    with pytest.raises(FrozenInstanceError):
        identity.runtime_id = "rt-other"  # type: ignore[misc]


def test_serialization_round_trip_stability() -> None:
    identity = WorkerIdentity(**_base_identity_kwargs(), trader_id="trader-1")
    as_dict = identity.to_dict()
    restored = WorkerIdentity(**as_dict)
    assert restored == identity
    assert hash(restored) == hash(identity)
    assert restored.to_dict() == as_dict

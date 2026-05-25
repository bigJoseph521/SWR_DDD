from __future__ import annotations

import pytest
from runtime.bootstrap.validator import LaunchSpecValidator
from runtime.domain.enums import WorkerMode
from runtime.domain.errors import (
    RuntimeStartValidationFailedError,
    SharedBoundaryErrorCode,
)


def _valid_payload() -> dict[str, object]:
    return {
        "runtime_id": "rt-1",
        "tenant_id": "tenant-1",
        "account_id": "acct-1",
        "strategy_version_id": "sv-1",
        "mode": "PAPER",
        "validated_parameter_identity": "vp-1",
        "artifact_reference": "registry://strategies/sv-1",
        "artifact_digest": "sha256:abc",
        "entrypoint": "strategy.main:run",
        "launch_attempt": 1,
        "correlation_id": "corr-1",
    }


def test_validate_happy_path_returns_launch_spec() -> None:
    validator = LaunchSpecValidator()
    spec = validator.validate(_valid_payload())
    assert spec.runtime_id == "rt-1"
    assert spec.mode is WorkerMode.PAPER


def test_validation_errors_map_to_runtime_start_validation_failed() -> None:
    validator = LaunchSpecValidator()
    payload = _valid_payload()
    payload.pop("runtime_id")

    with pytest.raises(RuntimeStartValidationFailedError) as exc_info:
        validator.validate(payload)

    err = exc_info.value
    assert err.code == SharedBoundaryErrorCode.RUNTIME_START_VALIDATION_FAILED
    assert err.message == "Runtime start validation failed."
    assert err.details["reason"] == "launch_spec_invalid"
    assert err.details["field_errors"]["runtime_id"] == "required_field_missing"


def test_exact_duplicate_payload_for_same_key_is_idempotent_success() -> None:
    validator = LaunchSpecValidator()
    payload = _valid_payload()

    first = validator.validate(payload)
    second = validator.validate(dict(payload))

    assert first == second
    assert first.runtime_id == second.runtime_id
    assert first.launch_attempt == second.launch_attempt


def test_duplicate_with_same_content_but_different_key_order_is_idempotent() -> None:
    validator = LaunchSpecValidator()
    payload = _valid_payload()
    reordered_payload = {key: payload[key] for key in reversed(tuple(payload.keys()))}

    validator.validate(payload)
    spec = validator.validate(reordered_payload)

    assert spec.runtime_id == "rt-1"
    assert spec.launch_attempt == 1


def test_different_payload_for_same_dedupe_key_is_rejected() -> None:
    validator = LaunchSpecValidator()
    payload = _valid_payload()
    validator.validate(payload)

    broadened = dict(payload)
    broadened["correlation_id"] = "corr-broadened"

    with pytest.raises(RuntimeStartValidationFailedError) as exc_info:
        validator.validate(broadened)

    err = exc_info.value
    assert err.code == SharedBoundaryErrorCode.RUNTIME_START_VALIDATION_FAILED
    assert err.details["reason"] == "duplicate_key_payload_mismatch"
    assert (
        err.details["field_errors"]["payload"]
        == "payload_broadening_or_mutation_for_existing_runtime_launch_attempt"
    )
    assert err.details["runtime_id"] == "rt-1"
    assert err.details["launch_attempt"] == 1


def test_same_runtime_id_with_new_launch_attempt_is_allowed() -> None:
    validator = LaunchSpecValidator()
    first = _valid_payload()
    second = _valid_payload()
    second["launch_attempt"] = 2

    validator.validate(first)
    spec = validator.validate(second)
    assert spec.launch_attempt == 2

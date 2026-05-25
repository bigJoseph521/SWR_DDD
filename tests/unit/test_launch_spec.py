from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest
from runtime.infrastructure.strategy_loader.digest_validation_env import (
    SKIP_ARTIFACT_DIGEST_VALIDATION_ENV,
)
from runtime.bootstrap.launch_spec import (
    LaunchSpec,
    LaunchSpecValidationError,
)
from runtime.domain.enums import WorkerMode


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


def test_launch_spec_happy_path() -> None:
    spec = LaunchSpec.from_payload(_valid_payload())
    assert spec.runtime_id == "rt-1"
    assert spec.mode is WorkerMode.PAPER
    assert spec.account_id == "acct-1"
    assert spec.trader_id is None
    assert spec.correlation_id == "corr-1"


def test_launch_spec_is_immutable() -> None:
    spec = LaunchSpec.from_payload(_valid_payload())
    with pytest.raises(FrozenInstanceError):
        spec.runtime_id = "rt-2"  # type: ignore[misc]


@pytest.mark.parametrize("missing_field", sorted(LaunchSpec._REQUIRED_FIELDS))
def test_rejects_missing_required_field(missing_field: str) -> None:
    payload = _valid_payload()
    payload.pop(missing_field)
    with pytest.raises(LaunchSpecValidationError) as exc_info:
        LaunchSpec.from_payload(payload)
    assert exc_info.value.reason == "launch_spec_invalid"
    assert exc_info.value.field_errors[missing_field] == "required_field_missing"


@pytest.mark.parametrize(
    ("field_name", "value", "expected_error"),
    [
        ("runtime_id", 100, "wrong_type_expected_string"),
        ("strategy_version_id", "  sv-1  ", "must_not_have_surrounding_whitespace"),
        ("artifact_reference", 10, "wrong_type_expected_string"),
        ("entrypoint", "", "must_not_be_empty"),
        ("launch_attempt", "1", "wrong_type_expected_integer"),
        ("launch_attempt", 0, "must_be_greater_or_equal_to_1"),
        ("correlation_id", 1, "wrong_type_expected_string"),
    ],
)
def test_rejects_wrong_types_and_invalid_scalars(
    field_name: str, value: object, expected_error: str
) -> None:
    payload = _valid_payload()
    payload[field_name] = value
    with pytest.raises(LaunchSpecValidationError) as exc_info:
        LaunchSpec.from_payload(payload)
    assert exc_info.value.field_errors[field_name] == expected_error


def test_rejects_invalid_mode() -> None:
    payload = _valid_payload()
    payload["mode"] = "SIMULATED"
    with pytest.raises(LaunchSpecValidationError) as exc_info:
        LaunchSpec.from_payload(payload)
    assert exc_info.value.field_errors["mode"] == "invalid_mode:SIMULATED"


def test_accepts_trader_id_alongside_account_id() -> None:
    payload = _valid_payload()
    payload["trader_id"] = "trader-1"
    spec = LaunchSpec.from_payload(payload)
    assert spec.account_id == "acct-1"
    assert spec.trader_id == "trader-1"


def test_rejects_missing_account_id() -> None:
    payload = _valid_payload()
    payload.pop("account_id")
    with pytest.raises(LaunchSpecValidationError) as exc_info:
        LaunchSpec.from_payload(payload)
    assert exc_info.value.field_errors["account_id"] == "required_field_missing"


def test_accepts_missing_tenant_id_as_user_scoped() -> None:
    payload = _valid_payload()
    payload.pop("tenant_id")
    spec = LaunchSpec.from_payload(payload)
    assert spec.tenant_id == ""


def test_rejects_unknown_fields() -> None:
    payload = _valid_payload()
    payload["unexpected_field"] = "registry://unexpected"
    with pytest.raises(LaunchSpecValidationError) as exc_info:
        LaunchSpec.from_payload(payload)
    assert exc_info.value.field_errors["payload"] == "unknown_fields:unexpected_field"


def test_accepts_strategy_params_dict() -> None:
    payload = _valid_payload()
    payload["strategy_params"] = {"lookback": 20}
    spec = LaunchSpec.from_payload(payload)
    assert spec.runtime_id == "rt-1"


def test_rejects_strategy_params_non_object() -> None:
    payload = _valid_payload()
    payload["strategy_params"] = "not-a-dict"
    with pytest.raises(LaunchSpecValidationError) as exc_info:
        LaunchSpec.from_payload(payload)
    assert (
        exc_info.value.field_errors.get("strategy_params")
        == "wrong_type_expected_object_or_null"
    )


def test_accepts_artifact_uri_alias_when_artifact_reference_missing() -> None:
    payload = _valid_payload()
    payload.pop("artifact_reference")
    payload["artifact_uri"] = "registry://strategies/sv-1"
    spec = LaunchSpec.from_payload(payload)
    assert spec.artifact_reference == "registry://strategies/sv-1"
    assert spec.artifact_uri == "registry://strategies/sv-1"


def test_rejects_unknown_backtest_job_id_field_for_non_backtest_mode() -> None:
    payload = _valid_payload()
    payload["backtest_job_id"] = "bt-1"
    with pytest.raises(LaunchSpecValidationError) as exc_info:
        LaunchSpec.from_payload(payload)
    assert "backtest_job_id" in str(exc_info.value.field_errors.get("payload", ""))


def test_accepts_optional_job_id_for_paper_mode() -> None:
    spec = LaunchSpec.from_payload(
        {
            "runtime_id": "rt-1",
            "tenant_id": "tenant-1",
            "account_id": "acct-1",
            "strategy_version_id": "sv-1",
            "mode": "PAPER",
            "launch_attempt": 1,
            "artifact_uri": "registry://strategies/sv-1",
            "artifact_digest": "sha256:abc",
            "entrypoint": "strategy.main:run",
            "job_id": "job-1",
        }
    )
    assert spec.job_id == "job-1"


def test_accepts_optional_symbol_from_launch_payload() -> None:
    spec = LaunchSpec.from_payload(
        {
            "runtime_id": "rt-1",
            "tenant_id": "tenant-1",
            "account_id": "acct-1",
            "strategy_version_id": "sv-1",
            "mode": "PAPER",
            "launch_attempt": 1,
            "artifact_uri": "registry://strategies/sv-1",
            "artifact_digest": "sha256:abc",
            "entrypoint": "strategy.main:run",
            "symbol": "AAPL",
        }
    )
    assert spec.symbol == "AAPL"


def test_accepts_symbol_from_parameters_nested_like_setting_json() -> None:
    """``setting.json`` nests the ticker under ``parameters.symbol``."""
    spec = LaunchSpec.from_payload(
        {
            "runtime_id": "rt-1",
            "tenant_id": "tenant-1",
            "account_id": "acct-1",
            "strategy_version_id": "sv-1",
            "mode": "PAPER",
            "launch_attempt": 1,
            "artifact_uri": "registry://strategies/sv-1",
            "artifact_digest": "sha256:abc",
            "entrypoint": "strategy.main:run",
            "parameters": {"symbol": "AAPL", "bar_timeframe": "1m"},
        }
    )
    assert spec.symbol == "AAPL"


def test_accepts_initial_cash_from_sds_runtime_context() -> None:
    spec = LaunchSpec.from_payload(
        {
            **_valid_payload(),
            "initial_cash": {"amount": "900.00", "currency": "USD"},
        }
    )
    assert spec.runtime_id == "rt-1"


def test_rejects_non_object_initial_cash() -> None:
    payload = _valid_payload()
    payload["initial_cash"] = "900"
    with pytest.raises(LaunchSpecValidationError) as exc_info:
        LaunchSpec.from_payload(payload)
    assert (
        exc_info.value.field_errors["initial_cash"]
        == "wrong_type_expected_object_or_null"
    )


def test_skip_digest_env_allows_missing_artifact_digest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(SKIP_ARTIFACT_DIGEST_VALIDATION_ENV, "1")
    payload = _valid_payload()
    payload.pop("artifact_digest")
    spec = LaunchSpec.from_payload(payload)
    assert spec.artifact_digest is None


def test_skip_digest_env_allows_blank_artifact_digest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(SKIP_ARTIFACT_DIGEST_VALIDATION_ENV, "1")
    payload = _valid_payload()
    payload["artifact_digest"] = "   "
    spec = LaunchSpec.from_payload(payload)
    assert spec.artifact_digest is None


def test_skip_digest_env_rejects_non_string_artifact_digest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(SKIP_ARTIFACT_DIGEST_VALIDATION_ENV, "1")
    payload = _valid_payload()
    payload["artifact_digest"] = 18
    with pytest.raises(LaunchSpecValidationError) as exc_info:
        LaunchSpec.from_payload(payload)
    assert (
        exc_info.value.field_errors["artifact_digest"] == "wrong_type_expected_string"
    )

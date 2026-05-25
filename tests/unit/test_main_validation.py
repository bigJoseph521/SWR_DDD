from __future__ import annotations

import importlib
import sys
import types
from typing import Any

import pytest
from runtime.domain.launch_spec import LaunchSpecValidationError
from runtime.domain.errors import (
    RuntimeWorkerReasonCode,
    WorkerErrorCode,
)


_STUBBED_MODULE_KEYS = (
    "runtime.bootstrap.dependency_container",
    "runtime.infrastructure.http.srm.manager_client",
    "runtime.infrastructure.grpc.risk_order_intent_client",
    "runtime.interface.cli.main",
)


def _clear_import_stubs() -> None:
    for key in _STUBBED_MODULE_KEYS:
        mod = sys.modules.get(key)
        if isinstance(mod, types.ModuleType) and getattr(mod, "__file__", None) is None:
            sys.modules.pop(key, None)


@pytest.fixture(autouse=True)
def _restore_modules_after_main_validation_stubs() -> None:
    yield
    _clear_import_stubs()


def _import_main_with_stubs() -> Any:
    dep_mod = types.ModuleType("runtime.bootstrap.dependency_container")
    setattr(dep_mod, "build_dependency_container", lambda *args, **kwargs: None)
    sys.modules["runtime.bootstrap.dependency_container"] = dep_mod

    class _NoopManagerClient:  # pragma: no cover - import stub only
        def emit_signal(self, envelope: object) -> dict[str, object]:
            return {"accepted": False}

        def close(self) -> None:
            return None

    manager_client_mod = types.ModuleType("runtime.infrastructure.http.srm.manager_client")
    setattr(
        manager_client_mod,
        "build_manager_client",
        lambda **_k: _NoopManagerClient(),
    )
    setattr(manager_client_mod, "CompositeManagerSignalClient", _NoopManagerClient)
    setattr(manager_client_mod, "SrmHeartbeatHttpClient", _NoopManagerClient)
    sys.modules["runtime.infrastructure.http.srm.manager_client"] = manager_client_mod

    risk_mod = types.ModuleType(
        "runtime.infrastructure.grpc.risk_order_intent_client"
    )
    setattr(
        risk_mod,
        "build_risk_order_intent_grpc_client",
        lambda **kwargs: None,
    )
    sys.modules["runtime.infrastructure.grpc.risk_order_intent_client"] = risk_mod

    sys.modules.pop("runtime.interface.cli.main", None)
    try:
        return importlib.import_module("runtime.interface.cli.main")
    except Exception:
        _clear_import_stubs()
        raise


class _RecordingManagerClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def emit_signal(self, payload: dict[str, object]) -> dict[str, object]:
        self.calls.append(payload)
        return {"accepted": True}


def test_emit_validation_failure_to_manager_uses_bootstrap_failed_shape(
    monkeypatch,
) -> None:
    client = _RecordingManagerClient()
    exc = LaunchSpecValidationError(
        reason="launch_spec_invalid",
        field_errors={"runtime_id": "required_field_missing"},
    )
    from runtime.bootstrap import runtime_entrypoint

    runtime_entrypoint.emit_validation_failure_to_manager(
        manager_client=client,
        exc=exc,
        identity=runtime_entrypoint._identity_from_raw_bundle(
            {
                "runtime_id": "rt-1",
                "tenant_id": "tenant-1",
                "account_id": "acct-1",
                "strategy_version_id": "sv-1",
                "launch_attempt": 1,
            }
        ),
    )

    assert len(client.calls) == 1
    envelope = client.calls[0]
    assert envelope["signal_type"] == "bootstrap_failed"
    payload = envelope["payload"]
    assert isinstance(payload, dict)
    assert (
        payload["reason_code"]
        == RuntimeWorkerReasonCode.BOOTSTRAP_METADATA_INCONSISTENT.value
    )
    assert payload["error_code"] == WorkerErrorCode.BOOTSTRAP_FAILED.value
    assert "missed_fields" in payload
    assert "invalid_fields" in payload
    assert "empty_fields" in payload
    assert payload["passed"] == []
    assert "validate_launch_metadata" in payload["failed"]
    assert "artifact_fetch" in payload["not_checked"]
    details = payload["details"]
    assert isinstance(details, dict)
    assert details["reason"] == "launch_spec_invalid"


def test_emit_validation_failure_to_manager_derives_field_buckets() -> None:
    client = _RecordingManagerClient()
    exc = LaunchSpecValidationError(
        reason="launch_spec_invalid",
        field_errors={
            "runtime_id": "required_field_missing",
            "mode": "required_field_missing",
            "trader_id": "must_not_be_empty",
            "scope": "exactly_one_of_trader_id_or_account_id_required",
            "payload": "unknown_fields:unexpected_a,unexpected_b",
        },
    )

    from runtime.bootstrap import runtime_entrypoint

    runtime_entrypoint.emit_validation_failure_to_manager(
        manager_client=client,
        exc=exc,
        identity={},
    )

    payload = client.calls[0]["payload"]
    assert isinstance(payload, dict)
    assert payload["missed_fields"] == ["mode", "runtime_id", "scope"]
    assert payload["empty_fields"] == ["trader_id"]
    assert payload["invalid_fields"] == ["unexpected_a", "unexpected_b"]


def test_main_emits_bootstrap_failed_when_load_settings_fails(monkeypatch) -> None:
    from types import SimpleNamespace

    from runtime.bootstrap import runtime_entrypoint
    from runtime.bootstrap.minimal_env_validation import MinimalEnvValidationResult

    monkeypatch.setenv("RUNTIME_ID", "rt-main-val")
    monkeypatch.setenv("STRATEGY_RUNTIME_MANAGER_BASE_URL", "http://127.0.0.1:8080")
    _import_main_with_stubs()
    client = _RecordingManagerClient()
    monkeypatch.setattr(
        runtime_entrypoint, "build_manager_client", lambda **_k: client
    )
    monkeypatch.setattr(
        runtime_entrypoint,
        "load_settings",
        lambda: (_ for _ in ()).throw(
            LaunchSpecValidationError(
                reason="launch_spec_invalid",
                field_errors={"mode": "required_field_missing"},
            )
        ),
    )
    monkeypatch.setattr(
        runtime_entrypoint,
        "validate_minimal_env_from_environ",
        lambda: MinimalEnvValidationResult(
            valid=True,
            field_errors={},
            message="ok",
            snapshot=SimpleNamespace(
                runtime_id="rt-main-val",
                srm_base_url="http://127.0.0.1:8080",
                deployment_id="",
                mode="PAPER",
            ),
        ),
    )
    monkeypatch.setattr(
        runtime_entrypoint, "report_minimal_env_validation_to_srm", lambda _r: None
    )
    monkeypatch.setattr(
        runtime_entrypoint, "print_minimal_env_validation_outcome", lambda *_a, **_k: None
    )

    with pytest.raises(SystemExit) as exc_info:
        runtime_entrypoint.run_runtime_from_cli()

    assert exc_info.value.code == 2
    assert len(client.calls) == 1
    assert client.calls[0]["signal_type"] == "bootstrap_failed"

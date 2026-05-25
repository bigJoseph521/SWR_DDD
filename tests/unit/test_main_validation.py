from __future__ import annotations

import importlib
import sys
import types
from typing import Any

import pytest
from runtime.bootstrap.launch_spec import LaunchSpecValidationError
from runtime.domain.errors import (
    RuntimeWorkerReasonCode,
    WorkerErrorCode,
)


def _import_main_with_stubs() -> Any:
    dep_mod = types.ModuleType("runtime.application.dependency_container")
    setattr(dep_mod, "build_dependency_container", lambda *args, **kwargs: None)
    sys.modules["runtime.application.dependency_container"] = dep_mod

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

    replay_service_mod = types.ModuleType("runtime.interface.grpc.replay_ingress_server")

    class _ReplayIngressServerRuntime:  # pragma: no cover - import stub only
        bind_address = "127.0.0.1:0"
        port = 0

        def start(self) -> None:
            return None

        def stop(self, grace_seconds: float = 0.0) -> None:
            _ = grace_seconds

    setattr(
        replay_service_mod, "ReplayIngressServerRuntime", _ReplayIngressServerRuntime
    )
    setattr(
        replay_service_mod,
        "build_replay_ingress_server_first_available",
        lambda *args, **kwargs: _ReplayIngressServerRuntime(),
    )
    sys.modules["runtime.interface.grpc.replay_ingress_server"] = replay_service_mod

    sys.modules.pop("runtime.interface.cli.main", None)
    return importlib.import_module("runtime.interface.cli.main")


class _RecordingManagerClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def emit_signal(self, payload: dict[str, object]) -> dict[str, object]:
        self.calls.append(payload)
        return {"accepted": True}


def test_emit_validation_failure_to_manager_uses_bootstrap_failed_shape(
    monkeypatch,
) -> None:
    main_module = _import_main_with_stubs()
    client = _RecordingManagerClient()
    exc = LaunchSpecValidationError(
        reason="launch_spec_invalid",
        field_errors={"runtime_id": "required_field_missing"},
    )
    main_module._emit_validation_failure_to_manager(
        manager_client=client,
        exc=exc,
        identity=main_module._identity_from_raw_bundle(
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
    main_module = _import_main_with_stubs()
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

    main_module._emit_validation_failure_to_manager(
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
    monkeypatch.setenv("RUNTIME_ID", "rt-main-val")
    monkeypatch.setenv("STRATEGY_RUNTIME_MANAGER_BASE_URL", "http://127.0.0.1:8080")
    main_module = _import_main_with_stubs()
    client = _RecordingManagerClient()
    monkeypatch.setattr(main_module, "build_manager_client", lambda **kwargs: client)
    monkeypatch.setattr(
        main_module,
        "load_settings",
        lambda: (_ for _ in ()).throw(
            LaunchSpecValidationError(
                reason="launch_spec_invalid",
                field_errors={"mode": "required_field_missing"},
            )
        ),
    )

    with pytest.raises(SystemExit) as exc_info:
        main_module.main()

    assert exc_info.value.code == 2
    assert len(client.calls) == 1
    assert client.calls[0]["signal_type"] == "bootstrap_failed"

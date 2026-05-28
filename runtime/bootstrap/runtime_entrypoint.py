"""CLI/runtime process entry: load settings, wire container, run worker lifecycle."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from runtime.application.cooperative_shutdown import (
    CooperativeShutdownCoordinator,
    install_sigterm_handler,
)
from runtime.domain.launch_spec import LaunchSpecValidationError
from runtime.bootstrap.minimal_env_validation import validate_minimal_env_from_environ
from runtime.bootstrap.runtime_composition import (
    build_runtime_container,
    build_runtime_outbound_clients,
)
from runtime.bootstrap.srm_env_status_report import (
    is_runtime_context_fetch_failure,
    print_minimal_env_validation_outcome,
    print_runtime_context_fetch_failure_outcome,
    report_minimal_env_validation_to_srm,
    report_runtime_context_fetch_failure_to_srm,
)
from runtime.domain.errors import RuntimeWorkerReasonCode, WorkerErrorCode
from runtime.domain.enums import WorkerMode
from runtime.domain.launch_field_errors import derive_field_buckets_from_field_errors
from runtime.infrastructure.config.settings import load_settings
from runtime.infrastructure.observability.stdout_event import write_stdout_event
from runtime.infrastructure.http.srm.manager_client import build_manager_client
from runtime.infrastructure.strategy_loader.strategy_bundle_loader import (
    default_bundle_setting_path,
    read_bundle_json,
)
from runtime.interface.http.stop_control_server import StopControlHttpServer


def _identity_from_raw_bundle(data: Mapping[str, Any]) -> dict[str, object]:
    runtime_id = str(data.get("runtime_id") or "").strip()
    tenant_id = str(data.get("tenant_id") or "").strip()
    account_id = str(data.get("account_id") or "").strip()
    strategy_version_id = str(data.get("strategy_version_id") or "").strip()
    launch_attempt_raw = data.get("launch_attempt", "")
    try:
        launch_attempt = int(launch_attempt_raw)
    except (TypeError, ValueError):
        launch_attempt = 0
    worker_identity = (
        f"{runtime_id}:{strategy_version_id}:{launch_attempt}"
        if runtime_id and strategy_version_id and launch_attempt > 0
        else ""
    )
    return {
        "runtime_id": runtime_id,
        "tenant_id": tenant_id,
        "account_id": account_id,
        "strategy_version_id": strategy_version_id,
        "launch_attempt": launch_attempt,
        "worker_identity": worker_identity,
    }


def emit_validation_failure_to_manager(
    *,
    manager_client: object,
    exc: Exception,
    identity: dict[str, object],
) -> None:
    emitter = getattr(manager_client, "emit_signal", None)
    if not callable(emitter):
        return
    occurred_at = datetime.now(timezone.utc)
    field_errors: dict[str, object]
    reason = "validation_runtime_error"
    if isinstance(exc, LaunchSpecValidationError):
        reason = exc.reason
        field_errors = dict(exc.field_errors)
    else:
        field_errors = {"payload": str(exc)}
    missed_fields, invalid_fields, empty_fields = (
        derive_field_buckets_from_field_errors(field_errors)
    )

    envelope = {
        "signal_type": "bootstrap_failed",
        "identity": identity,
        "occurred_at": occurred_at,
        "event_id": "",
        "event_name": "",
        "event_version": 1,
        "producer": "strategy-worker-runtime",
        "correlation_id": "",
        "payload": {
            "occurred_at": occurred_at,
            "reason_code": RuntimeWorkerReasonCode.BOOTSTRAP_METADATA_INCONSISTENT.value,
            "error_code": WorkerErrorCode.BOOTSTRAP_FAILED.value,
            "missed_fields": missed_fields,
            "invalid_fields": invalid_fields,
            "empty_fields": empty_fields,
            "passed": [],
            "failed": ["validate_launch_metadata"],
            "not_checked": [
                "artifact_fetch",
                "artifact_verify",
                "entrypoint_load",
                "sdk_validate",
            ],
            "details": {
                "reason": reason,
                "field_errors": field_errors,
            },
        },
    }
    id_ = identity
    payload = envelope["payload"]
    details = payload.get("details")
    field_errors: dict[str, object] | None = None
    if isinstance(details, Mapping):
        raw_fe = details.get("field_errors")
        if isinstance(raw_fe, Mapping):
            field_errors = dict(raw_fe)
    write_stdout_event(
        level="ERROR",
        event_name="worker.bootstrap.failed",
        message="Launch metadata validation failed",
        runtime_id=str(id_.get("runtime_id") or "") or None,
        reason_code=str(payload.get("reason_code") or "")
        or RuntimeWorkerReasonCode.BOOTSTRAP_METADATA_INCONSISTENT.value,
        error_code=str(payload.get("error_code") or "") or None,
        field_errors=field_errors,
    )
    try:
        emitter(envelope)
    except Exception:
        pass


def _print_startup_validation_error(exc: Exception) -> None:
    if isinstance(exc, LaunchSpecValidationError):
        write_stdout_event(
            level="ERROR",
            event_name="worker.launch_spec.invalid",
            message="Launch spec validation failed",
            reason=exc.reason,
            reason_code="LAUNCH_SPEC_INVALID",
            field_errors=dict(exc.field_errors),
        )
        return
    write_stdout_event(
        level="ERROR",
        event_name="worker.startup.validation_failed",
        message=str(exc),
        reason_code="STARTUP_VALIDATION_FAILED",
    )


def run_runtime_from_cli() -> None:
    minimal_env = validate_minimal_env_from_environ()
    srm_env_report = report_minimal_env_validation_to_srm(minimal_env)
    print_minimal_env_validation_outcome(minimal_env, srm_response=srm_env_report)
    if not minimal_env.valid:
        raise SystemExit(2)

    raw: dict[str, Any] = {}
    bundle_path = default_bundle_setting_path()
    if bundle_path.is_file():
        try:
            raw = read_bundle_json(bundle_path)
        except Exception:
            raw = {}

    manager_client = build_manager_client()
    try:
        settings = load_settings()
    except Exception as exc:
        if (
            is_runtime_context_fetch_failure(exc)
            and minimal_env.valid
            and minimal_env.snapshot is not None
        ):
            context_report = report_runtime_context_fetch_failure_to_srm(
                exc,
                snapshot=minimal_env.snapshot,
            )
            print_runtime_context_fetch_failure_outcome(
                exc,
                srm_response=context_report,
            )
        emit_validation_failure_to_manager(
            manager_client=manager_client,
            exc=exc,
            identity=_identity_from_raw_bundle(raw),
        )
        _print_startup_validation_error(exc)
        raise SystemExit(2) from None

    outbound_clients = build_runtime_outbound_clients(settings)
    if settings.launch_spec.mode is WorkerMode.BACKTEST:
        write_stdout_event(
            level="INFO",
            event_name="worker.order_intent.egress",
            message="Order intents egress via BACKTEST stdout JSONL",
            transport="stdio_jsonl",
        )
    elif outbound_clients.risk_order_intent_client is not None:
        write_stdout_event(
            level="WARNING",
            event_name="worker.order_intent.egress",
            message="Order intents egress via Risk Service gRPC",
            transport="grpc",
            proto="risk_worker.proto",
            target=settings.risk_grpc_target,
            timeout_seconds=settings.risk_grpc_timeout_seconds,
        )
    else:
        write_stdout_event(
            level="WARNING",
            event_name="worker.order_intent.egress",
            message="SWR_RISK_GRPC_TARGET is not set; SDK order intents unavailable",
            reason_code="RISK_GRPC_NOT_CONFIGURED",
        )
    container = build_runtime_container(
        settings,
        manager_client=outbound_clients.manager_client,
        risk_order_intent_client=outbound_clients.risk_order_intent_client,
    )
    lifecycle = container.lifecycle_service
    worker_app = container.worker_app
    shutdown_coordinator = CooperativeShutdownCoordinator(
        lifecycle=lifecycle,
        worker_app=worker_app,
    )
    install_sigterm_handler(shutdown_coordinator)

    def _on_http_stop_requested(reason: str) -> dict[str, object]:
        return shutdown_coordinator.request_http_stop(reason)

    stop_http_server: StopControlHttpServer | None = None
    http_bind = (settings.worker_control_http_bind or "").strip()
    if http_bind:
        stop_http_server = StopControlHttpServer(
            bind_address=http_bind,
            on_stop_requested=_on_http_stop_requested,
        )
        stop_http_server.start()

    try:
        container.worker_app.run()
    finally:
        if stop_http_server is not None:
            stop_http_server.stop()

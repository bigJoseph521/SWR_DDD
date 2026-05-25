from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping, Protocol

from runtime.transport.grpc.control_plane_envelope_log import (
    BANNER_RM_TO_WR,
    BANNER_WR_TO_RM_GRPC,
    build_control_envelope_message,
    control_rpc_event_id,
    identity_fields_from_create_worker_request,
    identity_fields_from_worker_control_request,
    write_control_plane_envelope,
)
from runtime.transport.grpc.serializers import (
    WorkerControlSerializer,
    _wire_reason_or_error_code,
)


class WorkerControlApplication(Protocol):
    def create_worker(self, command: Any) -> Any: ...
    def stop_worker(self, command: Any) -> Any: ...
    def check_health(self, command: Any) -> Any: ...


def _result_get(result: Any, key: str, default: Any = None) -> Any:
    if isinstance(result, Mapping):
        return result.get(key, default)
    return getattr(result, key, default)


def _dt_iso_utc(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return str(value)


def _identity_merge_response(ident: dict[str, Any], result: Any) -> dict[str, Any]:
    out = dict(ident)
    rid = _result_get(result, "runtime_id")
    if isinstance(rid, str) and rid.strip():
        out["runtime_id"] = rid.strip()
    wid = _result_get(result, "worker_identity")
    if isinstance(wid, str) and wid.strip():
        out["worker_identity"] = wid.strip()
    la = _result_get(result, "launch_attempt")
    if isinstance(la, int) and la >= 1:
        out["launch_attempt"] = la
    return out


class WorkerControlService:
    def __init__(
        self,
        application: WorkerControlApplication,
        *,
        serializer: WorkerControlSerializer | None = None,
    ) -> None:
        self._application = application
        self._serializer = serializer or WorkerControlSerializer()

    def CreateWorker(self, request: Any, context: Any) -> Any:  # noqa: N802
        ident = identity_fields_from_create_worker_request(request)
        now = datetime.now(timezone.utc)
        rid = ident.get("runtime_id") or "unknown"
        la = int(ident.get("launch_attempt") or 1)
        write_control_plane_envelope(
            banner=BANNER_RM_TO_WR,
            message=build_control_envelope_message(
                event_id=control_rpc_event_id(
                    runtime_id=rid,
                    launch_attempt=la,
                    event_name="worker.control.create_worker.request",
                ),
                event_name="worker.control.create_worker.request",
                occurred_at=now,
                identity=ident,
                payload={
                    "reason_code": "CREATE_REQUESTED",
                    "runtime_id": ident.get("runtime_id", ""),
                    "strategy_version_id": ident.get("strategy_version_id", ""),
                },
            ),
        )
        command = self._serializer.create_request_to_command(request)
        result = self._application.create_worker(command)
        response = self._serializer.create_result_to_response(result)
        ident_rsp = _identity_merge_response(ident, result)
        occurred = _result_get(result, "accepted_at") or now
        if not isinstance(occurred, datetime):
            occurred = now
        write_control_plane_envelope(
            banner=BANNER_WR_TO_RM_GRPC,
            message=build_control_envelope_message(
                event_id=control_rpc_event_id(
                    runtime_id=str(ident_rsp.get("runtime_id") or rid),
                    launch_attempt=int(ident_rsp.get("launch_attempt") or la),
                    event_name="worker.control.create_worker.response",
                ),
                event_name="worker.control.create_worker.response",
                occurred_at=occurred,
                identity=ident_rsp,
                payload={
                    "accepted": bool(_result_get(result, "accepted", False)),
                    "local_state": str(_result_get(result, "local_state", "") or ""),
                    "reason_code": _wire_reason_or_error_code(
                        _result_get(result, "reason_code", "")
                    ),
                    "accepted_at": _dt_iso_utc(_result_get(result, "accepted_at")),
                    "message": str(_result_get(result, "message", "") or ""),
                },
            ),
        )
        return response

    def StopWorker(self, request: Any, context: Any) -> Any:  # noqa: N802
        ident = identity_fields_from_worker_control_request(request)
        now = datetime.now(timezone.utc)
        rid = ident.get("runtime_id") or "unknown"
        la = int(ident.get("launch_attempt") or 1)
        write_control_plane_envelope(
            banner=BANNER_RM_TO_WR,
            message=build_control_envelope_message(
                event_id=control_rpc_event_id(
                    runtime_id=rid,
                    launch_attempt=la,
                    event_name="worker.control.stop_worker.request",
                ),
                event_name="worker.control.stop_worker.request",
                occurred_at=now,
                identity=ident,
                payload={
                    "rpc": "StopWorker",
                    "runtime_id": str(getattr(request, "runtime_id", "") or ""),
                },
            ),
        )
        command = self._serializer.stop_request_to_command(request)
        result = self._application.stop_worker(command)
        ident_rsp = _identity_merge_response(ident, result)
        response = self._serializer.stop_result_to_response(result)
        occurred = _result_get(result, "accepted_at") or now
        if not isinstance(occurred, datetime):
            occurred = now
        write_control_plane_envelope(
            banner=BANNER_WR_TO_RM_GRPC,
            message=build_control_envelope_message(
                event_id=control_rpc_event_id(
                    runtime_id=str(ident_rsp.get("runtime_id") or rid),
                    launch_attempt=int(ident_rsp.get("launch_attempt") or la),
                    event_name="worker.control.stop_worker.response",
                ),
                event_name="worker.control.stop_worker.response",
                occurred_at=occurred,
                identity=ident_rsp,
                payload={
                    "accepted": bool(_result_get(result, "accepted", False)),
                    "accepted_at": _dt_iso_utc(_result_get(result, "accepted_at")),
                    "local_state": str(_result_get(result, "local_state", "") or ""),
                    "reason_code": _wire_reason_or_error_code(
                        _result_get(result, "reason_code", "")
                    ),
                    "message": str(_result_get(result, "message", "") or ""),
                },
            ),
        )
        return response

    def CheckHealth(self, request: Any, context: Any) -> Any:  # noqa: N802
        ident = identity_fields_from_worker_control_request(request)
        now = datetime.now(timezone.utc)
        rid = ident.get("runtime_id") or "unknown"
        la = int(ident.get("launch_attempt") or 1)
        write_control_plane_envelope(
            banner=BANNER_RM_TO_WR,
            message=build_control_envelope_message(
                event_id=control_rpc_event_id(
                    runtime_id=rid,
                    launch_attempt=la,
                    event_name="worker.control.health_check.request",
                ),
                event_name="worker.control.health_check.request",
                occurred_at=now,
                identity=ident,
                payload={"rpc": "CheckHealth"},
            ),
        )
        command = self._serializer.health_request_to_command(request)
        result = self._application.check_health(command)
        response = self._serializer.health_result_to_response(result)
        ident_rsp = _identity_merge_response(ident, result)
        write_control_plane_envelope(
            banner=BANNER_WR_TO_RM_GRPC,
            message=build_control_envelope_message(
                event_id=control_rpc_event_id(
                    runtime_id=str(ident_rsp.get("runtime_id") or rid),
                    launch_attempt=int(ident_rsp.get("launch_attempt") or la),
                    event_name="worker.control.health_check.response",
                ),
                event_name="worker.control.health_check.response",
                occurred_at=now,
                identity=ident_rsp,
                payload={
                    "state": str(_result_get(result, "state", "") or ""),
                },
            ),
        )
        return response

"""
Worker→runtime-manager observability and JSON shape (where this package builds JSON):

Stdout / logging use one event envelope: top-level ``event_id``, ``event_name``,
``event_version``, ``producer``, ``occurred_at``, ``correlation_id``, ``causation_id``,
``tenant_id``, ``account_id``, ``runtime_id``, ``worker_identity``, ``launch_attempt``,
``strategy_version_id``, plus a single nested ``payload`` for domain fields.

Paths:
- ``transport.grpc.manager_service``: ``StopWorker`` returns ``StopWorkerResponse`` as the
  same event-envelope shape as **stdout** (root metadata + ``payload`` for outcome).
  Response **stdout** still logs via ``build_control_envelope_message`` (duplicate line for operators).
- ``integration.manager_gateway`` + ``ManagerGateway._print_runtime_manager_message``:
  prints the same envelope shape on stdout. When ``STRATEGY_RUNTIME_MANAGER_BASE_URL`` is set,
  ``transport.manager_client`` routes workload signals over HTTP: heartbeats via
  ``transport.heartbeat`` (``POST /internal/v1/runtimes/{runtime_id}/status``) and
  lifecycle signals via ``transport.lifecycle_signal`` (``POST .../lifecycle-signals``).

Postman/grpcurl JSON for ``StopWorker`` is the envelope (no duplicate flat outcome fields).
"""

from __future__ import annotations

import json
import sys
import threading
from datetime import datetime, timezone
from typing import Any, Mapping

# Serialize writes so gRPC worker threads and heartbeat do not interleave lines.
_CONTROL_PLANE_LOG_LOCK = threading.Lock()

BANNER_WR_TO_RM = "-----------------From WR to RM-------------"
BANNER_WR_TO_RM_HTTP = "-----------------From WR to RM (HTTP)-------------"
BANNER_RM_TO_WR = "-----------------From RM to WR (gRPC)-------------"
BANNER_WR_TO_RM_GRPC = "-----------------From WR to RM (gRPC response)-------------"


def write_control_plane_envelope(*, banner: str, message: Mapping[str, Any]) -> None:
    """Print one event-envelope-shaped JSON line to stdout (command window)."""
    now_text = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    payload_line = json.dumps(
        message, separators=(",", ":"), ensure_ascii=True, default=str
    )
    _block = f"{banner}\n{now_text}: {payload_line}\n"
    with _CONTROL_PLANE_LOG_LOCK:
        sys.stdout.write(_block)
        sys.stdout.flush()


def control_rpc_event_id(
    *, runtime_id: str, launch_attempt: int, event_name: str
) -> str:
    return f"{runtime_id}:{launch_attempt}:{event_name}:control_plane"


def identity_fields_from_worker_control_request(request: Any) -> dict[str, Any]:
    """Extract envelope top-level identity fields from a manager_worker control RPC request."""
    la = int(getattr(request, "launch_attempt", 0) or 0)
    if la < 1:
        la = 1
    return {
        "tenant_id": str(getattr(request, "tenant_id", "") or ""),
        "account_id": str(getattr(request, "account_id", "") or ""),
        "runtime_id": str(getattr(request, "runtime_id", "") or ""),
        "worker_identity": str(getattr(request, "worker_identity", "") or ""),
        "launch_attempt": la,
        "strategy_version_id": str(getattr(request, "strategy_version_id", "") or ""),
        "correlation_id": str(getattr(request, "correlation_id", "") or ""),
        "causation_id": str(getattr(request, "causation_id", "") or ""),
    }


def identity_fields_from_create_worker_request(request: Any) -> dict[str, Any]:
    ls = getattr(request, "launch_spec", None)
    if ls is None:
        return identity_fields_from_worker_control_request(request)
    la = int(getattr(ls, "launch_attempt", 0) or 0)
    if la < 1:
        la = 1
    return {
        "tenant_id": str(getattr(ls, "tenant_id", "") or ""),
        "account_id": str(getattr(ls, "account_id", "") or ""),
        "runtime_id": str(getattr(ls, "runtime_id", "") or ""),
        "worker_identity": str(getattr(ls, "worker_identity", "") or ""),
        "launch_attempt": la,
        "strategy_version_id": str(getattr(ls, "strategy_version_id", "") or ""),
        "correlation_id": str(getattr(ls, "correlation_id", "") or ""),
        "causation_id": str(getattr(ls, "causation_id", "") or ""),
    }


def build_control_envelope_message(
    *,
    event_id: str,
    event_name: str,
    occurred_at: datetime,
    identity: Mapping[str, Any],
    payload: Mapping[str, Any],
    producer: str = "strategy-worker-runtime",
    event_version: int = 1,
) -> dict[str, Any]:
    occurred_iso = (
        occurred_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    )
    return {
        "event_id": event_id,
        "event_name": event_name,
        "event_version": event_version,
        "producer": producer,
        "occurred_at": occurred_iso,
        "correlation_id": str(identity.get("correlation_id") or ""),
        "causation_id": str(identity.get("causation_id") or ""),
        "tenant_id": str(identity.get("tenant_id") or ""),
        "account_id": str(identity.get("account_id") or ""),
        "runtime_id": str(identity.get("runtime_id") or ""),
        "worker_identity": str(identity.get("worker_identity") or ""),
        "launch_attempt": int(identity.get("launch_attempt") or 1),
        "strategy_version_id": str(identity.get("strategy_version_id") or ""),
        "payload": dict(payload),
    }

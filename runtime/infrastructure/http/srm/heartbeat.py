from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from typing import Any, Final, Literal, Mapping, cast
from urllib.parse import urljoin

from runtime.domain.enums import WorkerMode

SrmStatusSource = Literal["HEARTBEAT", "UPDATE"]

_LOG = logging.getLogger(__name__)

SERVICE_NAME = "strategy-worker-runtime"
RUNTIME_TYPE_STRATEGY_WORKER = "STRATEGY_WORKER_RUNTIME"
SRM_STATUS_SOURCE_HEARTBEAT: Final[SrmStatusSource] = "HEARTBEAT"
SRM_STATUS_SOURCE_UPDATE: Final[SrmStatusSource] = "UPDATE"
_ALLOWED_SRM_STATUS_SOURCES: Final[frozenset[SrmStatusSource]] = frozenset(
    {SRM_STATUS_SOURCE_HEARTBEAT, SRM_STATUS_SOURCE_UPDATE}
)

HEADER_REQUEST_ID = "x-request-id"
HEADER_CORRELATION_ID = "x-correlation-id"
HEADER_SERVICE_NAME = "x-service-name"

_HEALTHY_LOCAL_STATES = frozenset(
    {
        "INITIALIZING",
        "READY",
        "RUNNING",
        "STOPPING",
        "COMPLETED",
    }
)


def health_status_for_local_state(local_state: str | None) -> str:
    normalized = str(local_state or "").strip().upper()
    if normalized in _HEALTHY_LOCAL_STATES:
        return "HEALTHY"
    return "UNHEALTHY"


def format_occurred_at(value: datetime) -> str:
    candidate = (
        value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    )
    return candidate.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def build_srm_status_metadata(
    *,
    reason_code: str | None = None,
    message: str | None = None,
    retryable: bool | None = None,
) -> dict[str, Any]:
    """Optional failure context for SRM ``POST .../status`` (empty strings when healthy)."""
    meta: dict[str, Any] = {
        "reason_code": str(reason_code or "").strip(),
        "message": str(message or "").strip(),
    }
    if retryable is not None:
        meta["retryable"] = retryable
    return meta


def normalize_srm_status_source(value: str | None) -> SrmStatusSource:
    candidate = str(value or SRM_STATUS_SOURCE_HEARTBEAT).strip().upper()
    if candidate not in _ALLOWED_SRM_STATUS_SOURCES:
        raise ValueError(
            f"Unsupported SRM status source: {value!r} (expected HEARTBEAT or UPDATE)"
        )
    return cast(SrmStatusSource, candidate)


def build_srm_heartbeat_body(
    *,
    runtime_id: str,
    mode: str,
    owner_resource_id: str,
    local_state: str | None,
    observed_at: datetime,
    source: SrmStatusSource = SRM_STATUS_SOURCE_HEARTBEAT,
    runtime_status: str | None = None,
    health_status: str | None = None,
    reason_code: str | None = None,
    message: str | None = None,
    retryable: bool | None = None,
    metadata_empty: bool = False,
) -> dict[str, Any]:
    phase = str(local_state or "").strip().upper()
    status_source = normalize_srm_status_source(source)
    effective_runtime_status = str(runtime_status or phase).strip().upper()
    effective_health_status = (
        str(health_status).strip().upper()
        if health_status is not None
        else health_status_for_local_state(phase)
    )
    return {
        "runtime_id": runtime_id,
        "runtime_type": RUNTIME_TYPE_STRATEGY_WORKER,
        "mode": mode.strip().upper(),
        "owner_resource_id": owner_resource_id,
        "runtime_status": effective_runtime_status,
        "health_status": effective_health_status,
        "source": status_source,
        "occurred_at": format_occurred_at(observed_at),
        "metadata": (
            {}
            if metadata_empty
            else build_srm_status_metadata(
                reason_code=reason_code,
                message=message,
                retryable=retryable,
            )
        ),
    }


def post_srm_runtime_status(
    *,
    base_url: str,
    runtime_id: str,
    mode: str,
    owner_resource_id: str,
    runtime_status: str,
    health_status: str,
    observed_at: datetime | None = None,
    source: SrmStatusSource = SRM_STATUS_SOURCE_UPDATE,
    reason_code: str | None = None,
    message: str | None = None,
    retryable: bool | None = None,
    metadata_empty: bool = False,
    timeout_seconds: float = 30.0,
) -> dict[str, Any]:
    """POST ``/internal/v1/runtimes/{{runtime_id}}/status`` (same JSON + headers as heartbeat)."""
    when = observed_at if observed_at is not None else datetime.now(timezone.utc)
    body = build_srm_heartbeat_body(
        runtime_id=runtime_id,
        mode=mode,
        owner_resource_id=owner_resource_id,
        local_state=runtime_status,
        observed_at=when,
        source=source,
        runtime_status=runtime_status,
        health_status=health_status,
        reason_code=reason_code,
        message=message,
        retryable=retryable,
        metadata_empty=metadata_empty,
    )
    raw = json.dumps(body, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    url = join_runtime_status_url(base_url, runtime_id)
    req = urllib.request.Request(
        url,
        data=raw,
        method="POST",
        headers=build_srm_heartbeat_headers(),
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            text = resp.read().decode("utf-8")
            return _map_http_result(int(resp.status), text)
    except urllib.error.HTTPError as exc:
        try:
            err_body = exc.read().decode("utf-8")
        except Exception:
            err_body = str(exc)
        _LOG.warning(
            "srm_runtime_status_http_error",
            extra={"status": exc.code, "url": url},
        )
        return _map_http_result(int(exc.code or 0), err_body)
    except urllib.error.URLError as exc:
        _LOG.warning("srm_runtime_status_unreachable", exc_info=True)
        return {
            "accepted": False,
            "signal_type": "heartbeat",
            "reason_code": "SRM_NETWORK",
            "details": str(exc.reason or exc),
        }


def build_srm_heartbeat_headers() -> dict[str, str]:
    request_id = str(uuid.uuid4())
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        HEADER_REQUEST_ID: request_id,
        HEADER_CORRELATION_ID: request_id,
        HEADER_SERVICE_NAME: SERVICE_NAME,
    }


def join_runtime_status_url(base_url: str, runtime_id: str) -> str:
    base = base_url.strip().rstrip("/") + "/"
    return urljoin(base, f"internal/v1/runtimes/{runtime_id.strip()}/status")


def join_heartbeat_url(base_url: str, runtime_id: str) -> str:
    """Backward-compatible alias for :func:`join_runtime_status_url`."""
    return join_runtime_status_url(base_url, runtime_id)


def _map_http_result(status: int, body: str) -> dict[str, Any]:
    trimmed = body.strip()
    if status in (200, 202):
        if not trimmed:
            return {"accepted": True, "signal_type": "heartbeat"}
        try:
            parsed = json.loads(trimmed)
        except json.JSONDecodeError:
            return {
                "accepted": False,
                "signal_type": "heartbeat",
                "reason_code": "SRM_DECODE_FAILED",
                "details": "success response is not valid JSON",
            }
        if isinstance(parsed, dict) and parsed.get("success") is True:
            return {"accepted": True, "signal_type": "heartbeat"}
        return {
            "accepted": False,
            "signal_type": "heartbeat",
            "reason_code": "SRM_DECODE_FAILED",
            "details": "success envelope missing success=true",
        }
    if status == 404:
        return {
            "accepted": False,
            "signal_type": "heartbeat",
            "reason_code": "RUNTIME_NOT_FOUND",
            "details": trimmed[:256] or "runtime not found",
        }
    if status in (409, 422):
        return {
            "accepted": False,
            "signal_type": "heartbeat",
            "reason_code": "HEARTBEAT_REJECTED",
            "details": trimmed[:256] or "heartbeat rejected",
        }
    if status == 400:
        return {
            "accepted": False,
            "signal_type": "heartbeat",
            "reason_code": "VALIDATION_ERROR",
            "details": trimmed[:256] or "validation error",
        }
    if status in (401, 403):
        return {
            "accepted": False,
            "signal_type": "heartbeat",
            "reason_code": "AUTH_FAILED",
            "details": trimmed[:256] or "authentication failed",
        }
    if status >= 500:
        return {
            "accepted": False,
            "signal_type": "heartbeat",
            "reason_code": "SRM_UNAVAILABLE",
            "details": f"HTTP {status}",
        }
    return {
        "accepted": False,
        "signal_type": "heartbeat",
        "reason_code": "UNEXPECTED_HTTP_STATUS",
        "details": f"HTTP {status}",
    }


class SrmHeartbeatHttpClient:
    """POST runtime status updates to strategy-runtime-manager ``.../runtimes/{{id}}/status``."""

    def __init__(
        self,
        *,
        base_url: str,
        owner_resource_id: str,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._base_url = base_url.strip()
        self._owner_resource_id = owner_resource_id.strip()
        self._timeout_seconds = timeout_seconds

    def emit_signal(self, envelope: Mapping[str, Any]) -> dict[str, Any]:
        signal_type = str(envelope.get("signal_type") or "")
        if signal_type != "heartbeat":
            return {
                "accepted": False,
                "signal_type": signal_type,
                "reason_code": "UNSUPPORTED_SIGNAL",
            }

        identity = envelope.get("identity")
        payload = envelope.get("payload")
        if not isinstance(identity, Mapping):
            raise ValueError("signal envelope must include identity mapping")
        if not isinstance(payload, Mapping):
            raise ValueError("signal envelope must include payload mapping")

        mode = str(identity.get("mode") or "").strip().upper()
        if mode == WorkerMode.BACKTEST.value:
            return {"accepted": True, "signal_type": "heartbeat", "skipped": True}

        if not self._base_url:
            return {
                "accepted": False,
                "signal_type": "heartbeat",
                "reason_code": "SRM_BASE_URL_NOT_CONFIGURED",
            }

        runtime_id = str(identity.get("runtime_id") or "").strip()
        if not runtime_id:
            raise ValueError("runtime_id is required for SRM heartbeat")

        if not self._owner_resource_id:
            return {
                "accepted": False,
                "signal_type": "heartbeat",
                "reason_code": "OWNER_RESOURCE_ID_REQUIRED",
            }

        observed_at = payload.get("observed_at")
        if not isinstance(observed_at, datetime):
            raise ValueError("heartbeat payload.observed_at must be datetime")

        reason_code_raw = payload.get("reason_code")
        message_raw = payload.get("message")
        source_raw = payload.get("source")
        runtime_status_raw = payload.get("runtime_status")
        health_status_raw = payload.get("health_status")
        retryable_raw = payload.get("retryable")
        retryable: bool | None
        if isinstance(retryable_raw, bool):
            retryable = retryable_raw
        else:
            retryable = None
        body = build_srm_heartbeat_body(
            runtime_id=runtime_id,
            mode=mode,
            owner_resource_id=self._owner_resource_id,
            local_state=(
                str(payload.get("local_state"))
                if payload.get("local_state") is not None
                else None
            ),
            observed_at=observed_at,
            source=normalize_srm_status_source(
                str(source_raw) if source_raw is not None else None
            ),
            runtime_status=(
                str(runtime_status_raw) if runtime_status_raw is not None else None
            ),
            health_status=(
                str(health_status_raw) if health_status_raw is not None else None
            ),
            reason_code=(str(reason_code_raw) if reason_code_raw is not None else None),
            message=(str(message_raw) if message_raw is not None else None),
            retryable=retryable,
        )
        raw = json.dumps(body, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        url = join_runtime_status_url(self._base_url, runtime_id)
        req = urllib.request.Request(
            url,
            data=raw,
            method="POST",
            headers=build_srm_heartbeat_headers(),
        )
        try:
            with urllib.request.urlopen(req, timeout=self._timeout_seconds) as resp:
                text = resp.read().decode("utf-8")
                return _map_http_result(int(resp.status), text)
        except urllib.error.HTTPError as exc:
            try:
                err_body = exc.read().decode("utf-8")
            except Exception:
                err_body = str(exc)
            _LOG.warning(
                "srm_heartbeat_http_error",
                extra={"status": exc.code, "url": url},
            )
            return _map_http_result(int(exc.code or 0), err_body)
        except urllib.error.URLError as exc:
            _LOG.warning("srm_heartbeat_unreachable", exc_info=True)
            return {
                "accepted": False,
                "signal_type": "heartbeat",
                "reason_code": "SRM_NETWORK",
                "details": str(exc.reason or exc),
            }

    def close(self) -> None:
        return None

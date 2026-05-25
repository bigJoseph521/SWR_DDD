from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from datetime import datetime
from typing import Any, Mapping
from urllib.parse import urljoin

from runtime.infrastructure.http.srm.heartbeat import (
    _map_http_result,
    build_srm_heartbeat_headers,
    format_occurred_at,
)

_LOG = logging.getLogger(__name__)


def join_lifecycle_signal_url(base_url: str, runtime_id: str) -> str:
    base = base_url.strip().rstrip("/") + "/"
    return urljoin(base, f"internal/v1/runtimes/{runtime_id.strip()}/lifecycle-signals")


def _flatten_lifecycle_envelope(envelope: Mapping[str, Any]) -> dict[str, Any]:
    identity = envelope.get("identity")
    identity_map = dict(identity) if isinstance(identity, Mapping) else {}
    body: dict[str, Any] = {
        k: v
        for k, v in envelope.items()
        if k not in ("identity", "payload", "signal_type")
    }
    for key, value in identity_map.items():
        body.setdefault(key, value)
    payload = envelope.get("payload")
    body["payload"] = dict(payload) if isinstance(payload, Mapping) else {}
    body["signal_type"] = str(envelope.get("signal_type") or "").strip()
    occurred_at = body.get("occurred_at")
    if isinstance(occurred_at, datetime):
        body["occurred_at"] = format_occurred_at(occurred_at)
    payload_occurred = body["payload"].get("occurred_at")
    if isinstance(payload_occurred, datetime):
        body["payload"]["occurred_at"] = format_occurred_at(payload_occurred)
    if "occurred_at" not in body and isinstance(
        body["payload"].get("occurred_at"), str
    ):
        body["occurred_at"] = body["payload"]["occurred_at"]
    return body


class SrmLifecycleHttpClient:
    """POST worker lifecycle signals to strategy-runtime-manager REST API."""

    def __init__(self, *, base_url: str, timeout_seconds: float = 30.0) -> None:
        self._base_url = base_url.strip()
        self._timeout_seconds = timeout_seconds

    def emit_signal(self, envelope: Mapping[str, Any]) -> dict[str, Any]:
        signal_type = str(envelope.get("signal_type") or "")
        if signal_type == "heartbeat":
            return {
                "accepted": False,
                "signal_type": signal_type,
                "reason_code": "HEARTBEAT_USE_HTTP",
            }

        if not self._base_url:
            return {
                "accepted": False,
                "signal_type": signal_type,
                "reason_code": "SRM_BASE_URL_NOT_CONFIGURED",
            }

        identity = envelope.get("identity")
        if not isinstance(identity, Mapping):
            raise ValueError("signal envelope must include identity mapping")

        runtime_id = str(identity.get("runtime_id") or "").strip()
        if not runtime_id:
            raise ValueError("runtime_id is required for SRM lifecycle signal")

        body = _flatten_lifecycle_envelope(envelope)
        raw = json.dumps(body, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        url = join_lifecycle_signal_url(self._base_url, runtime_id)
        req = urllib.request.Request(
            url,
            data=raw,
            method="POST",
            headers=build_srm_heartbeat_headers(),
        )
        try:
            with urllib.request.urlopen(req, timeout=self._timeout_seconds) as resp:
                text = resp.read().decode("utf-8")
                result = _map_http_result(int(resp.status), text)
                result["signal_type"] = signal_type
                return result
        except urllib.error.HTTPError as exc:
            try:
                err_body = exc.read().decode("utf-8")
            except Exception:
                err_body = str(exc)
            _LOG.warning(
                "srm_lifecycle_http_error",
                extra={
                    "status": exc.code,
                    "url": url,
                    "signal_type": signal_type,
                },
            )
            result = _map_http_result(int(exc.code or 0), err_body)
            result["signal_type"] = signal_type
            return result
        except urllib.error.URLError as exc:
            _LOG.warning("srm_lifecycle_unreachable", exc_info=True)
            return {
                "accepted": False,
                "signal_type": signal_type,
                "reason_code": "SRM_NETWORK",
                "details": str(exc.reason or exc),
            }

    def close(self) -> None:
        return None

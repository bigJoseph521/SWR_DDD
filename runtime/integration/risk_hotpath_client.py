from __future__ import annotations

import json
import logging
import re
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Mapping

_LOG = logging.getLogger(__name__)


def _utc_iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None or dt.utcoffset() is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    text = str(value).strip()
    return text or None


def oms_style_payload_to_risk_evaluate_json(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    """
    Map worker / SDK order-intent dict (same shape as OMS gRPC staging dict) to
    risk-service ``POST /order-intents/evaluate/new`` JSON body.
    """
    job = str(payload.get("job_id") or "").strip()
    runtime = str(payload.get("runtime_id") or "").strip()
    job_id = job or runtime
    oid = payload.get("order_intent_id")
    if oid is None or isinstance(oid, bool):
        order_intent_id = ""
    elif isinstance(oid, int):
        order_intent_id = str(oid)
    else:
        order_intent_id = str(oid).strip()

    lp = payload.get("limit_price")
    limit_price: str | None
    if lp is None or (isinstance(lp, str) and not lp.strip()):
        limit_price = None
    else:
        limit_price = str(lp).strip() or None

    sp = payload.get("stop_price")
    stop_price: str | None
    if sp is None or (isinstance(sp, str) and not sp.strip()):
        stop_price = None
    else:
        stop_price = str(sp).strip() or None

    qty = payload.get("quantity")
    quantity: str | None
    if qty is None or (isinstance(qty, str) and not qty.strip()):
        quantity = None
    else:
        quantity = str(qty).strip() or None

    tif = str(payload.get("time_in_force") or "").strip()
    body: dict[str, Any] = {
        "idempotency_key": str(payload.get("idempotency_key") or "").strip(),
        "instrument_id": str(payload.get("instrument_id") or "").strip(),
        "job_id": job_id,
        "order_type": str(payload.get("order_type") or "").strip().lower(),
        "side": str(payload.get("side") or "").strip().lower(),
        "order_intent_id": order_intent_id,
    }
    corr = str(payload.get("correlation_id") or payload.get("corr_id") or "").strip()
    if corr:
        body["correlation_id"] = corr
    ca = _utc_iso(payload.get("created_at"))
    if ca:
        body["created_at"] = ca
    ra = _utc_iso(payload.get("requested_at"))
    if ra:
        body["requested_at"] = ra
    if limit_price is not None:
        body["limit_price"] = limit_price
    if stop_price is not None:
        body["stop_price"] = stop_price
    if quantity is not None:
        body["quantity"] = quantity
    if tif:
        body["time_in_force"] = tif
    rt = str(payload.get("runtime_id") or "").strip()
    if rt:
        body["runtime_id"] = rt
    acct = str(payload.get("account_id") or "").strip()
    if acct:
        body["account_id"] = acct
    return body


def oms_style_payload_to_risk_evaluate_replace_json(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    """
    Map a replace-intent dict to risk-service ``POST /order-intents/evaluate/replace`` JSON body.
    Exactly one of ``quantity`` or ``order_type`` must be non-empty (enforced server-side).
    """
    job = str(payload.get("job_id") or "").strip()
    runtime = str(payload.get("runtime_id") or "").strip()
    job_id = job or runtime
    oid = payload.get("order_intent_id")
    if oid is None or isinstance(oid, bool):
        order_intent_id = ""
    elif isinstance(oid, int):
        order_intent_id = str(oid)
    else:
        order_intent_id = str(oid).strip()

    qty_raw = payload.get("quantity")
    quantity: str | None
    if qty_raw is None or (isinstance(qty_raw, str) and not qty_raw.strip()):
        quantity = None
    else:
        quantity = str(qty_raw).strip() or None

    ot = str(payload.get("order_type") or "").strip().lower()
    body: dict[str, Any] = {
        "idempotency_key": str(payload.get("idempotency_key") or "").strip(),
        "job_id": job_id,
        "order_intent_id": order_intent_id,
        "order_type": ot,
        "target_oms_order_id": str(payload.get("target_oms_order_id") or "").strip(),
        "account_id": str(payload.get("account_id") or "").strip(),
    }
    if quantity is not None:
        body["quantity"] = quantity
    corr = str(payload.get("correlation_id") or payload.get("corr_id") or "").strip()
    if corr:
        body["correlation_id"] = corr
    ca = _utc_iso(payload.get("created_at"))
    if ca:
        body["created_at"] = ca
    ra = _utc_iso(payload.get("requested_at"))
    if ra:
        body["requested_at"] = ra
    rt = str(payload.get("runtime_id") or "").strip()
    if rt:
        body["runtime_id"] = rt
    return body


def _map_risk_success_response(data: Mapping[str, Any]) -> dict[str, Any]:
    status = str(data.get("status") or "").strip().upper()
    accepted = status == "APPROVED"
    rc = str(data.get("reason_code") or "").strip()
    oid = data.get("oms_order_id")
    return {
        "accepted": accepted,
        "status": status,
        "order_id": oid if oid is not None else None,
        "state": None,
        "reason_code": rc.upper() if rc else None,
        "error_code": None,
        "risk_decision_id": data.get("risk_decision_id"),
        "oms_forward_required": data.get("oms_forward_required"),
    }


def _map_http_error(status_code: int, raw_body: str) -> dict[str, Any]:
    code = None
    message = raw_body[:500]
    try:
        parsed = json.loads(raw_body)
        err = parsed.get("error")
        if isinstance(err, dict):
            code = str(err.get("code") or "").strip().upper() or None
            message = str(err.get("message") or message).strip()
    except json.JSONDecodeError:
        pass
    return {
        "accepted": False,
        "status": "ERROR",
        "order_id": None,
        "state": None,
        "reason_code": code or f"HTTP_{status_code}",
        "error_code": code,
        "error_message": message,
    }


class RiskHotPathClient:
    """
    HTTP client for risk-service ``POST /order-intents/evaluate/new`` on the admin REST bind
    (same host/port as other control-plane HTTP; "base URL", not a separate hotpath port).

    ``SWR_RISK_GRPC_TARGET`` is ``host:port`` (same convention as other ``*_GRPC_TARGET``
    vars); strategy ingress uses REST to this URL, not gRPC.

    Correlation may be sent as JSON ``correlation_id`` (and/or ``x-corr-id``); the service
    prefers the header when both are present.

    Note: ``POST /order-intents/evaluate/new`` (and legacy ``/order-intents/evaluate``) always
    evaluates as **MANUAL**-sourced intents; the
    strategy worker should use gRPC to risk-service for **STRATEGY** sourcing.
    """

    def __init__(self, *, host: str, port: int, timeout_seconds: float) -> None:
        self._host = host
        self._port = port
        self._timeout_seconds = timeout_seconds

    @property
    def evaluate_url(self) -> str:
        return f"http://{self._host}:{self._port}/order-intents/evaluate/new"

    @property
    def evaluate_replace_url(self) -> str:
        return f"http://{self._host}:{self._port}/order-intents/evaluate/replace"

    def submit_order_intent(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        body = oms_style_payload_to_risk_evaluate_json(payload)
        raw = json.dumps(body, separators=(",", ":"), default=str).encode("utf-8")
        corr = (
            str(payload.get("corr_id") or payload.get("correlation_id") or "").strip()
            or str(body.get("correlation_id") or "").strip()
        )
        request_id = str(payload.get("request_id") or "").strip()
        user_id = str(payload.get("user_id") or "").strip()
        if not corr or not request_id:
            raise ValueError(
                "submit_order_intent requires correlation (corr_id|correlation_id in payload "
                "or correlation_id in evaluate JSON body) and request_id for risk-service HTTP evaluate"
            )
        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "x-request-id": request_id,
        }
        if user_id:
            headers["x-user-id"] = user_id
        # Duplicate correlation in header only when absent from JSON (body field is enough).
        if corr and "correlation_id" not in body:
            headers["x-corr-id"] = corr
        req = urllib.request.Request(
            self.evaluate_url,
            data=raw,
            method="POST",
            headers=headers,
        )
        try:
            with urllib.request.urlopen(req, timeout=self._timeout_seconds) as resp:
                text = resp.read().decode("utf-8")
                data = json.loads(text)
                if not isinstance(data, dict):
                    return _map_http_error(500, "non_object_json_response")
                return _map_risk_success_response(data)
        except urllib.error.HTTPError as exc:
            try:
                err_body = exc.read().decode("utf-8")
            except Exception:
                err_body = str(exc)
            _LOG.warning(
                "risk_hotpath_http_error",
                extra={"status": exc.code, "url": self.evaluate_url},
            )
            return _map_http_error(int(exc.code or 0), err_body)
        except urllib.error.URLError as exc:
            _LOG.warning("risk_hotpath_unreachable", exc_info=True)
            return {
                "accepted": False,
                "status": "UNAVAILABLE",
                "order_id": None,
                "state": None,
                "reason_code": "RISK_UNREACHABLE",
                "error_code": "RISK_UNREACHABLE",
                "error_message": str(exc.reason or exc),
            }
        except json.JSONDecodeError as exc:
            _LOG.warning("risk_hotpath_bad_json", exc_info=True)
            return {
                "accepted": False,
                "status": "ERROR",
                "order_id": None,
                "state": None,
                "reason_code": "INVALID_JSON",
                "error_code": "INVALID_JSON",
                "error_message": str(exc),
            }

    def submit_replace_order_intent(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        body = oms_style_payload_to_risk_evaluate_replace_json(payload)
        raw = json.dumps(body, separators=(",", ":"), default=str).encode("utf-8")
        corr = (
            str(payload.get("corr_id") or payload.get("correlation_id") or "").strip()
            or str(body.get("correlation_id") or "").strip()
        )
        request_id = str(payload.get("request_id") or "").strip()
        user_id = str(payload.get("user_id") or "").strip()
        if not corr or not request_id:
            raise ValueError(
                "submit_replace_order_intent requires correlation "
                "(corr_id|correlation_id in payload or correlation_id in JSON body) "
                "and request_id for risk-service HTTP evaluate"
            )
        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "x-request-id": request_id,
        }
        if user_id:
            headers["x-user-id"] = user_id
        if corr and "correlation_id" not in body:
            headers["x-corr-id"] = corr
        req = urllib.request.Request(
            self.evaluate_replace_url,
            data=raw,
            method="POST",
            headers=headers,
        )
        try:
            with urllib.request.urlopen(req, timeout=self._timeout_seconds) as resp:
                text = resp.read().decode("utf-8")
                data = json.loads(text)
                if not isinstance(data, dict):
                    return _map_http_error(500, "non_object_json_response")
                return _map_risk_success_response(data)
        except urllib.error.HTTPError as exc:
            try:
                err_body = exc.read().decode("utf-8")
            except Exception:
                err_body = str(exc)
            _LOG.warning(
                "risk_hotpath_http_error_replace",
                extra={"status": exc.code, "url": self.evaluate_replace_url},
            )
            return _map_http_error(int(exc.code or 0), err_body)
        except urllib.error.URLError as exc:
            _LOG.warning("risk_hotpath_unreachable_replace", exc_info=True)
            return {
                "accepted": False,
                "status": "UNAVAILABLE",
                "order_id": None,
                "state": None,
                "reason_code": "RISK_UNREACHABLE",
                "error_code": "RISK_UNREACHABLE",
                "error_message": str(exc.reason or exc),
            }
        except json.JSONDecodeError as exc:
            _LOG.warning("risk_hotpath_bad_json_replace", exc_info=True)
            return {
                "accepted": False,
                "status": "ERROR",
                "order_id": None,
                "state": None,
                "reason_code": "INVALID_JSON",
                "error_code": "INVALID_JSON",
                "error_message": str(exc),
            }


def _parse_host_port(target: str) -> tuple[str, int] | None:
    t = target.strip()
    if not t:
        return None
    t = re.sub(r"^https?://", "", t, flags=re.IGNORECASE).strip()
    if "/" in t:
        t = t.split("/", 1)[0].strip()
    if ":" not in t:
        return None
    host, _, port_s = t.rpartition(":")
    host = host.strip()
    if not host:
        return None
    try:
        port = int(port_s.strip())
    except ValueError:
        return None
    if port <= 0 or port > 65535:
        return None
    return host, port


def build_risk_hotpath_client(
    *, target: str, timeout_seconds: float
) -> RiskHotPathClient | None:
    parsed = _parse_host_port(target)
    if parsed is None:
        return None
    host, port = parsed
    return RiskHotPathClient(host=host, port=port, timeout_seconds=timeout_seconds)

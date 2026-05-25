from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping

import grpc
from google.protobuf.timestamp_pb2 import Timestamp
from runtime.transport.grpc.serializers import (
    risk_worker_pb2,
    risk_worker_pb2_grpc,
)


@dataclass(frozen=True, slots=True)
class DependencyClientError(Exception):
    code: str
    message: str
    retryable: bool = False
    details: Mapping[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        return self.message


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _to_proto_timestamp(value: datetime | None) -> Any:
    ts = Timestamp()
    if value is None:
        value = _utc_now()
    ts.FromDatetime(value.astimezone(timezone.utc))
    return ts


def _coerce_order_intent_id(value: object) -> str:
    """Order intent wire uses string ``order_intent_id`` (UUID; legacy JSON may be numeric)."""
    if value is None or isinstance(value, bool):
        return ""
    if isinstance(value, int):
        return str(value)
    return str(value).strip()


def _coerce_cancel_order_intent_id(value: object) -> str:
    """Cancel/replace wire uses string ``order_intent_id`` (may be numeric in legacy JSON)."""
    return _coerce_order_intent_id(value)


def _parse_rfc3339_utc(value: str) -> datetime | None:
    t = value.strip()
    if not t:
        return None
    if t.endswith("Z"):
        t = t[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(t).astimezone(timezone.utc)
    except ValueError:
        return None


def _coerce_decimal_string(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value)
    return str(value).strip()


def normalize_oms_submission_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """
    Merge legacy domain/SDK dicts into the canonical worker→OMS shape (see ``risk_worker.proto``).
    """
    raw = dict(payload)
    # Backtest / replay-only; must not appear on OMS wire or in OMS-bound journal payloads.
    raw.pop("replay_session_id", None)
    raw.pop("strategy_context", None)
    lp_src = raw.get("limit_price")
    if (lp_src is None or lp_src == "") and raw.get("price") not in (None, ""):
        raw["limit_price"] = _coerce_decimal_string(raw["price"])
    if "mode" not in raw and raw.get("environment"):
        raw["mode"] = raw["environment"]
    if "time_in_force" not in raw or raw.get("time_in_force") in (None, ""):
        tif = raw.get("tif")
        if tif not in (None, ""):
            raw["time_in_force"] = _coerce_decimal_string(tif)
    raw["quantity"] = _coerce_decimal_string(raw.get("quantity"))
    raw["limit_price"] = _coerce_decimal_string(raw.get("limit_price"))
    raw["stop_price"] = _coerce_decimal_string(raw.get("stop_price"))
    raw.setdefault("correlation_id", _coerce_decimal_string(raw.get("correlation_id")))
    raw.setdefault("account_id", _coerce_decimal_string(raw.get("account_id")))
    raw.setdefault("time_in_force", _coerce_decimal_string(raw.get("time_in_force")))
    raw.setdefault("instrument_id", _coerce_decimal_string(raw.get("instrument_id")))
    raw.setdefault("side", _coerce_decimal_string(raw.get("side")))
    raw.setdefault("order_type", _coerce_decimal_string(raw.get("order_type")))
    raw.setdefault(
        "idempotency_key", _coerce_decimal_string(raw.get("idempotency_key"))
    )
    raw.setdefault("job_id", _coerce_decimal_string(raw.get("job_id")))
    raw.setdefault("symbol", _coerce_decimal_string(raw.get("symbol")))
    raw["order_intent_id"] = _coerce_order_intent_id(raw.get("order_intent_id"))
    if raw.get("requested_at") in (None, "") and raw.get("occurred_at") not in (
        None,
        "",
    ):
        raw["requested_at"] = raw["occurred_at"]
    raw.pop("occurred_at", None)
    return raw


# Stable key order for console / logs (worker→risk ``OrderIntent`` wire + extension metadata).
ORDER_INTENT_DISPLAY_FIELD_NAMES: tuple[str, ...] = (
    "account_id",
    "causation_id",
    "correlation_id",
    "created_at",
    "deployment_id",
    "idempotency_key",
    "instrument_id",
    "job_id",
    "launch_attempt",
    "limit_price",
    "mode",
    "order_intent_id",
    "order_type",
    "quantity",
    "requested_at",
    "runtime_id",
    "side",
    "stop_price",
    "strategy_version_id",
    "symbol",
    "tenant_id",
    "time_in_force",
    "worker_identity",
)


def _mode_string_for_console(payload: Mapping[str, Any]) -> str:
    m = str(payload.get("mode") or payload.get("environment") or "").strip().upper()
    if m == "LIVE":
        return "LIVE"
    if m == "PAPER":
        return "PAPER"
    if m == "BACKTEST":
        return "BACKTEST"
    return m if m else "BACKTEST"


def _timestamp_wire_str(value: object) -> str:
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None or dt.utcoffset() is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if value is None:
        return ""
    return str(value).strip()


def order_intent_wire_dict_for_console(payload: Mapping[str, Any]) -> dict[str, Any]:
    """
    Canonical order-intent mapping for stdout: every wire field is present (empty string / 0
    when unknown), suitable for ``json.dumps`` without default handlers.
    """
    p = normalize_oms_submission_payload(dict(payload))
    for key in ("tenant_id", "worker_identity", "deployment_id", "causation_id"):
        if key not in p or p[key] is None:
            p[key] = ""
        else:
            p[key] = _coerce_decimal_string(p.get(key))
    raw_la = p.get("launch_attempt", 0)
    try:
        launch_attempt = int(raw_la) if raw_la is not None else 0
    except (TypeError, ValueError):
        launch_attempt = 0

    def s(key: str) -> str:
        return _coerce_decimal_string(p.get(key))

    values: dict[str, Any] = {
        "account_id": s("account_id"),
        "causation_id": s("causation_id"),
        "correlation_id": s("correlation_id"),
        "created_at": _timestamp_wire_str(p.get("created_at")),
        "deployment_id": s("deployment_id"),
        "idempotency_key": s("idempotency_key"),
        "instrument_id": s("instrument_id"),
        "job_id": s("job_id"),
        "launch_attempt": launch_attempt,
        "limit_price": s("limit_price"),
        "mode": _mode_string_for_console(p),
        "order_intent_id": _coerce_order_intent_id(p.get("order_intent_id")),
        "order_type": s("order_type"),
        "quantity": s("quantity"),
        "requested_at": _timestamp_wire_str(p.get("requested_at")),
        "runtime_id": s("runtime_id"),
        "side": s("side"),
        "stop_price": s("stop_price"),
        "strategy_version_id": s("strategy_version_id"),
        "symbol": s("symbol"),
        "tenant_id": s("tenant_id"),
        "time_in_force": s("time_in_force"),
        "worker_identity": s("worker_identity"),
    }
    return {name: values[name] for name in ORDER_INTENT_DISPLAY_FIELD_NAMES}


REPLACE_INTENT_DISPLAY_FIELD_NAMES: tuple[str, ...] = (
    "account_id",
    "correlation_id",
    "created_at",
    "idempotency_key",
    "job_id",
    "mode",
    "order_intent_id",
    "order_type",
    "quantity",
    "requested_at",
    "target_oms_order_id",
)


def replace_order_intent_wire_dict_for_console(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Canonical replace-intent mapping for stdout (``ReplaceOrderIntent`` / risk hot path)."""
    p = dict(payload)
    wall = p.get("requested_at")
    created = p.get("created_at")
    qty = _coerce_decimal_string(p.get("quantity"))
    ot = _coerce_decimal_string(p.get("order_type"))
    values: dict[str, Any] = {
        "account_id": _coerce_decimal_string(p.get("account_id")),
        "correlation_id": _coerce_decimal_string(p.get("correlation_id")),
        "created_at": _timestamp_wire_str(created),
        "idempotency_key": _coerce_decimal_string(p.get("idempotency_key")),
        "job_id": _coerce_decimal_string(p.get("job_id")),
        "mode": _mode_string_for_console(p),
        "order_intent_id": _coerce_cancel_order_intent_id(p.get("order_intent_id")),
        "order_type": ot,
        "quantity": qty,
        "requested_at": _timestamp_wire_str(wall),
        "target_oms_order_id": _coerce_decimal_string(p.get("target_oms_order_id")),
    }
    return {name: values[name] for name in REPLACE_INTENT_DISPLAY_FIELD_NAMES}


CANCEL_INTENT_DISPLAY_FIELD_NAMES: tuple[str, ...] = (
    "account_id",
    "correlation_id",
    "created_at",
    "idempotency_key",
    "job_id",
    "mode",
    "order_intent_id",
    "requested_at",
    "target_oms_order_id",
)


def cancel_order_intent_wire_dict_for_console(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Canonical cancel-intent mapping for stdout (``CancelOrderIntent`` / risk hot path)."""
    p = dict(payload)
    wall = p.get("requested_at")
    created = p.get("created_at")
    values: dict[str, Any] = {
        "account_id": _coerce_decimal_string(p.get("account_id")),
        "correlation_id": _coerce_decimal_string(p.get("correlation_id")),
        "created_at": _timestamp_wire_str(created),
        "idempotency_key": _coerce_decimal_string(p.get("idempotency_key")),
        "job_id": _coerce_decimal_string(p.get("job_id")),
        "mode": _mode_string_for_console(p),
        "order_intent_id": _coerce_cancel_order_intent_id(p.get("order_intent_id")),
        "requested_at": _timestamp_wire_str(wall),
        "target_oms_order_id": _coerce_decimal_string(p.get("target_oms_order_id")),
    }
    return {name: values[name] for name in CANCEL_INTENT_DISPLAY_FIELD_NAMES}


def _mode_enum(payload: Mapping[str, Any]) -> int:
    mode = str(payload.get("mode") or payload.get("environment") or "").strip().upper()
    if mode == "LIVE":
        return risk_worker_pb2.LIVE
    if mode in ("PAPER", ""):
        return risk_worker_pb2.PAPER
    if mode == "BACKTEST":
        return risk_worker_pb2.BACKTEST
    return risk_worker_pb2.BACKTEST


def _optional_extension(payload: Mapping[str, Any]) -> Any:
    """Populate when routing metadata is present (SDK / domain paths)."""
    if not any(
        payload.get(k) not in (None, "", 0)
        for k in ("runtime_id", "strategy_version_id", "tenant_id", "causation_id")
    ):
        return None
    return risk_worker_pb2.CommonMetadata(
        correlation_id=_coerce_decimal_string(payload.get("correlation_id")),
        causation_id=_coerce_decimal_string(payload.get("causation_id")),
        tenant_id=_coerce_decimal_string(payload.get("tenant_id")),
        account_id=_coerce_decimal_string(payload.get("account_id")),
        runtime_id=_coerce_decimal_string(payload.get("runtime_id")),
        worker_identity=_coerce_decimal_string(payload.get("worker_identity")),
        launch_attempt=0,
        strategy_version_id=_coerce_decimal_string(payload.get("strategy_version_id")),
        job_id=_coerce_decimal_string(payload.get("job_id")),
        deployment_id=_coerce_decimal_string(payload.get("deployment_id")),
    )


def _normalize_grpc_error(exc: grpc.RpcError) -> DependencyClientError:
    status_code = exc.code()
    details = {"grpc_code": str(status_code), "grpc_details": exc.details()}

    if status_code == grpc.StatusCode.NOT_FOUND:
        return DependencyClientError(
            code="DEPENDENCY_NOT_FOUND",
            message="OMS dependency entity not found.",
            retryable=False,
            details=details,
        )
    if status_code == grpc.StatusCode.INVALID_ARGUMENT:
        return DependencyClientError(
            code="DEPENDENCY_VALIDATION_FAILED",
            message="OMS dependency rejected request validation.",
            retryable=False,
            details=details,
        )
    if status_code in {
        grpc.StatusCode.FAILED_PRECONDITION,
        grpc.StatusCode.PERMISSION_DENIED,
    }:
        return DependencyClientError(
            code="DEPENDENCY_ACCESS_DENIED",
            message="OMS dependency denied request by policy/access.",
            retryable=False,
            details=details,
        )
    if status_code in {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}:
        return DependencyClientError(
            code="DEPENDENCY_UNAVAILABLE",
            message="OMS dependency is unavailable.",
            retryable=True,
            details=details,
        )
    return DependencyClientError(
        code="DEPENDENCY_INTERNAL_FAILURE",
        message="OMS dependency call failed unexpectedly.",
        retryable=False,
        details=details,
    )


class OmsGrpcClient:
    def __init__(
        self,
        stub: Any,
        *,
        timeout_seconds: float = 3.0,
    ) -> None:
        self._stub = stub
        self._timeout_seconds = timeout_seconds

    def submit_order_intent(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        p = normalize_oms_submission_payload(payload)
        wall = p.get("requested_at")
        wall_dt: datetime | None
        if isinstance(wall, datetime):
            wall_dt = wall
        else:
            wall_dt = None

        created_raw = p.get("created_at")
        created_dt: datetime | None
        if isinstance(created_raw, datetime):
            created_dt = created_raw
        else:
            created_dt = None

        ext = _optional_extension(p)

        request = risk_worker_pb2.OrderIntent(
            correlation_id=p["correlation_id"],
            account_id=p["account_id"],
            mode=_mode_enum(p),
            instrument_id=p["instrument_id"],
            side=p["side"],
            order_type=p["order_type"],
            quantity=p["quantity"],
            limit_price=p["limit_price"],
            stop_price=p["stop_price"],
            time_in_force=p["time_in_force"],
            idempotency_key=p["idempotency_key"],
            requested_at=_to_proto_timestamp(wall_dt),
            job_id=p["job_id"],
            order_intent_id=p["order_intent_id"],
            symbol=p["symbol"],
        )
        if created_dt is not None:
            request.created_at.CopyFrom(_to_proto_timestamp(created_dt))
        if ext is not None:
            request.extension.CopyFrom(ext)

        try:
            ack = self._stub.SubmitOrderIntent(request, timeout=self._timeout_seconds)
        except grpc.RpcError as exc:
            raise _normalize_grpc_error(exc) from exc

        rc = str(ack.reason_code or "").strip()
        ec = str(ack.error_code or "").strip()
        return {
            "accepted": bool(ack.accepted),
            "status": risk_worker_pb2.IntentAckStatus.Name(ack.status),
            "order_id": ack.order_id or None,
            "state": ack.state or None,
            "reason_code": rc.upper() if rc else None,
            "error_code": ec.upper() if ec else None,
        }

    def submit_replace_order_intent(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """
        Submit ``ReplaceOrderIntent`` over gRPC (``SubmitReplaceOrderIntent``).

        Exactly one of ``quantity`` or ``order_type`` must be non-empty. Required keys:
        ``job_id``, ``account_id``, ``target_oms_order_id``, ``idempotency_key``,
        ``order_intent_id``. ``correlation_id`` is optional for strategy workers but
        recommended for tracing.
        """
        p = dict(payload)
        job_id = _coerce_decimal_string(p.get("job_id"))
        account_id = _coerce_decimal_string(p.get("account_id"))
        target = _coerce_decimal_string(p.get("target_oms_order_id"))
        idem = _coerce_decimal_string(p.get("idempotency_key"))
        corr = _coerce_decimal_string(p.get("correlation_id"))
        order_intent_id = _coerce_cancel_order_intent_id(p.get("order_intent_id"))
        qty = _coerce_decimal_string(p.get("quantity"))
        ot = _coerce_decimal_string(p.get("order_type"))
        qty_nonempty = bool(qty)
        ot_nonempty = bool(ot)
        if (
            not job_id
            or not account_id
            or not target
            or not idem
            or not order_intent_id
        ):
            raise ValueError(
                "submit_replace_order_intent requires job_id, account_id, "
                "target_oms_order_id, idempotency_key, and order_intent_id"
            )
        if qty_nonempty == ot_nonempty:
            raise ValueError(
                "submit_replace_order_intent requires exactly one of quantity or order_type"
            )
        wall = p.get("requested_at")
        wall_dt: datetime | None
        if isinstance(wall, datetime):
            wall_dt = wall
        elif isinstance(wall, str):
            wall_dt = _parse_rfc3339_utc(wall)
        else:
            wall_dt = None
        created_raw = p.get("created_at")
        created_dt: datetime | None
        if isinstance(created_raw, datetime):
            created_dt = created_raw
        elif isinstance(created_raw, str):
            created_dt = _parse_rfc3339_utc(created_raw)
        else:
            created_dt = None
        ext = _optional_extension(
            {
                **p,
                "correlation_id": corr,
                "account_id": account_id,
                "job_id": job_id,
            }
        )
        request_msg = risk_worker_pb2.ReplaceOrderIntent(
            correlation_id=corr,
            account_id=account_id,
            job_id=job_id,
            target_oms_order_id=target,
            idempotency_key=idem,
            requested_at=_to_proto_timestamp(wall_dt),
            mode=_mode_enum(p),
            order_intent_id=order_intent_id,
            quantity=qty,
            order_type=ot,
        )
        if created_dt is not None:
            request_msg.created_at.CopyFrom(_to_proto_timestamp(created_dt))
        if ext is not None:
            request_msg.extension.CopyFrom(ext)

        try:
            ack = self._stub.SubmitReplaceOrderIntent(
                request_msg, timeout=self._timeout_seconds
            )
        except grpc.RpcError as exc:
            raise _normalize_grpc_error(exc) from exc

        rc = str(ack.reason_code or "").strip()
        ec = str(ack.error_code or "").strip()
        return {
            "accepted": bool(ack.accepted),
            "status": risk_worker_pb2.IntentAckStatus.Name(ack.status),
            "order_id": ack.order_id or None,
            "state": ack.state or None,
            "reason_code": rc.upper() if rc else None,
            "error_code": ec.upper() if ec else None,
        }

    def submit_cancel_order_intent(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """
        Submit ``CancelOrderIntent`` over gRPC (``SubmitCancelOrderIntent``).

        Required keys: ``job_id``, ``account_id``, ``target_oms_order_id``, ``idempotency_key``,
        ``correlation_id``, ``order_intent_id``. Optional: ``created_at`` / ``requested_at``
        (``datetime`` or RFC3339 string), ``mode`` / ``environment``, and extension fields
        consumed by :func:`_optional_extension`.
        """
        p = dict(payload)
        job_id = _coerce_decimal_string(p.get("job_id"))
        account_id = _coerce_decimal_string(p.get("account_id"))
        target = _coerce_decimal_string(p.get("target_oms_order_id"))
        idem = _coerce_decimal_string(p.get("idempotency_key"))
        corr = _coerce_decimal_string(p.get("correlation_id"))
        order_intent_id = _coerce_cancel_order_intent_id(p.get("order_intent_id"))
        if (
            not job_id
            or not account_id
            or not target
            or not idem
            or not corr
            or not order_intent_id
        ):
            raise ValueError(
                "submit_cancel_order_intent requires job_id, account_id, "
                "target_oms_order_id, idempotency_key, correlation_id, and order_intent_id"
            )
        wall = p.get("requested_at")
        wall_dt: datetime | None
        if isinstance(wall, datetime):
            wall_dt = wall
        elif isinstance(wall, str):
            wall_dt = _parse_rfc3339_utc(wall)
        else:
            wall_dt = None
        created_raw = p.get("created_at")
        created_dt: datetime | None
        if isinstance(created_raw, datetime):
            created_dt = created_raw
        elif isinstance(created_raw, str):
            created_dt = _parse_rfc3339_utc(created_raw)
        else:
            created_dt = None
        ext = _optional_extension(
            {
                **p,
                "correlation_id": corr,
                "account_id": account_id,
                "job_id": job_id,
            }
        )
        request_msg = risk_worker_pb2.CancelOrderIntent(
            correlation_id=corr,
            account_id=account_id,
            job_id=job_id,
            target_oms_order_id=target,
            idempotency_key=idem,
            requested_at=_to_proto_timestamp(wall_dt),
            mode=_mode_enum(p),
            order_intent_id=order_intent_id,
        )
        if created_dt is not None:
            request_msg.created_at.CopyFrom(_to_proto_timestamp(created_dt))
        if ext is not None:
            request_msg.extension.CopyFrom(ext)

        try:
            ack = self._stub.SubmitCancelOrderIntent(
                request_msg, timeout=self._timeout_seconds
            )
        except grpc.RpcError as exc:
            raise _normalize_grpc_error(exc) from exc

        rc = str(ack.reason_code or "").strip()
        ec = str(ack.error_code or "").strip()
        return {
            "accepted": bool(ack.accepted),
            "status": risk_worker_pb2.IntentAckStatus.Name(ack.status),
            "order_id": ack.order_id or None,
            "state": ack.state or None,
            "reason_code": rc.upper() if rc else None,
            "error_code": ec.upper() if ec else None,
        }


def build_oms_grpc_client(
    *, target: str, timeout_seconds: float = 3.0
) -> OmsGrpcClient | None:
    """When ``target`` is non-empty (``host:port``), returns an :class:`OmsGrpcClient`."""
    t = (target or "").strip()
    if not t:
        return None
    channel = grpc.insecure_channel(t)
    stub = risk_worker_pb2_grpc.OrderIntentServiceStub(channel)
    return OmsGrpcClient(stub, timeout_seconds=timeout_seconds)

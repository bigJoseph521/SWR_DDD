from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Literal

from runtime.bootstrap.launch_spec import LaunchSpec
from runtime.domain.errors import (
    RuntimeWorkerReasonCode,
    WorkerErrorCode,
)
from runtime.domain.worker_identity import WorkerIdentity
from runtime.integration.oms_gateway import OmsGateway
from runtime.runtime.dependencies import RuntimeDependencies

_LOG = logging.getLogger(__name__)


def _intent_idempotency_key(intent: Any) -> str:
    for attr in ("idempotency_key", "client_order_id"):
        v = getattr(intent, attr, None)
        if v and str(v).strip():
            return str(v).strip()
    return uuid.uuid4().hex


def _intent_quantity_str(intent: Any) -> str:
    q = getattr(intent, "quantity", None)
    if q is None:
        q = getattr(intent, "qty", None)
    return "" if q is None else str(q)


def _enum_value(obj: Any) -> str:
    if obj is None:
        return ""
    v = getattr(obj, "value", obj)
    return str(v)


def _worker_local_requested_at() -> datetime:
    """Wall-clock instant on the worker host when the intent is submitted (``ctx.timer`` is *not* used here)."""
    return datetime.now().astimezone()


def _normalize_utc_dt(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _intent_created_at_sim(intent: Any, clock: Any) -> datetime:
    """
    Fallback when no market snapshot exists: SDK ``created_at``, then ``clock.now()``,
    then worker UTC wall time (see :func:`_intent_created_at_for_payload`).
    """
    created = getattr(intent, "created_at", None)
    if isinstance(created, datetime):
        return _normalize_utc_dt(created)
    now_fn = getattr(clock, "now", None)
    if callable(now_fn):
        try:
            t = now_fn()
            if isinstance(t, datetime):
                return _normalize_utc_dt(t)
        except Exception:
            _LOG.debug(
                "intent_created_at_clock_unavailable_using_wall_time",
                exc_info=True,
            )
    return datetime.now(timezone.utc)


def _intent_created_at_for_payload(
    intent: Any,
    clock: Any,
    *,
    latest_market_event_at: Callable[[], datetime | None] | None,
) -> datetime:
    """
    Time when the intent was created in market/simulation terms: the timestamp of the
    latest bar, tick, or quote (``latest_market_event_at``), when known.

    Falls back to :func:`_intent_created_at_sim` before the first data event or when
    the snapshot callback is omitted.
    """
    if latest_market_event_at is not None:
        try:
            snap = latest_market_event_at()
        except Exception:
            _LOG.debug("intent_created_at_market_snapshot_failed", exc_info=True)
        else:
            if isinstance(snap, datetime):
                return _normalize_utc_dt(snap)
    return _intent_created_at_sim(intent, clock)


def sdk_order_intent_to_oms_grpc_payload(
    intent: Any,
    *,
    launch_spec: LaunchSpec,
    _worker_identity: WorkerIdentity,
    correlation_id: str = "",
) -> dict[str, Any]:
    """Canonical dict for :class:`OmsGrpcClient` / ``risk_worker.proto`` ``OrderIntent``.

    ``runtime_id``, ``tenant_id``, ``account_id``, ``job_id``, ``correlation_id``, ``mode``,
    and ``symbol`` match :class:`~runtime.bootstrap.launch_spec.LaunchSpec` (from
    ``strategy_bundle/setting.json`` / bundle launch payload). ``correlation_id`` falls back
    to the ``correlation_id=`` argument (e.g. env ``oms_correlation_id``) only when the bundle
    omits it, then to a new UUID hex. ``launch_attempt`` is omitted from this dict; the OMS
    gRPC client sets extension ``launch_attempt`` to zero.
    """
    cid = _intent_idempotency_key(intent)
    limit = getattr(intent, "limit_price", None)
    stop = getattr(intent, "stop_price", None)
    ref_price = getattr(intent, "price", None)
    bundle_corr = (launch_spec.correlation_id or "").strip()
    fallback_corr = correlation_id.strip()
    wire_corr = bundle_corr or fallback_corr or uuid.uuid4().hex
    price_field = limit if limit is not None else ref_price
    return {
        "correlation_id": wire_corr,
        "account_id": launch_spec.account_id or launch_spec.trader_id or "",
        "mode": launch_spec.mode.value,
        "instrument_id": getattr(intent, "instrument_id", ""),
        "side": _enum_value(getattr(intent, "side", None)),
        "order_type": _enum_value(getattr(intent, "order_type", None)),
        "time_in_force": _enum_value(getattr(intent, "time_in_force", None)),
        "quantity": _intent_quantity_str(intent),
        "limit_price": "" if price_field is None else str(price_field),
        "stop_price": "" if stop is None else str(stop),
        "idempotency_key": cid,
        "runtime_id": launch_spec.runtime_id,
        "strategy_version_id": launch_spec.strategy_version_id,
        "tenant_id": launch_spec.tenant_id,
        "job_id": (launch_spec.job_id or "").strip(),
        "symbol": (launch_spec.symbol or "").strip(),
    }


def sdk_order_intent_to_backtest_grpc_payload(
    intent: Any,
    *,
    launch_spec: LaunchSpec,
    _worker_identity: WorkerIdentity,
) -> dict[str, Any]:
    """Payload shape expected by :class:`ReplayGrpcClient.submit_backtest_order_intent`.

    Identity fields ``account_id``, ``runtime_id``, ``job_id``, ``correlation_id``,
    and ``symbol`` align with :class:`~runtime.bootstrap.launch_spec.LaunchSpec`
    (bundle ``setting.json``). ``account_id`` uses the same rule as OMS:
    ``launch_spec.account_id`` or ``launch_spec.trader_id``, else empty string.
    """
    cid = _intent_idempotency_key(intent)
    limit = getattr(intent, "limit_price", None)
    stop = getattr(intent, "stop_price", None)
    return {
        "correlation_id": (launch_spec.correlation_id or "").strip(),
        "account_id": launch_spec.account_id or launch_spec.trader_id or "",
        "runtime_id": launch_spec.runtime_id,
        "job_id": (launch_spec.job_id or "").strip(),
        "symbol": (launch_spec.symbol or "").strip(),
        "instrument_id": getattr(intent, "instrument_id", ""),
        "side": _enum_value(getattr(intent, "side", None)),
        "order_type": _enum_value(getattr(intent, "order_type", None)),
        "time_in_force": _enum_value(getattr(intent, "time_in_force", None)),
        "quantity": _intent_quantity_str(intent),
        "limit_price": "" if limit is None else str(limit),
        "stop_price": "" if stop is None else str(stop),
        "idempotency_key": cid,
    }


def build_sdk_order_intent_submitter(
    *,
    dependencies: RuntimeDependencies,
    launch_spec: LaunchSpec,
    worker_identity: WorkerIdentity,
    on_order_intent_result: (
        Callable[[Literal["oms", "backtest"], dict[str, Any], dict[str, Any]], None]
        | None
    ) = None,
    oms_correlation_id: str = "",
    disable_order_intent_grpc: bool = False,
    latest_market_event_at: Callable[[], datetime | None] | None = None,
    allocate_order_intent_id: Callable[[], str] | None = None,
) -> Callable[[Any], dict[str, Any]] | None:
    """
    Returns a callable that submits SDK :class:`OrderIntent` to risk-service gRPC
    (``risk_worker.proto`` / ``OrderIntentService``) for every :class:`RuntimeMode`, or
    ``None`` when no :class:`~runtime.integration.oms_gateway.OmsGateway` is configured.

    ``latest_market_event_at`` (optional) supplies the timestamp of the latest bar/tick/quote
    for ``payload['created_at']`` and the order intent journal; when it returns ``None``,
    the submitter falls back to the SDK intent's ``created_at``, then the runtime clock.

    ``allocate_order_intent_id`` (optional) supplies UUID strings for
    ``payload['order_intent_id']``. When omitted, a fresh :func:`uuid.uuid4` is used per intent.
    """
    if allocate_order_intent_id is not None:
        next_order_intent_id: Callable[[], str] = allocate_order_intent_id
    else:

        def next_order_intent_id() -> str:
            return str(uuid.uuid4())

    oms = dependencies.oms
    if not isinstance(oms, OmsGateway):
        return None
    oms_inner = getattr(oms, "_oms_client", None)
    can_call_oms = callable(getattr(oms_inner, "submit_order_intent", None))

    def _submit(intent: Any) -> dict[str, Any]:
        _unavailable = {
            "accepted": False,
            "reason_code": RuntimeWorkerReasonCode.BOUND_DEPENDENCY_UNAVAILABLE.value,
            "error_code": WorkerErrorCode.DEPENDENCY_UNHEALTHY.value,
        }
        payload: dict[str, Any] = {}
        result: dict[str, Any] = dict(_unavailable)
        try:
            payload = sdk_order_intent_to_oms_grpc_payload(
                intent,
                launch_spec=launch_spec,
                _worker_identity=worker_identity,
                correlation_id=oms_correlation_id,
            )
            payload["order_intent_id"] = next_order_intent_id()
            payload["created_at"] = _intent_created_at_for_payload(
                intent,
                dependencies.clock,
                latest_market_event_at=latest_market_event_at,
            )
            payload["requested_at"] = _worker_local_requested_at()
            if disable_order_intent_grpc or not can_call_oms:
                result = dict(_unavailable)
            else:
                result = oms.submit_order_intent_payload(payload)
        except Exception:
            _LOG.exception("risk_order_intent_submit_prepare_failed")
            payload = {
                "_prepare_failed": True,
                "runtime_id": launch_spec.runtime_id,
                "instrument_id": str(getattr(intent, "instrument_id", "") or ""),
                "symbol": (launch_spec.symbol or "").strip(),
            }
            result = dict(_unavailable)
        if on_order_intent_result is not None:
            try:
                on_order_intent_result("oms", payload, result)
            except Exception:
                _LOG.exception("on_order_intent_result_callback_failed")
        return result

    if (
        not disable_order_intent_grpc and can_call_oms
    ) or on_order_intent_result is not None:
        return _submit
    return None


def safe_submit_sdk_order_intent(
    submit: Callable[[Any], dict[str, Any]],
    intent: Any,
) -> None:
    try:
        submit(intent)
    except Exception:
        _LOG.exception("sdk_order_intent_grpc_submit_failed")

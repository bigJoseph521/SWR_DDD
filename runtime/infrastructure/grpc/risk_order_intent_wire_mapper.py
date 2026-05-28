from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable

from runtime.domain.launch_spec import LaunchSpec
from runtime.domain.enums import OrderIntentSide, OrderIntentType
from runtime.domain.errors import (
    ORDER_INTENT_WIRE_MAPPING_MISSING_CORRELATION_ID,
    OrderIntentWireMappingError,
)
from runtime.domain.model.platform_trace_spec import PlatformTraceSpec
from runtime.domain.model.strategy_order_intent import StrategyOrderIntent

_LOG = logging.getLogger(__name__)


def _enum_value(obj: Any) -> str:
    if obj is None:
        return ""
    v = getattr(obj, "value", obj)
    return str(v)


def _normalize_side(raw: Any) -> OrderIntentSide:
    if raw is None:
        return OrderIntentSide.BUY
    return OrderIntentSide(_enum_value(raw).upper())


def _normalize_order_type(raw: Any) -> OrderIntentType:
    if raw is None:
        return OrderIntentType.MARKET
    return OrderIntentType(_enum_value(raw).upper())


def _normalize_utc_dt(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _intent_idempotency_key(intent: StrategyOrderIntent) -> str:
    if intent.client_order_id:
        return intent.client_order_id
    return uuid.uuid4().hex


def strategy_order_intent_from_sdk(intent: Any) -> StrategyOrderIntent:
    """Map SDK / duck-typed order intent objects to :class:`StrategyOrderIntent`."""
    limit = getattr(intent, "limit_price", None)
    stop = getattr(intent, "stop_price", None)
    qty = getattr(intent, "quantity", None)
    if qty is None:
        qty = getattr(intent, "qty", None)

    side_raw = getattr(intent, "side", None)
    side = _normalize_side(side_raw)

    order_type_raw = getattr(intent, "order_type", None)
    order_type = _normalize_order_type(order_type_raw)

    limit_dec: Decimal | None
    if order_type is OrderIntentType.MARKET:
        limit_dec = None
    elif limit is not None:
        limit_dec = Decimal(str(limit))
    else:
        limit_dec = None

    stop_dec = None if stop is None else Decimal(str(stop))
    created = getattr(intent, "created_at", None)
    client_id = getattr(intent, "client_order_id", None) or getattr(
        intent, "idempotency_key", None
    )

    return StrategyOrderIntent(
        instrument_id=str(getattr(intent, "instrument_id", "") or "").strip(),
        side=side,
        order_type=order_type,
        quantity=Decimal(str(qty if qty is not None else "0")),
        limit_price=limit_dec,
        stop_price=stop_dec,
        time_in_force=_enum_value(getattr(intent, "time_in_force", None)) or None,
        client_order_id=str(client_id).strip() if client_id else None,
        created_at=created if isinstance(created, datetime) else None,
    )


@dataclass(frozen=True, slots=True)
class OrderSubmissionContext:
    """Platform metadata used when serializing :class:`StrategyOrderIntent` for egress."""

    launch_spec: LaunchSpec
    correlation_id_fallback: str = ""
    platform_trace: PlatformTraceSpec | None = None
    allocate_order_intent_id: Callable[[], str] | None = None
    requested_at: datetime | None = None

    def next_order_intent_id(self) -> str:
        if self.allocate_order_intent_id is not None:
            return self.allocate_order_intent_id()
        return str(uuid.uuid4())

    def effective_requested_at(self) -> datetime:
        if self.requested_at is not None:
            return self.requested_at
        return datetime.now().astimezone()


def _resolve_wire_correlation_id(context: OrderSubmissionContext) -> str:
    """
    Resolve Risk egress correlation_id with fail-closed semantics.

    Precedence:
    1. launch/bundle ``correlation_id`` on :class:`LaunchSpec`
    2. :meth:`PlatformTraceSpec.effective_correlation_id` when platform trace is set
    3. validated runtime fallback ``correlation_id_fallback`` on submission context
    """
    launch_spec = context.launch_spec
    bundle_corr = (launch_spec.correlation_id or "").strip()
    if bundle_corr:
        return bundle_corr

    if context.platform_trace is not None:
        trace_corr = context.platform_trace.effective_correlation_id(
            env_fallback=context.correlation_id_fallback
        )
        if trace_corr:
            return trace_corr.strip()

    fallback_corr = (context.correlation_id_fallback or "").strip()
    if fallback_corr:
        return fallback_corr

    diagnostics: dict[str, Any] = {
        "reason_code": ORDER_INTENT_WIRE_MAPPING_MISSING_CORRELATION_ID,
        "runtime_id": launch_spec.runtime_id,
        "strategy_version_id": launch_spec.strategy_version_id,
        "account_id": launch_spec.account_id or launch_spec.trader_id or "",
        "job_id": (launch_spec.job_id or "").strip(),
    }
    if context.platform_trace is not None:
        if context.platform_trace.strategy_id:
            diagnostics["strategy_id"] = context.platform_trace.strategy_id
        if context.platform_trace.request_id:
            diagnostics["request_id"] = context.platform_trace.request_id

    _LOG.error("risk_order_intent_wire_missing_correlation_id", extra=diagnostics)
    raise OrderIntentWireMappingError(
        reason_code=ORDER_INTENT_WIRE_MAPPING_MISSING_CORRELATION_ID,
        diagnostics=diagnostics,
    )


def build_risk_order_intent_wire_payload(
    intent: StrategyOrderIntent,
    *,
    context: OrderSubmissionContext,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    """
    Build the Risk Service wire dict (``risk_worker.proto`` / ``OrderIntent``).

    Application supplies :class:`StrategyOrderIntent`; infrastructure attaches
    runtime identity, correlation, and egress timestamps here.
    """
    launch_spec = context.launch_spec
    wire_corr = _resolve_wire_correlation_id(context)

    price_field = intent.limit_price
    effective_created = created_at if created_at is not None else intent.created_at
    if effective_created is not None:
        effective_created = _normalize_utc_dt(effective_created)

    payload: dict[str, Any] = {
        "correlation_id": wire_corr,
        "account_id": launch_spec.account_id or launch_spec.trader_id or "",
        "mode": launch_spec.mode.value,
        "instrument_id": intent.instrument_id,
        "side": intent.side.value,
        "order_type": intent.order_type.value,
        "time_in_force": intent.time_in_force or "",
        "quantity": str(intent.quantity),
        "limit_price": "" if price_field is None else str(price_field),
        "stop_price": "" if intent.stop_price is None else str(intent.stop_price),
        "idempotency_key": _intent_idempotency_key(intent),
        "runtime_id": launch_spec.runtime_id,
        "strategy_version_id": launch_spec.strategy_version_id,
        "tenant_id": launch_spec.tenant_id,
        "job_id": (launch_spec.job_id or "").strip(),
        "symbol": (launch_spec.symbol or "").strip(),
        "order_intent_id": context.next_order_intent_id(),
        "requested_at": context.effective_requested_at(),
    }
    if effective_created is not None:
        payload["created_at"] = effective_created
    return payload

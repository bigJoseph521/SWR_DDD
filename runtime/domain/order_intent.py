from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from runtime.domain.enums import (
    OrderIntentSide,
    OrderIntentType,
    RuntimeMode,
)
from runtime.domain.errors import (
    OrderIntentValidationError,
    UnsupportedModeError,
)


def _require_non_empty(value: str, field_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise OrderIntentValidationError(
            field_name=field_name, reason="must_not_be_blank"
        )
    return normalized


def _normalize_optional(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        raise OrderIntentValidationError(
            field_name=field_name,
            reason="must_not_be_blank_if_provided",
        )
    return normalized


def _to_decimal(value: Decimal | str | int | float, field_name: str) -> Decimal:
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except Exception as exc:  # pragma: no cover - defensive
        raise OrderIntentValidationError(
            field_name=field_name, reason="invalid_decimal"
        ) from exc
    return parsed


@dataclass(frozen=True, slots=True)
class OrderIntent:
    idempotency_key: str
    runtime_id: str
    strategy_version_id: str
    mode: RuntimeMode
    instrument_id: str
    side: OrderIntentSide
    order_type: OrderIntentType
    quantity: Decimal
    limit_price: Decimal | None = None
    stop_price: Decimal | None = None
    time_in_force: str | None = None
    correlation_id: str | None = None
    causation_id: str | None = None
    #: Market/simulation time when the intent was formed (latest bar/tick/quote); runtime may override.
    created_at: datetime | None = None
    #: Wall-clock time on the worker host when the intent was submitted.
    requested_at: datetime | None = None
    #: Worker-assigned intent UUID when set (matches ``risk_worker`` wire / journal).
    order_intent_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "idempotency_key",
            _require_non_empty(self.idempotency_key, "idempotency_key"),
        )
        object.__setattr__(
            self, "runtime_id", _require_non_empty(self.runtime_id, "runtime_id")
        )
        object.__setattr__(
            self,
            "strategy_version_id",
            _require_non_empty(self.strategy_version_id, "strategy_version_id"),
        )
        object.__setattr__(
            self,
            "instrument_id",
            _require_non_empty(self.instrument_id, "instrument_id"),
        )

        optionals = ("time_in_force", "correlation_id", "causation_id")
        for field_name in optionals:
            object.__setattr__(
                self,
                field_name,
                _normalize_optional(getattr(self, field_name), field_name),
            )

        mode = self.mode
        if isinstance(mode, str):
            try:
                mode = RuntimeMode(mode)
            except ValueError as exc:
                raise UnsupportedModeError(
                    mode=str(self.mode), reason="invalid_mode"
                ) from exc
            object.__setattr__(self, "mode", mode)

        side = self.side
        if isinstance(side, str):
            try:
                side = OrderIntentSide(side)
            except ValueError as exc:
                raise OrderIntentValidationError(
                    field_name="side", reason="invalid_side"
                ) from exc
            object.__setattr__(self, "side", side)

        order_type = self.order_type
        if isinstance(order_type, str):
            try:
                order_type = OrderIntentType(order_type)
            except ValueError as exc:
                raise OrderIntentValidationError(
                    field_name="order_type",
                    reason="invalid_order_type",
                ) from exc
            object.__setattr__(self, "order_type", order_type)

        qty = _to_decimal(self.quantity, "quantity")
        if qty <= 0:
            raise OrderIntentValidationError(
                field_name="quantity", reason="must_be_positive"
            )
        object.__setattr__(self, "quantity", qty)

        if self.limit_price is not None:
            limit_price = _to_decimal(self.limit_price, "limit_price")
            if limit_price <= 0:
                raise OrderIntentValidationError(
                    field_name="limit_price",
                    reason="must_be_positive_when_provided",
                )
            object.__setattr__(self, "limit_price", limit_price)

        if self.stop_price is not None:
            stop_price = _to_decimal(self.stop_price, "stop_price")
            if stop_price <= 0:
                raise OrderIntentValidationError(
                    field_name="stop_price",
                    reason="must_be_positive_when_provided",
                )
            object.__setattr__(self, "stop_price", stop_price)

        if self.order_type is OrderIntentType.LIMIT and self.limit_price is None:
            raise OrderIntentValidationError(
                field_name="limit_price",
                reason="required_for_limit_order",
            )

        if self.order_type is OrderIntentType.STOP and self.stop_price is None:
            raise OrderIntentValidationError(
                field_name="stop_price",
                reason="required_for_stop_order",
            )

        if self.order_type is OrderIntentType.STOP_LIMIT:
            if self.stop_price is None or self.limit_price is None:
                raise OrderIntentValidationError(
                    field_name="prices",
                    reason="limit_and_stop_prices_required_for_stop_limit_order",
                )

        for name in ("created_at", "requested_at"):
            raw_dt = getattr(self, name)
            if raw_dt is None:
                continue
            if not isinstance(raw_dt, datetime):
                raise OrderIntentValidationError(
                    field_name=name,
                    reason="must_be_datetime_or_none",
                )

        oid = self.order_intent_id
        if oid is not None:
            if isinstance(oid, int):
                object.__setattr__(self, "order_intent_id", str(oid))
            elif not isinstance(oid, str) or not oid.strip():
                raise OrderIntentValidationError(
                    field_name="order_intent_id",
                    reason="must_be_non_empty_string_when_provided",
                )
            else:
                object.__setattr__(self, "order_intent_id", oid.strip())

    def validate_for_oms_submission(self) -> None:
        """No-op hook before risk / OMS wire egress; :class:`OrderIntent` is validated in ``__post_init__``."""
        return None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "idempotency_key": self.idempotency_key,
            "runtime_id": self.runtime_id,
            "strategy_version_id": self.strategy_version_id,
            "mode": self.mode.value,
            "instrument_id": self.instrument_id,
            "side": self.side.value,
            "order_type": self.order_type.value,
            "quantity": str(self.quantity),
            "limit_price": None if self.limit_price is None else str(self.limit_price),
            "stop_price": None if self.stop_price is None else str(self.stop_price),
            "time_in_force": self.time_in_force,
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
        }
        if self.created_at is not None:
            out["created_at"] = self.created_at
        if self.requested_at is not None:
            out["requested_at"] = self.requested_at
        if self.order_intent_id is not None:
            out["order_intent_id"] = self.order_intent_id
        return out

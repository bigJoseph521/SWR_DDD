from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from runtime.domain.enums import OrderIntentSide, OrderIntentType
from runtime.domain.errors import OrderIntentValidationError


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
class StrategyOrderIntent:
    """
    Strategy-facing order decision (transport- and runtime-metadata-free).

    Platform fields such as ``runtime_id``, ``correlation_id``, and ``mode`` are
    attached when building outbound wire DTOs in infrastructure.
    """

    instrument_id: str
    side: OrderIntentSide
    order_type: OrderIntentType
    quantity: Decimal
    limit_price: Decimal | None = None
    stop_price: Decimal | None = None
    time_in_force: str | None = None
    #: Strategy/client idempotency hint (optional); runtime may generate if absent.
    client_order_id: str | None = None
    #: Market/simulation time when the intent was formed (optional until submit).
    created_at: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "instrument_id",
            _require_non_empty(self.instrument_id, "instrument_id"),
        )

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

        object.__setattr__(
            self,
            "time_in_force",
            _normalize_optional(self.time_in_force, "time_in_force"),
        )
        object.__setattr__(
            self,
            "client_order_id",
            _normalize_optional(self.client_order_id, "client_order_id"),
        )

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

        raw_dt = self.created_at
        if raw_dt is not None and not isinstance(raw_dt, datetime):
            raise OrderIntentValidationError(
                field_name="created_at",
                reason="must_be_datetime_or_none",
            )

    def with_created_at(self, created_at: datetime) -> StrategyOrderIntent:
        return StrategyOrderIntent(
            instrument_id=self.instrument_id,
            side=self.side,
            order_type=self.order_type,
            quantity=self.quantity,
            limit_price=self.limit_price,
            stop_price=self.stop_price,
            time_in_force=self.time_in_force,
            client_order_id=self.client_order_id,
            created_at=created_at,
        )

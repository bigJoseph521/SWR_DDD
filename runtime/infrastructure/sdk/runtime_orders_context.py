from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from alphovex_sdk.context.orders_context import OrdersContext
from alphovex_sdk.enums.order import OrderSide, OrderType
from alphovex_sdk.models.order import Order, OrderId, OrderIntent
from alphovex_sdk.typedefs import InstrumentId


class RuntimeOrdersContext(OrdersContext):
    """Delegates to SDK :class:`DefaultOrderService` / :class:`GrpcSubmittingOrderService`."""

    __slots__ = ("_svc",)

    def __init__(self, *, order_service: object) -> None:
        self._svc = order_service

    def buy(  # type: ignore[override]
        self,
        first: OrderIntent | InstrumentId,
        /,
        qty: float | None = None,
        **kwargs: Any,
    ) -> None:
        fn = getattr(self._svc, "buy", None)
        if not callable(fn):
            raise NotImplementedError("order service has no buy()")
        if isinstance(first, OrderIntent):
            fn(first)
            return
        if qty is None:
            raise TypeError(
                "buy(instrument_id, qty=...) requires qty= when not passing OrderIntent"
            )
        price = float(kwargs.get("price") or kwargs.get("reference_price") or 1.0)
        fn(
            OrderIntent(
                instrument_id=str(first),
                side=OrderSide.BUY,
                quantity=float(qty),
                price=price,
                order_type=OrderType.MARKET,
            )
        )

    def sell(  # type: ignore[override]
        self,
        first: OrderIntent | InstrumentId,
        /,
        qty: float | None = None,
        **kwargs: Any,
    ) -> None:
        fn = getattr(self._svc, "sell", None)
        if not callable(fn):
            raise NotImplementedError("order service has no sell()")
        if isinstance(first, OrderIntent):
            fn(first)
            return
        if qty is None:
            raise TypeError(
                "sell(instrument_id, qty=...) requires qty= when not passing OrderIntent"
            )
        price = float(kwargs.get("price") or kwargs.get("reference_price") or 1.0)
        fn(
            OrderIntent(
                instrument_id=str(first),
                side=OrderSide.SELL,
                quantity=float(qty),
                price=price,
                order_type=OrderType.MARKET,
            )
        )

    def cancel_all(self) -> None:
        fn = getattr(self._svc, "cancel_all", None)
        if callable(fn):
            fn()

    def cancel_for_instrument(self, instrument_id: InstrumentId) -> None:
        fn = getattr(self._svc, "cancel_for_instrument", None)
        if callable(fn):
            fn(instrument_id)

    def cancel(self, order_id: OrderId) -> None:
        fn = getattr(self._svc, "cancel", None)
        if callable(fn):
            fn(order_id)

    def list(self) -> Sequence[Order]:
        fn = getattr(self._svc, "list", None)
        if callable(fn):
            return fn()
        return ()

    def get(self, order_id: OrderId) -> Order | None:
        fn = getattr(self._svc, "get", None)
        if callable(fn):
            return fn(order_id)
        return None

    def active(self) -> Sequence[Order]:
        fn = getattr(self._svc, "active", None)
        if callable(fn):
            return fn()
        return ()

    def done(self) -> Sequence[Order]:
        fn = getattr(self._svc, "done", None)
        if callable(fn):
            return fn()
        return ()

    def filled(self) -> Sequence[Order]:
        fn = getattr(self._svc, "filled", None)
        if callable(fn):
            return fn()
        return ()

    def for_instrument(self, instrument_id: InstrumentId) -> Sequence[Order]:
        fn = getattr(self._svc, "for_instrument", None)
        if callable(fn):
            return fn(instrument_id)
        return ()

    def active_for_instrument(self, instrument_id: InstrumentId) -> Sequence[Order]:
        fn = getattr(self._svc, "active_for_instrument", None)
        if callable(fn):
            return fn(instrument_id)
        return ()

    def has_active_for_instrument(self, instrument_id: InstrumentId) -> bool:
        fn = getattr(self._svc, "has_active_for_instrument", None)
        if callable(fn):
            return bool(fn(instrument_id))
        return False

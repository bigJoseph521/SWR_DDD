"""Serialize captured replay order intents to stdio ``ORDER_INTENTS`` wire dicts."""

from __future__ import annotations

from typing import Any

from runtime.domain.launch_spec import LaunchSpec
from runtime.infrastructure.backtest_runner.order_intent_collector import (
    StdioOrderIntentCollector,
)
from runtime.infrastructure.strategy_loader.runtime_stub_support import (
    RuntimeOrderIntent,
)


class OrderIntentWireError(ValueError):
    """Raised when a captured intent cannot be encoded for the runner."""


def _enum_str(value: object) -> str:
    if value is None:
        return ""
    raw = getattr(value, "value", value)
    return str(raw).strip().upper()


def _wire_decimal(value: float | None) -> str | None:
    if value is None:
        return None
    text = f"{float(value):.8f}".rstrip("0").rstrip(".")
    return text or "0"


def runtime_intent_to_wire(
    intent: RuntimeOrderIntent,
    *,
    launch_spec: LaunchSpec,
    client_order_id: str,
) -> dict[str, Any]:
    instrument_id = str(intent.instrument_id or "").strip()
    if not instrument_id:
        raise OrderIntentWireError("order intent missing instrument_id")
    symbol = (launch_spec.symbol or instrument_id).strip()
    if not symbol:
        raise OrderIntentWireError("order intent missing symbol")

    quantity = _wire_decimal(intent.quantity)
    if not quantity:
        raise OrderIntentWireError("order intent missing quantity")

    side = _enum_str(intent.side)
    order_type = _enum_str(intent.order_type)
    if not side or not order_type:
        raise OrderIntentWireError("order intent missing side or order_type")

    wire: dict[str, Any] = {
        "action": "NEW",
        "client_order_id": client_order_id,
        "instrument_id": instrument_id,
        "symbol": symbol,
        "side": side,
        "order_type": order_type,
        "quantity": quantity,
    }
    limit_price = _wire_decimal(intent.limit_price)
    if limit_price is not None:
        wire["limit_price"] = limit_price
    stop_price = _wire_decimal(intent.stop_price)
    if stop_price is not None:
        wire["stop_price"] = stop_price
    tif = _enum_str(intent.time_in_force)
    if tif:
        wire["time_in_force"] = tif
    return wire


def drain_collector_to_wire_intents(
    collector: StdioOrderIntentCollector,
    *,
    launch_spec: LaunchSpec,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for intent in collector.drain():
        client_order_id = collector.allocate_client_order_id(intent)
        out.append(
            runtime_intent_to_wire(
                intent,
                launch_spec=launch_spec,
                client_order_id=client_order_id,
            )
        )
    return out

"""Console / stdout helpers for order intent (worker → risk wire shape)."""

from __future__ import annotations

from datetime import datetime, timezone

from runtime.infrastructure.grpc.risk_order_intent_client import (
    ORDER_INTENT_DISPLAY_FIELD_NAMES,
    order_intent_wire_dict_for_console,
)


def test_order_intent_wire_dict_includes_every_display_field() -> None:
    wire = order_intent_wire_dict_for_console(
        {
            "instrument_id": "AAPL",
            "quantity": "1",
            "mode": "BACKTEST",
        }
    )
    assert set(wire) == set(ORDER_INTENT_DISPLAY_FIELD_NAMES)
    for name in ORDER_INTENT_DISPLAY_FIELD_NAMES:
        assert name in wire


def test_order_intent_wire_dict_serializes_datetimes_to_rfc3339_z() -> None:
    t0 = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 5, 1, 12, 0, 1, tzinfo=timezone.utc)
    wire = order_intent_wire_dict_for_console(
        {
            "instrument_id": "AAPL",
            "quantity": "1",
            "created_at": t0,
            "requested_at": t1,
        }
    )
    assert wire["created_at"] == "2026-05-01T12:00:00Z"
    assert wire["requested_at"] == "2026-05-01T12:00:01Z"

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RuntimeChannelSpec:
    """Transport channel names for input/output adapters only."""

    market_data_channel: str | None
    order_intent_report_channel: str | None
    portfolio_update_channel: str | None

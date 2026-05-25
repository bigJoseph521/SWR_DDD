"""
Placeholder stdio message types for BACKTEST runner → worker stdin input.

Payload shape is intentionally not finalized. Only message type labels are defined here.
"""

from __future__ import annotations

from enum import StrEnum


class StdinMessageType(StrEnum):
    """Allowed stdin JSON-line message types (skeleton; contract TBD)."""

    MARKET_DATA = "MARKET_DATA"
    CONTROL_STOP = "CONTROL_STOP"
    END_OF_STREAM = "END_OF_STREAM"


ALLOWED_STDIN_MESSAGE_TYPES: frozenset[str] = frozenset(
    member.value for member in StdinMessageType
)

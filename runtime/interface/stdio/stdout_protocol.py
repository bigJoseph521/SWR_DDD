"""Stdout JSONL message types for BACKTEST worker → runner output."""

from __future__ import annotations

from enum import StrEnum


class StdoutMessageType(StrEnum):
    """Allowed stdout JSON-line message types for BACKTEST egress."""

    ORDER_INTENT = "ORDER_INTENT"


ALLOWED_STDOUT_MESSAGE_TYPES: frozenset[str] = frozenset(
    member.value for member in StdoutMessageType
)

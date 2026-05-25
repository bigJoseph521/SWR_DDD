from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from runtime.application.ports.clock_port import ClockPort


@dataclass(frozen=True, slots=True)
class RuntimeDependencies:
    """Application-facing runtime dependency bundle (concrete types wired in bootstrap)."""

    clock: ClockPort
    manager: Any
    risk_order_intent: Any | None

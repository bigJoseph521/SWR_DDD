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
    replay: Any | None

    @property
    def oms(self) -> Any | None:
        """Deprecated alias for :attr:`risk_order_intent`."""
        return self.risk_order_intent

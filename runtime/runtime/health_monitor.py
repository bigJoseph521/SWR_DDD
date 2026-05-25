from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Mapping

from runtime.events.dedupe import DedupeDecision


@dataclass(slots=True)
class HealthMonitor:
    manager_gateway: object
    launch_attempt: int

    def emit_unhealthy(
        self,
        *,
        occurred_at: datetime,
        observed_at: datetime,
        reason_code: str,
        details: Mapping[str, object] | None = None,
        attempted_launch_attempt: int | None = None,
    ) -> DedupeDecision:
        candidate_attempt = attempted_launch_attempt or self.launch_attempt
        if candidate_attempt < self.launch_attempt:
            return DedupeDecision.DROP_STALE_ATTEMPT

        emitter = getattr(self.manager_gateway, "emit_unhealthy", None)
        if not callable(emitter):
            raise TypeError("manager_gateway must expose emit_unhealthy.")
        result = emitter(
            occurred_at=occurred_at,
            observed_at=observed_at,
            reason_code=reason_code,
            details=details,
        )
        if isinstance(result, dict) and result.get("suppressed") is True:
            decision = str(
                result.get("decision") or DedupeDecision.DROP_DUPLICATE.value
            )
            return DedupeDecision(decision)
        return DedupeDecision.ACCEPT

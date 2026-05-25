from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from runtime.events.dedupe import DedupeDecision


@dataclass(slots=True)
class HeartbeatService:
    manager_gateway: object
    runtime_id: str
    launch_attempt: int

    def emit(
        self,
        *,
        local_state: str | None,
        observed_at: datetime,
        attempted_launch_attempt: int | None = None,
        source: str = "HEARTBEAT",
        reason_code: str | None = None,
        message: str | None = None,
    ) -> DedupeDecision:
        candidate_attempt = attempted_launch_attempt or self.launch_attempt
        if candidate_attempt < self.launch_attempt:
            return DedupeDecision.DROP_STALE_ATTEMPT

        emitter = getattr(self.manager_gateway, "emit_heartbeat", None)
        if not callable(emitter):
            raise TypeError("manager_gateway must expose emit_heartbeat.")

        result = emitter(
            local_state=local_state,
            observed_at=observed_at,
            source=source,
            reason_code=reason_code,
            message=message,
        )
        if isinstance(result, dict) and result.get("suppressed") is True:
            decision = str(
                result.get("decision") or DedupeDecision.DROP_DUPLICATE.value
            )
            return DedupeDecision(decision)
        return DedupeDecision.ACCEPT

    def emit_from_payload(self, payload: dict[str, Any]) -> DedupeDecision:
        observed_at = payload["observed_at"]
        if not isinstance(observed_at, datetime):
            raise ValueError("Heartbeat payload must include datetime observed_at.")
        return self.emit(
            local_state=payload.get("local_state"),
            observed_at=observed_at,
            attempted_launch_attempt=(
                int(payload["launch_attempt"]) if "launch_attempt" in payload else None
            ),
        )

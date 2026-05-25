from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from runtime.events.event_envelope import LifecycleEventEnvelope


class DedupeDecision(StrEnum):
    ACCEPT = "ACCEPT"
    DROP_DUPLICATE = "DROP_DUPLICATE"
    DROP_STALE_ATTEMPT = "DROP_STALE_ATTEMPT"


@dataclass(slots=True)
class LifecycleSignalDedupe:
    _latest_attempt_by_runtime: dict[str, int] = field(default_factory=dict)
    _seen_identity_keys: set[tuple[str, int, str, str, str]] = field(
        default_factory=set
    )

    def decide(
        self,
        *,
        envelope: LifecycleEventEnvelope,
        current_launch_attempt: int,
    ) -> DedupeDecision:
        latest_seen = self._latest_attempt_by_runtime.get(
            envelope.runtime_id, current_launch_attempt
        )
        effective_current = max(current_launch_attempt, latest_seen)

        if envelope.launch_attempt < effective_current:
            return DedupeDecision.DROP_STALE_ATTEMPT

        key = (
            envelope.runtime_id,
            envelope.launch_attempt,
            envelope.event_family,
            envelope.event_name,
            envelope.identity_key,
        )
        if key in self._seen_identity_keys:
            return DedupeDecision.DROP_DUPLICATE

        self._latest_attempt_by_runtime[envelope.runtime_id] = max(
            envelope.launch_attempt,
            latest_seen,
        )
        self._seen_identity_keys.add(key)
        return DedupeDecision.ACCEPT

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping


def _require_non_empty(value: str, *, field_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be blank.")
    return normalized


@dataclass(frozen=True, slots=True)
class LifecycleEventEnvelope:
    event_id: str
    event_type: str
    event_name: str
    event_family: str
    event_version: int
    occurred_at: datetime
    correlation_id: str
    producer: str
    payload: Mapping[str, Any]
    runtime_id: str
    launch_attempt: int
    worker_identity: str
    identity_key: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "event_id", _require_non_empty(self.event_id, field_name="event_id")
        )
        object.__setattr__(
            self,
            "event_type",
            _require_non_empty(self.event_type, field_name="event_type"),
        )
        object.__setattr__(
            self,
            "event_name",
            _require_non_empty(self.event_name, field_name="event_name"),
        )
        object.__setattr__(
            self,
            "event_family",
            _require_non_empty(self.event_family, field_name="event_family"),
        )
        object.__setattr__(
            self,
            "correlation_id",
            _require_non_empty(self.correlation_id, field_name="correlation_id"),
        )
        object.__setattr__(
            self, "producer", _require_non_empty(self.producer, field_name="producer")
        )
        object.__setattr__(
            self,
            "runtime_id",
            _require_non_empty(self.runtime_id, field_name="runtime_id"),
        )
        object.__setattr__(
            self,
            "worker_identity",
            _require_non_empty(self.worker_identity, field_name="worker_identity"),
        )
        object.__setattr__(
            self,
            "identity_key",
            _require_non_empty(self.identity_key, field_name="identity_key"),
        )
        if self.event_version < 1:
            raise ValueError("event_version must be >= 1.")
        if self.launch_attempt < 1:
            raise ValueError("launch_attempt must be >= 1.")
        if self.occurred_at.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware.")

    def to_manager_signal(
        self,
        *,
        signal_type: str,
        identity: Mapping[str, Any],
    ) -> dict[str, Any]:
        envelope = {
            "signal_type": signal_type,
            "identity": dict(identity),
            "payload": dict(self.payload),
            "event_id": self.event_id,
            "event_type": self.event_type,
            "event_name": self.event_name,
            "event_family": self.event_family,
            "event_version": self.event_version,
            "occurred_at": self.occurred_at,
            "correlation_id": self.correlation_id,
            "producer": self.producer,
        }
        return envelope

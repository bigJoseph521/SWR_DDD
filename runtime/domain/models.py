from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from runtime.domain.enums import WorkerPhase
from runtime.domain.launch_context import LaunchContext
from runtime.domain.order_intent import OrderIntent
from runtime.domain.worker_identity import WorkerIdentity


def _require_aware_utc(dt: datetime, field_name: str) -> datetime:
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return dt.astimezone(timezone.utc)


def _serialize_datetime(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class RuntimeStateSnapshot:
    identity: WorkerIdentity
    phase: WorkerPhase
    reason_code: str | None
    created_at: datetime
    updated_at: datetime
    last_heartbeat_at: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "created_at", _require_aware_utc(self.created_at, "created_at")
        )
        object.__setattr__(
            self, "updated_at", _require_aware_utc(self.updated_at, "updated_at")
        )

        if self.last_heartbeat_at is not None:
            object.__setattr__(
                self,
                "last_heartbeat_at",
                _require_aware_utc(self.last_heartbeat_at, "last_heartbeat_at"),
            )

        if self.updated_at < self.created_at:
            raise ValueError("updated_at must be greater than or equal to created_at")

        if self.reason_code is not None:
            normalized = self.reason_code.strip().upper()
            if not normalized:
                raise ValueError("reason_code must not be blank when provided")
            object.__setattr__(self, "reason_code", normalized)

    def to_dict(self) -> dict[str, Any]:
        return {
            "identity": self.identity.to_dict(),
            "phase": self.phase.value,
            "reason_code": self.reason_code,
            "created_at": _serialize_datetime(self.created_at),
            "updated_at": _serialize_datetime(self.updated_at),
            "last_heartbeat_at": _serialize_datetime(self.last_heartbeat_at),
        }


WorkerRuntimeState = RuntimeStateSnapshot

__all__ = [
    "LaunchContext",
    "OrderIntent",
    "RuntimeStateSnapshot",
    "WorkerIdentity",
    "WorkerRuntimeState",
]

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from sqlalchemy import insert, select
from sqlalchemy.engine import Connection
from runtime.persistence import schema

_WORKER_IDENTITY_DETAILS_KEY = "__worker_identity"


def _require_non_empty(value: str, *, field_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be blank")
    return normalized


def _normalize_optional(value: str | None, *, field_name: str) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be blank when provided")
    return normalized


def _require_aware_utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _to_wire_ts(value: datetime) -> str:
    return (
        _require_aware_utc(value, field_name="datetime")
        .isoformat()
        .replace("+00:00", "Z")
    )


def _from_wire_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _serialize_details(value: Mapping[str, Any] | None, *, worker_identity: str) -> str:
    payload = dict(value or {})
    payload[_WORKER_IDENTITY_DETAILS_KEY] = worker_identity
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _deserialize_details(value: str | None) -> tuple[str, dict[str, Any]]:
    if not value:
        return "", {}
    loaded = json.loads(value)
    if not isinstance(loaded, dict):
        return "", {"value": loaded}
    payload = dict(loaded)
    worker_identity = str(payload.pop(_WORKER_IDENTITY_DETAILS_KEY, ""))
    return worker_identity, payload


@dataclass(frozen=True, slots=True)
class DiagnosticEntry:
    runtime_id: str
    worker_identity: str
    launch_attempt: int
    diagnostic_type: str
    observed_at: datetime
    reason_code: str | None = None
    details: Mapping[str, Any] | None = None
    correlation_id: str | None = None

    def __post_init__(self) -> None:
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
            "diagnostic_type",
            _require_non_empty(self.diagnostic_type, field_name="diagnostic_type"),
        )
        if self.launch_attempt < 1:
            raise ValueError("launch_attempt must be greater than or equal to 1")
        object.__setattr__(
            self,
            "reason_code",
            _normalize_optional(self.reason_code, field_name="reason_code"),
        )
        object.__setattr__(
            self,
            "correlation_id",
            _normalize_optional(self.correlation_id, field_name="correlation_id"),
        )
        object.__setattr__(
            self,
            "observed_at",
            _require_aware_utc(self.observed_at, field_name="observed_at"),
        )

    @property
    def identity_key(self) -> str:
        observed = _to_wire_ts(self.observed_at)
        return (
            f"{self.runtime_id}:{self.worker_identity}:"
            f"{self.launch_attempt}:{self.diagnostic_type}:{observed}"
        )


class SQLiteDiagnosticRecorder:
    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    def record(self, entry: DiagnosticEntry) -> None:
        self._connection.execute(
            insert(schema.worker_diagnostics).values(
                runtime_id=entry.runtime_id,
                launch_attempt=entry.launch_attempt,
                diagnostic_type=entry.diagnostic_type,
                stage=None,
                reason_code=entry.reason_code,
                occurred_at=_to_wire_ts(entry.observed_at),
                observed_at=_to_wire_ts(entry.observed_at),
                correlation_id=entry.correlation_id,
                causation_id=None,
                details=_serialize_details(
                    entry.details, worker_identity=entry.worker_identity
                ),
            )
        )

    def list_for_runtime(self, runtime_id: str) -> list[DiagnosticEntry]:
        normalized_runtime_id = _require_non_empty(runtime_id, field_name="runtime_id")
        rows = (
            self._connection.execute(
                select(schema.worker_diagnostics)
                .where(schema.worker_diagnostics.c.runtime_id == normalized_runtime_id)
                .order_by(
                    schema.worker_diagnostics.c.observed_at.asc(),
                    schema.worker_diagnostics.c.launch_attempt.asc(),
                    schema.worker_diagnostics.c.diagnostic_type.asc(),
                    schema.worker_diagnostics.c.id.asc(),
                )
            )
            .mappings()
            .all()
        )
        return [self._row_to_entry(row) for row in rows]

    def list_for_runtime_attempt(
        self, runtime_id: str, launch_attempt: int
    ) -> list[DiagnosticEntry]:
        normalized_runtime_id = _require_non_empty(runtime_id, field_name="runtime_id")
        if launch_attempt < 1:
            raise ValueError("launch_attempt must be greater than or equal to 1")
        rows = (
            self._connection.execute(
                select(schema.worker_diagnostics)
                .where(schema.worker_diagnostics.c.runtime_id == normalized_runtime_id)
                .where(schema.worker_diagnostics.c.launch_attempt == launch_attempt)
                .order_by(
                    schema.worker_diagnostics.c.observed_at.asc(),
                    schema.worker_diagnostics.c.diagnostic_type.asc(),
                    schema.worker_diagnostics.c.id.asc(),
                )
            )
            .mappings()
            .all()
        )
        return [self._row_to_entry(row) for row in rows]

    def _row_to_entry(self, row: Any) -> DiagnosticEntry:
        worker_identity, details = _deserialize_details(row["details"])
        return DiagnosticEntry(
            runtime_id=row["runtime_id"],
            worker_identity=worker_identity,
            launch_attempt=row["launch_attempt"],
            diagnostic_type=row["diagnostic_type"],
            observed_at=_from_wire_ts(row["observed_at"]),
            reason_code=row["reason_code"],
            details=details,
            correlation_id=row["correlation_id"],
        )

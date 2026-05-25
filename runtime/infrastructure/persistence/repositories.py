from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Protocol

from sqlalchemy import insert, select, update
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError
from runtime.infrastructure.persistence import schema


def _require_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(timezone.utc)


def _to_wire_ts(value: datetime) -> str:
    return _require_utc(value).isoformat().replace("+00:00", "Z")


def _from_wire_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _to_json(value: Mapping[str, Any] | None) -> str | None:
    if value is None:
        return None
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _from_json(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    loaded = json.loads(value)
    if not isinstance(loaded, dict):
        return {"value": loaded}
    return loaded


@dataclass(frozen=True, slots=True)
class WorkerInstanceRecord:
    runtime_id: str
    worker_identity: str
    tenant_id: str
    strategy_version_id: str
    launch_attempt: int
    state: str
    occurred_at: datetime
    observed_at: datetime
    trader_id: str | None = None
    account_id: str | None = None
    reason_code: str | None = None
    last_heartbeat_at: datetime | None = None
    correlation_id: str | None = None


@dataclass(frozen=True, slots=True)
class LaunchAttemptRecord:
    runtime_id: str
    launch_attempt: int
    state: str
    occurred_at: datetime
    observed_at: datetime
    reason_code: str | None = None
    correlation_id: str | None = None
    details: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class HeartbeatObservationRecord:
    runtime_id: str
    launch_attempt: int
    occurred_at: datetime
    observed_at: datetime
    correlation_id: str | None = None
    details: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class WorkerEventRecord:
    event_id: str
    runtime_id: str
    worker_identity: str
    launch_attempt: int
    event_family: str
    occurred_at: datetime
    observed_at: datetime
    state: str | None = None
    reason_code: str | None = None
    correlation_id: str | None = None
    payload: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class DiagnosticRecord:
    runtime_id: str
    launch_attempt: int
    diagnostic_type: str
    occurred_at: datetime
    observed_at: datetime
    stage: str | None = None
    reason_code: str | None = None
    correlation_id: str | None = None
    details: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class RuntimeStateJournalRecord:
    runtime_id: str
    launch_attempt: int
    kind: str
    observed_at: datetime
    phase: str | None = None
    previous_phase: str | None = None
    step_name: str | None = None
    level: str | None = None
    reason_code: str | None = None
    details: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class OrderIntentJournalRecord:
    runtime_id: str
    launch_attempt: int
    source: str
    requested_at: datetime
    payload: Mapping[str, Any]
    result: Mapping[str, Any] | None = None
    correlation_id: str | None = None
    #: Latest bar/tick/quote event time (UTC) when the intent was created; not wall clock.
    created_at: datetime | None = None
    job_id: str | None = None
    idempotency_key: str | None = None
    order_intent_id: str | None = None
    symbol: str | None = None


class WorkerInstanceRepository(Protocol):
    def upsert_instance(self, instance: WorkerInstanceRecord) -> None: ...

    def get_by_runtime_id(self, runtime_id: str) -> WorkerInstanceRecord | None: ...

    def update_state(
        self,
        runtime_id: str,
        *,
        state: str,
        reason_code: str | None,
        launch_attempt: int,
        occurred_at: datetime,
        observed_at: datetime,
        correlation_id: str | None,
    ) -> None: ...

    def record_heartbeat(
        self, runtime_id: str, *, heartbeat_at: datetime, observed_at: datetime
    ) -> None: ...

    def list_by_strategy_version(
        self, strategy_version_id: str
    ) -> list[WorkerInstanceRecord]: ...


class LaunchAttemptRepository(Protocol):
    def append_attempt(self, attempt: LaunchAttemptRecord) -> None: ...

    def get_attempt(
        self, runtime_id: str, launch_attempt: int
    ) -> LaunchAttemptRecord | None: ...

    def list_attempts(self, runtime_id: str) -> list[LaunchAttemptRecord]: ...

    def mark_outcome(
        self,
        runtime_id: str,
        launch_attempt: int,
        *,
        state: str,
        reason_code: str | None,
        occurred_at: datetime,
        observed_at: datetime,
        details: Mapping[str, Any] | None,
    ) -> None: ...


class HeartbeatRepository(Protocol):
    def append_observation(self, observation: HeartbeatObservationRecord) -> None: ...

    def list_for_runtime(self, runtime_id: str) -> list[HeartbeatObservationRecord]: ...

    def latest_for_runtime(
        self, runtime_id: str
    ) -> HeartbeatObservationRecord | None: ...


class WorkerEventRepository(Protocol):
    def append_event(self, event: WorkerEventRecord) -> bool: ...

    def get_by_event_id(self, event_id: str) -> WorkerEventRecord | None: ...


class DiagnosticRepository(Protocol):
    def append_diagnostic(self, diagnostic: DiagnosticRecord) -> None: ...

    def list_for_runtime(self, runtime_id: str) -> list[DiagnosticRecord]: ...


class SQLiteWorkerInstanceRepository:
    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    def upsert_instance(self, instance: WorkerInstanceRecord) -> None:
        update_stmt = (
            update(schema.worker_instances)
            .where(schema.worker_instances.c.runtime_id == instance.runtime_id)
            .values(
                worker_identity=instance.worker_identity,
                tenant_id=instance.tenant_id,
                trader_id=instance.trader_id,
                account_id=instance.account_id,
                strategy_version_id=instance.strategy_version_id,
                launch_attempt=instance.launch_attempt,
                state=instance.state,
                reason_code=instance.reason_code,
                occurred_at=_to_wire_ts(instance.occurred_at),
                observed_at=_to_wire_ts(instance.observed_at),
                correlation_id=instance.correlation_id,
            )
        )
        result = self._connection.execute(update_stmt)
        if result.rowcount == 0:
            self._connection.execute(
                insert(schema.worker_instances).values(
                    runtime_id=instance.runtime_id,
                    worker_identity=instance.worker_identity,
                    tenant_id=instance.tenant_id,
                    trader_id=instance.trader_id,
                    account_id=instance.account_id,
                    strategy_version_id=instance.strategy_version_id,
                    launch_attempt=instance.launch_attempt,
                    state=instance.state,
                    reason_code=instance.reason_code,
                    occurred_at=_to_wire_ts(instance.occurred_at),
                    observed_at=_to_wire_ts(instance.observed_at),
                    last_heartbeat_at=(
                        _to_wire_ts(instance.last_heartbeat_at)
                        if instance.last_heartbeat_at is not None
                        else None
                    ),
                    correlation_id=instance.correlation_id,
                )
            )

    def get_by_runtime_id(self, runtime_id: str) -> WorkerInstanceRecord | None:
        row = (
            self._connection.execute(
                select(schema.worker_instances).where(
                    schema.worker_instances.c.runtime_id == runtime_id
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            return None
        return WorkerInstanceRecord(
            runtime_id=row["runtime_id"],
            worker_identity=row["worker_identity"],
            tenant_id=row["tenant_id"],
            trader_id=row["trader_id"],
            account_id=row["account_id"],
            strategy_version_id=row["strategy_version_id"],
            launch_attempt=row["launch_attempt"],
            state=row["state"],
            reason_code=row["reason_code"],
            occurred_at=_from_wire_ts(row["occurred_at"]),
            observed_at=_from_wire_ts(row["observed_at"]),
            last_heartbeat_at=(
                _from_wire_ts(row["last_heartbeat_at"])
                if row["last_heartbeat_at"] is not None
                else None
            ),
            correlation_id=row["correlation_id"],
        )

    def update_state(
        self,
        runtime_id: str,
        *,
        state: str,
        reason_code: str | None,
        launch_attempt: int,
        occurred_at: datetime,
        observed_at: datetime,
        correlation_id: str | None,
    ) -> None:
        self._connection.execute(
            update(schema.worker_instances)
            .where(schema.worker_instances.c.runtime_id == runtime_id)
            .values(
                launch_attempt=launch_attempt,
                state=state,
                reason_code=reason_code,
                occurred_at=_to_wire_ts(occurred_at),
                observed_at=_to_wire_ts(observed_at),
                correlation_id=correlation_id,
            )
        )

    def record_heartbeat(
        self, runtime_id: str, *, heartbeat_at: datetime, observed_at: datetime
    ) -> None:
        current = self.get_by_runtime_id(runtime_id)
        if current is None:
            return
        candidate = _require_utc(heartbeat_at)
        current_ts = current.last_heartbeat_at
        accepted = current_ts is None or candidate >= current_ts
        if not accepted:
            return
        self._connection.execute(
            update(schema.worker_instances)
            .where(schema.worker_instances.c.runtime_id == runtime_id)
            .values(
                last_heartbeat_at=_to_wire_ts(candidate),
                observed_at=_to_wire_ts(observed_at),
            )
        )

    def list_by_strategy_version(
        self, strategy_version_id: str
    ) -> list[WorkerInstanceRecord]:
        rows = (
            self._connection.execute(
                select(schema.worker_instances).where(
                    schema.worker_instances.c.strategy_version_id == strategy_version_id
                )
            )
            .mappings()
            .all()
        )
        return [
            WorkerInstanceRecord(
                runtime_id=row["runtime_id"],
                worker_identity=row["worker_identity"],
                tenant_id=row["tenant_id"],
                trader_id=row["trader_id"],
                account_id=row["account_id"],
                strategy_version_id=row["strategy_version_id"],
                launch_attempt=row["launch_attempt"],
                state=row["state"],
                reason_code=row["reason_code"],
                occurred_at=_from_wire_ts(row["occurred_at"]),
                observed_at=_from_wire_ts(row["observed_at"]),
                last_heartbeat_at=(
                    _from_wire_ts(row["last_heartbeat_at"])
                    if row["last_heartbeat_at"] is not None
                    else None
                ),
                correlation_id=row["correlation_id"],
            )
            for row in rows
        ]


class SQLiteLaunchAttemptRepository:
    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    def append_attempt(self, attempt: LaunchAttemptRecord) -> None:
        self._connection.execute(
            insert(schema.worker_launch_attempts).values(
                runtime_id=attempt.runtime_id,
                launch_attempt=attempt.launch_attempt,
                state=attempt.state,
                reason_code=attempt.reason_code,
                occurred_at=_to_wire_ts(attempt.occurred_at),
                observed_at=_to_wire_ts(attempt.observed_at),
                correlation_id=attempt.correlation_id,
                details=_to_json(attempt.details),
            )
        )

    def get_attempt(
        self, runtime_id: str, launch_attempt: int
    ) -> LaunchAttemptRecord | None:
        row = (
            self._connection.execute(
                select(schema.worker_launch_attempts)
                .where(schema.worker_launch_attempts.c.runtime_id == runtime_id)
                .where(schema.worker_launch_attempts.c.launch_attempt == launch_attempt)
            )
            .mappings()
            .first()
        )
        if row is None:
            return None
        return LaunchAttemptRecord(
            runtime_id=row["runtime_id"],
            launch_attempt=row["launch_attempt"],
            state=row["state"],
            reason_code=row["reason_code"],
            occurred_at=_from_wire_ts(row["occurred_at"]),
            observed_at=_from_wire_ts(row["observed_at"]),
            correlation_id=row["correlation_id"],
            details=_from_json(row["details"]),
        )

    def list_attempts(self, runtime_id: str) -> list[LaunchAttemptRecord]:
        rows = (
            self._connection.execute(
                select(schema.worker_launch_attempts)
                .where(schema.worker_launch_attempts.c.runtime_id == runtime_id)
                .order_by(schema.worker_launch_attempts.c.launch_attempt.asc())
            )
            .mappings()
            .all()
        )
        return [
            LaunchAttemptRecord(
                runtime_id=row["runtime_id"],
                launch_attempt=row["launch_attempt"],
                state=row["state"],
                reason_code=row["reason_code"],
                occurred_at=_from_wire_ts(row["occurred_at"]),
                observed_at=_from_wire_ts(row["observed_at"]),
                correlation_id=row["correlation_id"],
                details=_from_json(row["details"]),
            )
            for row in rows
        ]

    def mark_outcome(
        self,
        runtime_id: str,
        launch_attempt: int,
        *,
        state: str,
        reason_code: str | None,
        occurred_at: datetime,
        observed_at: datetime,
        details: Mapping[str, Any] | None,
    ) -> None:
        self._connection.execute(
            update(schema.worker_launch_attempts)
            .where(schema.worker_launch_attempts.c.runtime_id == runtime_id)
            .where(schema.worker_launch_attempts.c.launch_attempt == launch_attempt)
            .values(
                state=state,
                reason_code=reason_code,
                occurred_at=_to_wire_ts(occurred_at),
                observed_at=_to_wire_ts(observed_at),
                details=_to_json(details),
            )
        )


class SQLiteHeartbeatRepository:
    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    def append_observation(self, observation: HeartbeatObservationRecord) -> None:
        self._connection.execute(
            insert(schema.worker_heartbeat_observations).values(
                runtime_id=observation.runtime_id,
                launch_attempt=observation.launch_attempt,
                occurred_at=_to_wire_ts(observation.occurred_at),
                observed_at=_to_wire_ts(observation.observed_at),
                correlation_id=observation.correlation_id,
                details=_to_json(observation.details),
            )
        )

    def list_for_runtime(self, runtime_id: str) -> list[HeartbeatObservationRecord]:
        rows = (
            self._connection.execute(
                select(schema.worker_heartbeat_observations)
                .where(schema.worker_heartbeat_observations.c.runtime_id == runtime_id)
                .order_by(schema.worker_heartbeat_observations.c.observed_at.asc())
            )
            .mappings()
            .all()
        )
        return [
            HeartbeatObservationRecord(
                runtime_id=row["runtime_id"],
                launch_attempt=row["launch_attempt"],
                occurred_at=_from_wire_ts(row["occurred_at"]),
                observed_at=_from_wire_ts(row["observed_at"]),
                correlation_id=row["correlation_id"],
                details=_from_json(row["details"]),
            )
            for row in rows
        ]

    def latest_for_runtime(self, runtime_id: str) -> HeartbeatObservationRecord | None:
        row = (
            self._connection.execute(
                select(schema.worker_heartbeat_observations)
                .where(schema.worker_heartbeat_observations.c.runtime_id == runtime_id)
                .order_by(schema.worker_heartbeat_observations.c.observed_at.desc())
                .limit(1)
            )
            .mappings()
            .first()
        )
        if row is None:
            return None
        return HeartbeatObservationRecord(
            runtime_id=row["runtime_id"],
            launch_attempt=row["launch_attempt"],
            occurred_at=_from_wire_ts(row["occurred_at"]),
            observed_at=_from_wire_ts(row["observed_at"]),
            correlation_id=row["correlation_id"],
            details=_from_json(row["details"]),
        )


class SQLiteWorkerEventRepository:
    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    def append_event(self, event: WorkerEventRecord) -> bool:
        try:
            self._connection.execute(
                insert(schema.worker_events).values(
                    event_id=event.event_id,
                    runtime_id=event.runtime_id,
                    worker_identity=event.worker_identity,
                    launch_attempt=event.launch_attempt,
                    event_family=event.event_family,
                    state=event.state,
                    reason_code=event.reason_code,
                    occurred_at=_to_wire_ts(event.occurred_at),
                    observed_at=_to_wire_ts(event.observed_at),
                    correlation_id=event.correlation_id,
                    payload=_to_json(event.payload),
                )
            )
            return True
        except IntegrityError:
            existing = self.get_by_event_id(event.event_id)
            if existing is not None:
                return False
            raise

    def get_by_event_id(self, event_id: str) -> WorkerEventRecord | None:
        row = (
            self._connection.execute(
                select(schema.worker_events).where(
                    schema.worker_events.c.event_id == event_id
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            return None
        return WorkerEventRecord(
            event_id=row["event_id"],
            runtime_id=row["runtime_id"],
            worker_identity=row["worker_identity"],
            launch_attempt=row["launch_attempt"],
            event_family=row["event_family"],
            state=row["state"],
            reason_code=row["reason_code"],
            occurred_at=_from_wire_ts(row["occurred_at"]),
            observed_at=_from_wire_ts(row["observed_at"]),
            correlation_id=row["correlation_id"],
            payload=_from_json(row["payload"]),
        )


class SQLiteDiagnosticRepository:
    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    def append_diagnostic(self, diagnostic: DiagnosticRecord) -> None:
        self._connection.execute(
            insert(schema.worker_diagnostics).values(
                runtime_id=diagnostic.runtime_id,
                launch_attempt=diagnostic.launch_attempt,
                diagnostic_type=diagnostic.diagnostic_type,
                stage=diagnostic.stage,
                reason_code=diagnostic.reason_code,
                occurred_at=_to_wire_ts(diagnostic.occurred_at),
                observed_at=_to_wire_ts(diagnostic.observed_at),
                correlation_id=diagnostic.correlation_id,
                details=_to_json(diagnostic.details),
            )
        )

    def list_for_runtime(self, runtime_id: str) -> list[DiagnosticRecord]:
        rows = (
            self._connection.execute(
                select(schema.worker_diagnostics)
                .where(schema.worker_diagnostics.c.runtime_id == runtime_id)
                .order_by(schema.worker_diagnostics.c.observed_at.asc())
            )
            .mappings()
            .all()
        )
        return [
            DiagnosticRecord(
                runtime_id=row["runtime_id"],
                launch_attempt=row["launch_attempt"],
                diagnostic_type=row["diagnostic_type"],
                stage=row["stage"],
                reason_code=row["reason_code"],
                occurred_at=_from_wire_ts(row["occurred_at"]),
                observed_at=_from_wire_ts(row["observed_at"]),
                correlation_id=row["correlation_id"],
                details=_from_json(row["details"]),
            )
            for row in rows
        ]


class SQLiteRuntimeStateJournalRepository:
    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    def append(self, record: RuntimeStateJournalRecord) -> None:
        self._connection.execute(
            insert(schema.runtime_state_journal).values(
                runtime_id=record.runtime_id,
                launch_attempt=record.launch_attempt,
                kind=record.kind,
                phase=record.phase,
                previous_phase=record.previous_phase,
                step_name=record.step_name,
                level=record.level,
                reason_code=record.reason_code,
                observed_at=_to_wire_ts(record.observed_at),
                details_json=_to_json(record.details),
            )
        )


class SQLiteOrderIntentJournalRepository:
    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    def append(self, record: OrderIntentJournalRecord) -> None:
        created_wire = (
            _to_wire_ts(record.created_at) if record.created_at is not None else None
        )
        self._connection.execute(
            insert(schema.order_intent_journal).values(
                runtime_id=record.runtime_id,
                launch_attempt=record.launch_attempt,
                source=record.source,
                payload_json=_to_json(record.payload) or "{}",
                result_json=_to_json(record.result),
                requested_at=_to_wire_ts(record.requested_at),
                created_at=created_wire,
                correlation_id=record.correlation_id,
                job_id=record.job_id,
                idempotency_key=record.idempotency_key,
                order_intent_id=record.order_intent_id,
                symbol=record.symbol,
            )
        )

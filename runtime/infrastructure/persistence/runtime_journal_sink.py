from __future__ import annotations

import hashlib
import json
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Mapping
import uuid
from uuid import UUID

from sqlalchemy import insert
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from runtime.domain.enums import WorkerPhase
from runtime.domain.events.event_envelope import LifecycleEventEnvelope
from runtime.infrastructure.persistence import schema
from runtime.infrastructure.persistence.db import begin_connection, create_engine
from runtime.infrastructure.persistence.migrations import apply_migrations
from runtime.infrastructure.persistence.repositories import (
    HeartbeatObservationRecord,
    OrderIntentJournalRecord,
    RuntimeStateJournalRecord,
    SQLiteHeartbeatRepository,
    SQLiteOrderIntentJournalRepository,
    SQLiteRuntimeStateJournalRepository,
    SQLiteWorkerEventRepository,
    SQLiteWorkerInstanceRepository,
    WorkerEventRecord,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _requested_at_for_order_intent(payload: Mapping[str, Any]) -> datetime:
    raw = payload.get("requested_at") or payload.get("occurred_at")
    if isinstance(raw, datetime):
        if raw.tzinfo is None or raw.utcoffset() is None:
            return raw.replace(tzinfo=timezone.utc)
        return raw.astimezone(timezone.utc)
    return _utc_now()


def _created_at_for_order_intent(payload: Mapping[str, Any]) -> datetime | None:
    """Market/simulation event time from ``payload['created_at']`` (latest bar/tick/quote when submitted)."""
    raw = payload.get("created_at")
    if isinstance(raw, datetime):
        if raw.tzinfo is None or raw.utcoffset() is None:
            return raw.replace(tzinfo=timezone.utc)
        return raw.astimezone(timezone.utc)
    return _parse_dt(raw)


def _wire_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical_order_intent_journal_payload(d: dict[str, Any]) -> dict[str, Any]:
    """Drop unknown alias keys; use ``idempotency_key`` only (migrate legacy ``client_intent_id``)."""
    out = dict(d)
    legacy_occ = out.pop("occurred_at", None)
    if legacy_occ is not None and out.get("requested_at") in (None, ""):
        out["requested_at"] = legacy_occ
    out.pop("backtest_job_id", None)
    out.pop("strategy_context", None)
    out.pop("replay_session_id", None)
    out.pop("launch_attempt", None)
    legacy = out.pop("client_intent_id", None)
    if legacy and not str(out.get("idempotency_key") or "").strip():
        out["idempotency_key"] = str(legacy).strip()
    return out


def _json_safe(value: Any) -> Any:
    """Recursively coerce values so :func:`json.dumps` succeeds for journal payloads/results."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (int, float, str)):
        return value
    if isinstance(value, datetime):
        return _wire_iso(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="replace")
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    if isinstance(value, set):
        return [_json_safe(v) for v in value]
    return str(value)


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value.strip():
        try:
            dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
        return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
    return None


class RuntimeJournalSink:
    """Persists worker runtime state transitions and order intent submissions to SQLite and a newline-delimited JSON .txt file."""

    def __init__(
        self,
        *,
        db_path: Path,
        txt_path: Path,
        runtime_id: str,
        launch_attempt: int,
        launch_job_id: str | None = None,
    ) -> None:
        self._runtime_id = runtime_id
        self._launch_attempt = launch_attempt
        lj = (launch_job_id or "").strip()
        self._launch_job_id: str | None = lj if lj else None
        self._txt_path = txt_path
        self._txt_path.parent.mkdir(parents=True, exist_ok=True)
        self._engine: Engine = create_engine(db_path)
        self._lock = threading.Lock()
        with begin_connection(self._engine) as conn:
            apply_migrations(conn)

    def allocate_order_intent_id(self) -> str:
        """New UUID ``order_intent_id`` for the next order intent on this worker."""
        return str(uuid.uuid4())

    def _with_sqlite_lock_retry(self, op: Any) -> None:
        retries = 4
        delay_s = 0.05
        for attempt in range(retries + 1):
            try:
                op()
                return
            except OperationalError as exc:
                message = str(exc).lower()
                if (
                    "database is locked" not in message
                    and "database table is locked" not in message
                ):
                    raise
                if attempt >= retries:
                    raise
                time.sleep(delay_s)
                delay_s *= 2

    def record_phase_change(
        self,
        *,
        previous_phase: WorkerPhase | None,
        phase: WorkerPhase,
        level: str,
        reason_code: str | None,
    ) -> None:
        observed_at = _utc_now()
        rec = RuntimeStateJournalRecord(
            runtime_id=self._runtime_id,
            launch_attempt=self._launch_attempt,
            kind="phase",
            observed_at=observed_at,
            phase=phase.value,
            previous_phase=previous_phase.value if previous_phase is not None else None,
            level=level,
            reason_code=reason_code,
        )
        line: dict[str, Any] = {
            "kind": "phase",
            "runtime_id": self._runtime_id,
            "launch_attempt": self._launch_attempt,
            "phase": phase.value,
            "previous_phase": (
                previous_phase.value if previous_phase is not None else None
            ),
            "level": level,
            "reason_code": reason_code,
            "observed_at": _wire_iso(observed_at),
        }
        self._persist_state(rec, line)

    def record_startup_step(self, step: str) -> None:
        observed_at = _utc_now()
        details = {"step_name": step, "stage": "startup"}
        rec = RuntimeStateJournalRecord(
            runtime_id=self._runtime_id,
            launch_attempt=self._launch_attempt,
            kind="startup_step",
            observed_at=observed_at,
            step_name=step,
            level="INFO",
            reason_code="STARTUP_PROGRESS",
            details=details,
        )
        line = {
            "kind": "startup_step",
            "runtime_id": self._runtime_id,
            "launch_attempt": self._launch_attempt,
            "step_name": step,
            "level": "INFO",
            "reason_code": "STARTUP_PROGRESS",
            "details": details,
            "observed_at": _wire_iso(observed_at),
        }
        self._persist_state(rec, line)

    def record_shutdown_step(self, step: str) -> None:
        observed_at = _utc_now()
        details = {"step_name": step, "stage": "shutdown"}
        rec = RuntimeStateJournalRecord(
            runtime_id=self._runtime_id,
            launch_attempt=self._launch_attempt,
            kind="shutdown_step",
            observed_at=observed_at,
            step_name=step,
            level="INFO",
            reason_code="SHUTDOWN_PROGRESS",
            details=details,
        )
        line = {
            "kind": "shutdown_step",
            "runtime_id": self._runtime_id,
            "launch_attempt": self._launch_attempt,
            "step_name": step,
            "level": "INFO",
            "reason_code": "SHUTDOWN_PROGRESS",
            "details": details,
            "observed_at": _wire_iso(observed_at),
        }
        self._persist_state(rec, line)

    def record_first_data(self) -> None:
        observed_at = _utc_now()
        details = {"marker": "first_data_observed"}
        rec = RuntimeStateJournalRecord(
            runtime_id=self._runtime_id,
            launch_attempt=self._launch_attempt,
            kind="first_data",
            observed_at=observed_at,
            level="INFO",
            reason_code="FIRST_DATA_OBSERVED",
            details=details,
        )
        line = {
            "kind": "first_data",
            "runtime_id": self._runtime_id,
            "launch_attempt": self._launch_attempt,
            "level": "INFO",
            "reason_code": "FIRST_DATA_OBSERVED",
            "details": details,
            "observed_at": _wire_iso(observed_at),
        }
        self._persist_state(rec, line)

    def record_heartbeat(
        self,
        *,
        local_state: str,
        observed_at: datetime,
        correlation_id: str | None = None,
    ) -> None:
        details: dict[str, Any] = {"local_state": local_state}
        rec = HeartbeatObservationRecord(
            runtime_id=self._runtime_id,
            launch_attempt=self._launch_attempt,
            occurred_at=observed_at,
            observed_at=observed_at,
            correlation_id=correlation_id,
            details=details,
        )
        line = {
            "kind": "heartbeat",
            "runtime_id": self._runtime_id,
            "launch_attempt": self._launch_attempt,
            "local_state": local_state,
            "observed_at": _wire_iso(observed_at),
        }
        with self._lock:

            def _write() -> None:
                with begin_connection(self._engine) as conn:
                    self._ensure_worker_instance_stub(
                        conn=conn, observed_at=observed_at
                    )
                    SQLiteHeartbeatRepository(conn).append_observation(rec)
                    instances = SQLiteWorkerInstanceRepository(conn)
                    instances.record_heartbeat(
                        self._runtime_id,
                        heartbeat_at=observed_at,
                        observed_at=observed_at,
                    )
                    current = instances.get_by_runtime_id(self._runtime_id)
                    instances.update_state(
                        self._runtime_id,
                        state=local_state,
                        reason_code=(
                            current.reason_code if current is not None else None
                        ),
                        launch_attempt=self._launch_attempt,
                        occurred_at=observed_at,
                        observed_at=observed_at,
                        correlation_id=(
                            current.correlation_id if current is not None else None
                        ),
                    )
                with self._txt_path.open("a", encoding="utf-8") as fh:
                    fh.write(
                        json.dumps(line, separators=(",", ":"), ensure_ascii=True)
                        + "\n"
                    )

            self._with_sqlite_lock_retry(_write)

    def record_lifecycle_event(self, event: LifecycleEventEnvelope) -> None:
        payload = _json_safe(dict(event.payload))
        payload_observed = (
            _parse_dt(payload.get("observed_at"))
            if isinstance(payload, Mapping)
            else None
        )
        observed_at = payload_observed or event.occurred_at
        local_state = (
            payload.get("local_state")
            if isinstance(payload.get("local_state"), str)
            else None
        )
        reason_code = (
            payload.get("reason_code")
            if isinstance(payload.get("reason_code"), str)
            else None
        )
        rec = WorkerEventRecord(
            event_id=event.event_id,
            runtime_id=self._runtime_id,
            worker_identity=event.worker_identity,
            launch_attempt=self._launch_attempt,
            event_family=event.event_name,
            occurred_at=event.occurred_at,
            observed_at=observed_at,
            state=local_state,
            reason_code=reason_code,
            correlation_id=event.correlation_id,
            payload=payload if isinstance(payload, Mapping) else None,
        )
        line = {
            "kind": "worker_event",
            "runtime_id": self._runtime_id,
            "launch_attempt": self._launch_attempt,
            "event_id": event.event_id,
            "event_name": event.event_name,
            "occurred_at": _wire_iso(event.occurred_at),
        }
        with self._lock:

            def _write() -> None:
                with begin_connection(self._engine) as conn:
                    self._ensure_worker_instance_stub(
                        conn=conn, observed_at=observed_at
                    )
                    SQLiteWorkerEventRepository(conn).append_event(rec)
                    instances = SQLiteWorkerInstanceRepository(conn)
                    current = instances.get_by_runtime_id(self._runtime_id)
                    if local_state is not None:
                        instances.update_state(
                            self._runtime_id,
                            state=local_state,
                            reason_code=(
                                reason_code
                                if reason_code is not None
                                else (
                                    current.reason_code if current is not None else None
                                )
                            ),
                            launch_attempt=self._launch_attempt,
                            occurred_at=event.occurred_at,
                            observed_at=observed_at,
                            correlation_id=event.correlation_id,
                        )
                    if event.event_name == "runtime.heartbeat":
                        SQLiteHeartbeatRepository(conn).append_observation(
                            HeartbeatObservationRecord(
                                runtime_id=self._runtime_id,
                                launch_attempt=self._launch_attempt,
                                occurred_at=event.occurred_at,
                                observed_at=observed_at,
                                correlation_id=event.correlation_id,
                                details=(
                                    {"local_state": local_state}
                                    if local_state is not None
                                    else None
                                ),
                            )
                        )
                        instances.record_heartbeat(
                            self._runtime_id,
                            heartbeat_at=observed_at,
                            observed_at=observed_at,
                        )
                with self._txt_path.open("a", encoding="utf-8") as fh:
                    fh.write(
                        json.dumps(line, separators=(",", ":"), ensure_ascii=True)
                        + "\n"
                    )

            self._with_sqlite_lock_retry(_write)

    def record_launch_failed_event(
        self,
        *,
        occurred_at: datetime,
        reason_code: str,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        payload: dict[str, Any] = {
            "occurred_at": _wire_iso(occurred_at),
            "reason_code": reason_code,
            "details": _json_safe(dict(details or {})),
            "runtime_id": self._runtime_id,
            "launch_attempt": self._launch_attempt,
            "worker_identity": f"{self._runtime_id}:{self._launch_attempt}",
        }
        digest = hashlib.sha256(
            f"{self._runtime_id}:{self._launch_attempt}:{reason_code}:{payload['occurred_at']}".encode(
                "utf-8"
            )
        ).hexdigest()[:24]
        event_id = (
            f"{self._runtime_id}:{self._launch_attempt}:runtime.launch_failed:{digest}"
        )
        rec = WorkerEventRecord(
            event_id=event_id,
            runtime_id=self._runtime_id,
            worker_identity=f"{self._runtime_id}:{self._launch_attempt}",
            launch_attempt=self._launch_attempt,
            event_family="runtime.launch_failed",
            occurred_at=occurred_at,
            observed_at=occurred_at,
            state="FAILED",
            reason_code=reason_code,
            correlation_id=f"corr:{self._runtime_id}:{self._launch_attempt}",
            payload=payload,
        )
        line = {
            "kind": "worker_event",
            "runtime_id": self._runtime_id,
            "launch_attempt": self._launch_attempt,
            "event_id": event_id,
            "event_name": "runtime.launch_failed",
            "occurred_at": _wire_iso(occurred_at),
        }
        with self._lock:

            def _write() -> None:
                with begin_connection(self._engine) as conn:
                    self._ensure_worker_instance_stub(
                        conn=conn, observed_at=occurred_at
                    )
                    SQLiteWorkerEventRepository(conn).append_event(rec)
                with self._txt_path.open("a", encoding="utf-8") as fh:
                    fh.write(
                        json.dumps(line, separators=(",", ":"), ensure_ascii=True)
                        + "\n"
                    )

            self._with_sqlite_lock_retry(_write)

    def record_manager_stop_request(
        self,
        *,
        last_data_event_at: datetime | None,
        last_clock_at: datetime | None,
        last_replay_cursor: str,
        stop_requested_at: datetime,
    ) -> None:
        observed_at = stop_requested_at
        details: dict[str, Any] = {
            "rpc": "StopWorker",
            "last_replay_cursor": last_replay_cursor,
            "stop_requested_at": _wire_iso(stop_requested_at),
            "last_data_event_at": (
                _wire_iso(last_data_event_at)
                if last_data_event_at is not None
                else None
            ),
            "last_clock_at": (
                _wire_iso(last_clock_at) if last_clock_at is not None else None
            ),
        }
        rec = RuntimeStateJournalRecord(
            runtime_id=self._runtime_id,
            launch_attempt=self._launch_attempt,
            kind="manager_stop_request",
            observed_at=observed_at,
            level="INFO",
            reason_code="MANAGER_STOP_REQUESTED",
            details=details,
        )
        line = {
            "kind": "manager_stop_request",
            "runtime_id": self._runtime_id,
            "launch_attempt": self._launch_attempt,
            "level": "INFO",
            "reason_code": "MANAGER_STOP_REQUESTED",
            "observed_at": _wire_iso(observed_at),
            **details,
        }
        self._persist_state(rec, line)

    def record_order_intent(
        self,
        source: str,
        payload: Mapping[str, Any],
        result: Mapping[str, Any],
    ) -> None:
        requested_at = _requested_at_for_order_intent(payload)
        created_at = _created_at_for_order_intent(payload)
        corr = (
            payload.get("correlation_id")
            if isinstance(payload.get("correlation_id"), str)
            else None
        )
        # gRPC payloads may include datetime (e.g. requested_at); SQLite/JSON need wire-safe dicts.
        payload_safe = _json_safe(dict(payload))
        if isinstance(payload_safe, dict):
            payload_safe = _canonical_order_intent_journal_payload(payload_safe)
        # ``job_id`` column and persisted payload align with launch bundle ``job_id``
        # (``setting.json`` → :class:`~runtime.bootstrap.launch_spec.LaunchSpec`) when set;
        # otherwise fall back to the intent payload (e.g. unit tests without a launch binding).
        job_col: str | None = None
        if isinstance(payload_safe, dict):
            if self._launch_job_id:
                job_col = self._launch_job_id
                payload_safe = dict(payload_safe)
                payload_safe["job_id"] = self._launch_job_id
            else:
                j = str(payload_safe.get("job_id") or "").strip()
                job_col = j if j else None
        else:
            job_col = self._launch_job_id
        if (
            isinstance(payload_safe, dict)
            and not str(payload_safe.get("symbol") or "").strip()
        ):
            nested = payload_safe.get("parameters")
            if isinstance(nested, dict):
                ps = nested.get("symbol")
                if isinstance(ps, str) and ps.strip():
                    payload_safe = dict(payload_safe)
                    payload_safe["symbol"] = ps.strip()
        idem_col: str | None = None
        oid_col: str | None = None
        sym_col: str | None = None
        if isinstance(payload_safe, dict):
            ik = str(payload_safe.get("idempotency_key") or "").strip()
            idem_col = ik if ik else None
            raw_oid = payload_safe.get("order_intent_id")
            if raw_oid is not None:
                oid_s = str(raw_oid).strip()
                oid_col = oid_s if oid_s else None
            sym = str(payload_safe.get("symbol") or "").strip()
            sym_col = sym if sym else None
        result_safe = _json_safe(dict(result))
        rec = OrderIntentJournalRecord(
            runtime_id=self._runtime_id,
            launch_attempt=self._launch_attempt,
            source=source,
            requested_at=requested_at,
            payload=payload_safe,
            result=result_safe,
            correlation_id=corr,
            created_at=created_at,
            job_id=job_col,
            idempotency_key=idem_col,
            order_intent_id=oid_col,
            symbol=sym_col,
        )
        line = {
            "kind": "order_intent",
            "runtime_id": self._runtime_id,
            "launch_attempt": self._launch_attempt,
            "source": source,
            "job_id": job_col,
            "idempotency_key": idem_col,
            "order_intent_id": oid_col,
            "symbol": sym_col,
            "payload": payload_safe,
            "result": result_safe,
            "requested_at": _wire_iso(requested_at),
            "created_at": _wire_iso(created_at) if created_at is not None else None,
            "correlation_id": corr,
        }
        with self._lock:

            def _write() -> None:
                with begin_connection(self._engine) as conn:
                    SQLiteOrderIntentJournalRepository(conn).append(rec)
                wire = json.dumps(line, separators=(",", ":"), ensure_ascii=True)
                with self._txt_path.open("a", encoding="utf-8") as fh:
                    fh.write(wire + "\n")
                print(f"[order_intent_journal] {wire}", flush=True)

            self._with_sqlite_lock_retry(_write)

    def _persist_state(
        self, rec: RuntimeStateJournalRecord, line: dict[str, Any]
    ) -> None:
        with self._lock:

            def _write() -> None:
                with begin_connection(self._engine) as conn:
                    if rec.kind == "phase" and rec.phase is not None:
                        self._ensure_worker_instance_stub(
                            conn=conn, observed_at=rec.observed_at
                        )
                        instances = SQLiteWorkerInstanceRepository(conn)
                        current = instances.get_by_runtime_id(self._runtime_id)
                        instances.update_state(
                            self._runtime_id,
                            state=rec.phase,
                            reason_code=(
                                rec.reason_code
                                if rec.reason_code is not None
                                else (
                                    current.reason_code if current is not None else None
                                )
                            ),
                            launch_attempt=self._launch_attempt,
                            occurred_at=rec.observed_at,
                            observed_at=rec.observed_at,
                            correlation_id=(
                                current.correlation_id if current is not None else None
                            ),
                        )
                    SQLiteRuntimeStateJournalRepository(conn).append(rec)
                with self._txt_path.open("a", encoding="utf-8") as fh:
                    fh.write(
                        json.dumps(line, separators=(",", ":"), ensure_ascii=True)
                        + "\n"
                    )

            self._with_sqlite_lock_retry(_write)

    def _ensure_worker_instance_stub(self, *, conn: Any, observed_at: datetime) -> None:
        """Ensure FK target exists for worker_heartbeat_observations without updating existing rows."""
        conn.execute(
            insert(schema.worker_instances)
            .prefix_with("OR IGNORE")
            .values(
                runtime_id=self._runtime_id,
                worker_identity=f"{self._runtime_id}:{self._launch_attempt}",
                tenant_id="",
                trader_id=None,
                account_id=None,
                strategy_version_id="",
                launch_attempt=self._launch_attempt,
                state="INITIALIZING",
                reason_code=None,
                occurred_at=_wire_iso(observed_at),
                observed_at=_wire_iso(observed_at),
                last_heartbeat_at=None,
                correlation_id=None,
            )
        )

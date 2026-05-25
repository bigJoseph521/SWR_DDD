from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import text
from runtime.domain.enums import WorkerPhase
from runtime.events.event_envelope import LifecycleEventEnvelope
from runtime.persistence.db import begin_connection, create_engine
from runtime.persistence.runtime_journal_sink import RuntimeJournalSink


def test_runtime_journal_order_intent_symbol_column_from_parameters_nested(
    tmp_path: Path,
) -> None:
    db = tmp_path / "j_sym.db"
    txt = tmp_path / "j_sym.txt"
    sink = RuntimeJournalSink(
        db_path=db,
        txt_path=txt,
        runtime_id="rt-sym",
        launch_attempt=1,
    )
    t_occ = datetime(2025, 6, 2, 12, 0, 0, tzinfo=timezone.utc)
    sink.record_order_intent(
        "backtest",
        {
            "parameters": {"symbol": "SPY"},
            "requested_at": t_occ,
        },
        {"accepted": True},
    )
    engine = create_engine(db)
    with begin_connection(engine) as conn:
        row = conn.execute(
            text("SELECT symbol, payload_json FROM order_intent_journal LIMIT 1")
        ).one()
    assert row[0] == "SPY"
    stored = json.loads(row[1])
    assert stored["symbol"] == "SPY"


def test_runtime_journal_order_intent_payload_omits_replay_session_id(
    tmp_path: Path,
) -> None:
    db = tmp_path / "j_rs.db"
    txt = tmp_path / "j_rs.txt"
    sink = RuntimeJournalSink(
        db_path=db,
        txt_path=txt,
        runtime_id="rt-rs",
        launch_attempt=1,
    )
    t_occ = datetime(2025, 6, 1, 11, 0, 0, tzinfo=timezone.utc)
    sink.record_order_intent(
        "backtest",
        {
            "replay_session_id": "sess-should-not-persist",
            "launch_attempt": 99,
            "instrument_id": "QQQ",
            "requested_at": t_occ,
        },
        {"accepted": True},
    )
    engine = create_engine(db)
    with begin_connection(engine) as conn:
        row = conn.execute(
            text("SELECT payload_json FROM order_intent_journal LIMIT 1")
        ).one()
    stored = json.loads(row[0])
    assert "replay_session_id" not in stored
    assert "launch_attempt" not in stored
    line = json.loads(txt.read_text(encoding="utf-8").strip().splitlines()[-1])
    assert "replay_session_id" not in line["payload"]
    assert "launch_attempt" not in line["payload"]


def test_runtime_journal_order_intent_payload_omits_strategy_context(
    tmp_path: Path,
) -> None:
    db = tmp_path / "j_sc.db"
    txt = tmp_path / "j_sc.txt"
    sink = RuntimeJournalSink(
        db_path=db,
        txt_path=txt,
        runtime_id="rt-sc",
        launch_attempt=1,
    )
    t_req = datetime(2025, 7, 1, 10, 0, 0, tzinfo=timezone.utc)
    sink.record_order_intent(
        "oms",
        {
            "strategy_context": {"alpha": 1},
            "instrument_id": "X",
            "requested_at": t_req,
        },
        {"accepted": True},
    )
    engine = create_engine(db)
    with begin_connection(engine) as conn:
        row = conn.execute(
            text("SELECT payload_json FROM order_intent_journal LIMIT 1")
        ).one()
    stored = json.loads(row[0])
    assert "strategy_context" not in stored


def test_runtime_journal_sink_allocate_order_intent_id_uuid(
    tmp_path: Path,
) -> None:
    from uuid import UUID

    db = tmp_path / "j_oid.db"
    txt = tmp_path / "j_oid.txt"
    sink = RuntimeJournalSink(
        db_path=db,
        txt_path=txt,
        runtime_id="rt-oid",
        launch_attempt=1,
    )
    oid1 = sink.allocate_order_intent_id()
    oid2 = sink.allocate_order_intent_id()
    UUID(oid1)
    UUID(oid2)
    assert oid1 != oid2
    t_occ = datetime(2025, 5, 1, 8, 0, 0, tzinfo=timezone.utc)
    sink.record_order_intent(
        "oms",
        {"order_intent_id": oid1, "requested_at": t_occ},
        {"accepted": True},
    )
    engine = create_engine(db)
    with begin_connection(engine) as conn:
        row = conn.execute(
            text(
                "SELECT order_intent_id, payload_json FROM order_intent_journal LIMIT 1"
            )
        ).one()
    assert row[0] == oid1
    stored = json.loads(row[1])
    assert stored["order_intent_id"] == oid1


def test_runtime_journal_order_intent_idempotency_key_column_and_legacy_payload_alias(
    tmp_path: Path,
) -> None:
    db = tmp_path / "j_idem.db"
    txt = tmp_path / "j_idem.txt"
    sink = RuntimeJournalSink(
        db_path=db,
        txt_path=txt,
        runtime_id="rt-idem",
        launch_attempt=1,
    )
    t_occ = datetime(2025, 4, 1, 9, 0, 0, tzinfo=timezone.utc)
    sink.record_order_intent(
        "oms",
        {"client_intent_id": "legacy-idem-1", "requested_at": t_occ},
        {"accepted": True},
    )
    engine = create_engine(db)
    with begin_connection(engine) as conn:
        row = conn.execute(
            text(
                "SELECT idempotency_key, payload_json FROM order_intent_journal LIMIT 1"
            )
        ).one()
    assert row[0] == "legacy-idem-1"
    stored = json.loads(row[1])
    assert stored["idempotency_key"] == "legacy-idem-1"
    assert "client_intent_id" not in stored

    line = json.loads(txt.read_text(encoding="utf-8").strip().splitlines()[-1])
    assert line["idempotency_key"] == "legacy-idem-1"
    assert line["payload"]["idempotency_key"] == "legacy-idem-1"


def test_runtime_journal_sink_order_intent_payload_strips_unknown_job_alias(
    tmp_path: Path,
) -> None:
    db = tmp_path / "j_job.db"
    txt = tmp_path / "j_job.txt"
    sink = RuntimeJournalSink(
        db_path=db,
        txt_path=txt,
        runtime_id="rt-job",
        launch_attempt=1,
        launch_job_id="j1",
    )
    sink.record_order_intent(
        "oms",
        {
            "job_id": "from-payload-should-not-win",
            "backtest_job_id": "ignored-alias",
            "requested_at": datetime(2025, 3, 2, 10, 0, 0, tzinfo=timezone.utc),
        },
        {"accepted": True},
    )
    line = json.loads(txt.read_text(encoding="utf-8").strip().splitlines()[-1])
    assert line["job_id"] == "j1"
    assert line["payload"]["job_id"] == "j1"
    assert "backtest_job_id" not in line["payload"]

    engine = create_engine(db)
    with begin_connection(engine) as conn:
        row = conn.execute(
            text("SELECT job_id, payload_json FROM order_intent_journal LIMIT 1")
        ).one()
    assert row[0] == "j1"
    stored = json.loads(row[1])
    assert stored["job_id"] == "j1"
    assert "backtest_job_id" not in stored


def test_runtime_journal_sink_writes_sqlite_and_txt(tmp_path: Path) -> None:
    db = tmp_path / "j.db"
    txt = tmp_path / "j.txt"
    sink = RuntimeJournalSink(
        db_path=db,
        txt_path=txt,
        runtime_id="rt-a",
        launch_attempt=3,
        launch_job_id="job-bt-99",
    )
    sink.record_phase_change(
        previous_phase=None,
        phase=WorkerPhase.INITIALIZING,
        level="INFO",
        reason_code=None,
    )
    t_sim = datetime(2025, 2, 28, 15, 30, 0, tzinfo=timezone.utc)
    sink.record_order_intent(
        "backtest",
        {
            "correlation_id": "c1",
            "x": 1,
            "requested_at": datetime(2025, 3, 1, 12, 0, 0, tzinfo=timezone.utc),
            "created_at": t_sim,
        },
        {"accepted": True},
    )

    lines = txt.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    a = json.loads(lines[0])
    b = json.loads(lines[1])
    assert a["kind"] == "phase"
    assert a["phase"] == "INITIALIZING"
    assert b["kind"] == "order_intent"
    assert b["source"] == "backtest"
    assert b["correlation_id"] == "c1"
    assert b["job_id"] == "job-bt-99"
    assert b["payload"]["job_id"] == "job-bt-99"
    assert b["payload"]["requested_at"] == "2025-03-01T12:00:00Z"
    assert b["created_at"] == "2025-02-28T15:30:00Z"

    engine = create_engine(db)
    with begin_connection(engine) as conn:
        intent_row = conn.execute(
            text(
                "SELECT job_id, requested_at, created_at FROM order_intent_journal LIMIT 1"
            )
        ).one()
        assert intent_row[0] == "job-bt-99"
        assert intent_row[1] == "2025-03-01T12:00:00Z"
        assert intent_row[2] == "2025-02-28T15:30:00Z"
        n_state = conn.execute(
            text("SELECT COUNT(*) FROM runtime_state_journal")
        ).scalar_one()
        n_intent = conn.execute(
            text("SELECT COUNT(*) FROM order_intent_journal")
        ).scalar_one()
        state_row = conn.execute(
            text(
                "SELECT kind, level, reason_code, details_json "
                "FROM runtime_state_journal WHERE kind = 'phase' ORDER BY id ASC LIMIT 1"
            )
        ).one()
        startup_row = conn.execute(
            text(
                "SELECT kind, level, reason_code, details_json, step_name "
                "FROM runtime_state_journal WHERE kind = 'startup_step' ORDER BY id ASC LIMIT 1"
            )
        ).one_or_none()
        instance = conn.execute(
            text(
                "SELECT runtime_id, launch_attempt, state FROM worker_instances WHERE runtime_id = 'rt-a'"
            )
        ).one()
    assert n_state == 1
    assert n_intent == 1
    assert state_row[0] == "phase"
    assert state_row[1] == "INFO"
    assert state_row[2] is None
    assert state_row[3] is None
    assert startup_row is None
    assert instance[0] == "rt-a"
    assert instance[1] == 3
    assert instance[2] == "INITIALIZING"


def test_runtime_journal_sink_phase_change_updates_worker_instance_state(
    tmp_path: Path,
) -> None:
    db = tmp_path / "j_phase.db"
    txt = tmp_path / "j_phase.txt"
    sink = RuntimeJournalSink(
        db_path=db,
        txt_path=txt,
        runtime_id="rt-phase",
        launch_attempt=5,
    )
    sink.record_phase_change(
        previous_phase=WorkerPhase.INITIALIZING,
        phase=WorkerPhase.RUNNING,
        level="INFO",
        reason_code=None,
    )
    sink.record_phase_change(
        previous_phase=WorkerPhase.RUNNING,
        phase=WorkerPhase.STOPPED,
        level="INFO",
        reason_code="STOP_REQUESTED",
    )

    engine = create_engine(db)
    with begin_connection(engine) as conn:
        row = conn.execute(
            text(
                "SELECT runtime_id, launch_attempt, state, reason_code "
                "FROM worker_instances WHERE runtime_id = 'rt-phase'"
            )
        ).one()
    assert row[0] == "rt-phase"
    assert row[1] == 5
    assert row[2] == "STOPPED"
    assert row[3] == "STOP_REQUESTED"


def test_runtime_journal_sink_manager_stop_request(tmp_path: Path) -> None:
    db = tmp_path / "j.db"
    txt = tmp_path / "j.txt"
    sink = RuntimeJournalSink(
        db_path=db,
        txt_path=txt,
        runtime_id="rt-b",
        launch_attempt=1,
    )
    t0 = datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 1, 15, 12, 0, 1, tzinfo=timezone.utc)
    sink.record_manager_stop_request(
        last_data_event_at=t0,
        last_clock_at=t1,
        last_replay_cursor="cursor-9",
        stop_requested_at=t1,
    )
    lines = txt.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    row = json.loads(lines[0])
    assert row["kind"] == "manager_stop_request"
    assert row["rpc"] == "StopWorker"
    assert row["last_replay_cursor"] == "cursor-9"
    assert row["last_data_event_at"] == "2026-01-15T12:00:00Z"
    assert row["last_clock_at"] == "2026-01-15T12:00:01Z"

    engine = create_engine(db)
    with begin_connection(engine) as conn:
        kind = conn.execute(
            text(
                "SELECT kind, level, reason_code, details_json FROM runtime_state_journal LIMIT 1"
            )
        ).one()
    assert kind[0] == "manager_stop_request"
    assert kind[1] == "INFO"
    assert kind[2] == "MANAGER_STOP_REQUESTED"
    details = json.loads(kind[3] or "{}")
    assert details["rpc"] == "StopWorker"
    assert details["last_replay_cursor"] == "cursor-9"


def test_runtime_journal_sink_startup_step_populates_level_reason_and_details(
    tmp_path: Path,
) -> None:
    db = tmp_path / "j_step.db"
    txt = tmp_path / "j_step.txt"
    sink = RuntimeJournalSink(
        db_path=db,
        txt_path=txt,
        runtime_id="rt-step",
        launch_attempt=1,
    )
    sink.record_startup_step("resolve_artifact_and_entrypoint")

    engine = create_engine(db)
    with begin_connection(engine) as conn:
        row = conn.execute(
            text(
                "SELECT kind, step_name, level, reason_code, details_json "
                "FROM runtime_state_journal LIMIT 1"
            )
        ).one()
    assert row[0] == "startup_step"
    assert row[1] == "resolve_artifact_and_entrypoint"
    assert row[2] == "INFO"
    assert row[3] == "STARTUP_PROGRESS"
    details = json.loads(row[4] or "{}")
    assert details["step_name"] == "resolve_artifact_and_entrypoint"
    assert details["stage"] == "startup"


def test_runtime_journal_sink_heartbeat(tmp_path: Path) -> None:
    db = tmp_path / "j.db"
    txt = tmp_path / "j.txt"
    sink = RuntimeJournalSink(
        db_path=db,
        txt_path=txt,
        runtime_id="rt-c",
        launch_attempt=2,
    )
    t0 = datetime(2026, 1, 15, 12, 0, 2, tzinfo=timezone.utc)
    sink.record_heartbeat(local_state="RUNNING", observed_at=t0)

    lines = txt.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    row = json.loads(lines[0])
    assert row["kind"] == "heartbeat"
    assert row["local_state"] == "RUNNING"
    assert row["observed_at"] == "2026-01-15T12:00:02Z"

    engine = create_engine(db)
    with begin_connection(engine) as conn:
        row = conn.execute(
            text(
                "SELECT runtime_id, launch_attempt, occurred_at, observed_at, details "
                "FROM worker_heartbeat_observations LIMIT 1"
            )
        ).one()
        inst = conn.execute(
            text(
                "SELECT state, last_heartbeat_at, correlation_id, causation_id "
                "FROM worker_instances WHERE runtime_id = 'rt-c'"
            )
        ).one()
    assert row[0] == "rt-c"
    assert row[1] == 2
    assert row[2] == "2026-01-15T12:00:02Z"
    assert row[3] == "2026-01-15T12:00:02Z"
    details = json.loads(row[4] or "{}")
    assert details["local_state"] == "RUNNING"
    assert inst[0] == "RUNNING"
    assert inst[1] == "2026-01-15T12:00:02Z"
    assert inst[2] is None
    assert inst[3] is None


def test_runtime_journal_sink_records_lifecycle_event_to_worker_events(
    tmp_path: Path,
) -> None:
    db = tmp_path / "j.db"
    txt = tmp_path / "j.txt"
    sink = RuntimeJournalSink(
        db_path=db,
        txt_path=txt,
        runtime_id="rt-e",
        launch_attempt=4,
    )
    occurred = datetime(2026, 1, 15, 12, 0, 4, tzinfo=timezone.utc)
    event = LifecycleEventEnvelope(
        event_id="rt-e:4:runtime.heartbeat:abc",
        event_type="worker.lifecycle.heartbeat",
        event_name="runtime.heartbeat",
        event_family="heartbeat",
        event_version=1,
        occurred_at=occurred,
        correlation_id="corr:rt-e:4",
        causation_id="cmd_heartbeat_rt-e_4",
        producer="strategy-worker-runtime",
        payload={"local_state": "RUNNING", "observed_at": occurred},
        runtime_id="rt-e",
        launch_attempt=4,
        worker_identity="rt-e:sv-e:4",
        identity_key="heartbeat:abc",
    )
    sink.record_lifecycle_event(event)

    engine = create_engine(db)
    with begin_connection(engine) as conn:
        row = conn.execute(
            text(
                "SELECT event_id, runtime_id, launch_attempt, event_family, state, correlation_id "
                "FROM worker_events LIMIT 1"
            )
        ).one()
        heartbeat = conn.execute(
            text(
                "SELECT correlation_id, causation_id, details "
                "FROM worker_heartbeat_observations LIMIT 1"
            )
        ).one()
        inst = conn.execute(
            text(
                "SELECT state, reason_code, correlation_id, causation_id, last_heartbeat_at "
                "FROM worker_instances WHERE runtime_id = 'rt-e'"
            )
        ).one()
    assert row[0] == "rt-e:4:runtime.heartbeat:abc"
    assert row[1] == "rt-e"
    assert row[2] == 4
    assert row[3] == "runtime.heartbeat"
    assert row[4] == "RUNNING"
    assert row[5] == "corr:rt-e:4"
    assert inst[0] == "RUNNING"
    assert inst[1] is None
    assert inst[2] == "corr:rt-e:4"
    assert inst[3] == "cmd_heartbeat_rt-e_4"
    assert inst[4] == "2026-01-15T12:00:04Z"
    assert heartbeat[0] == "corr:rt-e:4"
    assert heartbeat[1] == "cmd_heartbeat_rt-e_4"
    hb_details = json.loads(heartbeat[2] or "{}")
    assert hb_details["local_state"] == "RUNNING"


def test_runtime_journal_sink_lifecycle_reason_code_persists_on_phase_update(
    tmp_path: Path,
) -> None:
    db = tmp_path / "j_reason.db"
    txt = tmp_path / "j_reason.txt"
    sink = RuntimeJournalSink(
        db_path=db,
        txt_path=txt,
        runtime_id="rt-r",
        launch_attempt=7,
    )
    observed = datetime(2026, 1, 15, 12, 0, 8, tzinfo=timezone.utc)
    event = LifecycleEventEnvelope(
        event_id="rt-r:7:runtime.terminated:abc",
        event_type="worker.lifecycle.terminated",
        event_name="runtime.terminated",
        event_family="terminated",
        event_version=1,
        occurred_at=observed,
        correlation_id="corr:rt-r:7",
        causation_id="cmd_terminated_rt-r_7",
        producer="strategy-worker-runtime",
        payload={
            "local_state": "STOPPED",
            "reason_code": "STOP_REQUESTED",
            "observed_at": observed,
        },
        runtime_id="rt-r",
        launch_attempt=7,
        worker_identity="rt-r:sv-r:7",
        identity_key="terminated:abc",
    )
    sink.record_lifecycle_event(event)
    sink.record_phase_change(
        previous_phase=WorkerPhase.STOPPING,
        phase=WorkerPhase.STOPPED,
        level="INFO",
        reason_code=None,
    )

    engine = create_engine(db)
    with begin_connection(engine) as conn:
        inst = conn.execute(
            text(
                "SELECT state, reason_code, correlation_id, causation_id "
                "FROM worker_instances WHERE runtime_id = 'rt-r'"
            )
        ).one()
    assert inst[0] == "STOPPED"
    assert inst[1] == "STOP_REQUESTED"
    assert inst[2] == "corr:rt-r:7"
    assert inst[3] == "cmd_terminated_rt-r_7"

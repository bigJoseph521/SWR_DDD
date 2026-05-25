from __future__ import annotations

import logging
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from runtime.application.lifecycle.lifecycle_service import (
    LifecycleService,
    _backtest_window_bounds,
    _coerce_timestamp,
)
from runtime.bootstrap.launch_spec import LaunchSpec
from runtime.bootstrap.validator import LaunchSpecValidator
from runtime.domain.enums import WorkerMode
from runtime.domain.worker_identity import WorkerIdentity
from runtime.infrastructure.clock.clock import SimulatedClock
from runtime.infrastructure.http.srm.manager_gateway import ManagerGateway
from runtime.infrastructure.grpc.replay.replay_gateway import ReplayGateway
from runtime.infrastructure.observability.logger import (
    RuntimeBoundLogger,
    RuntimeLogContext,
    bind_runtime_context,
)
from runtime.application.runtime_dependencies import RuntimeDependencies
from runtime.domain.policies.mode_policy import get_mode_policy


class _FakeManager:
    def emit_signal(self, payload: dict[str, object]) -> dict[str, object]:
        return {"accepted": True}


def _backtest_payload() -> dict[str, object]:
    return {
        "runtime_id": "rt-bt-1",
        "tenant_id": "tenant-1",
        "account_id": "acct-bt-1",
        "strategy_version_id": "sv-1",
        "mode": "BACKTEST",
        "launch_attempt": 1,
        "artifact_uri": "file:///tmp/s",
        "artifact_digest": "sha256:abcd",
        "entrypoint": "strategy.main:Strategy",
        "job_id": "job-1",
        "ts_start": "2025-06-01T00:00:00Z",
        "ts_end": "2025-06-01T23:59:59Z",
    }


def _identity(spec: LaunchSpec) -> WorkerIdentity:
    return WorkerIdentity(
        runtime_id=spec.runtime_id,
        tenant_id=spec.tenant_id,
        strategy_version_id=spec.strategy_version_id,
        mode=spec.mode,
        trader_id=spec.trader_id,
        account_id=spec.account_id,
        artifact_uri=spec.artifact_uri,
        artifact_digest=spec.artifact_digest,
        entrypoint=spec.entrypoint,
        launch_attempt=spec.launch_attempt,
    )


def _log_binder() -> RuntimeBoundLogger:
    logger = logging.getLogger("test-bt-replay")
    logger.handlers.clear()
    logger.propagate = False
    context = RuntimeLogContext(
        runtime_id="rt-bt-1",
        tenant_id="tenant-1",
        worker_identity="rt-bt-1:1",
        launch_attempt=1,
        account_id=None,
        strategy_version_id="sv-1",
        mode="BACKTEST",
    )
    return bind_runtime_context(logger, context)


def test_coerce_timestamp_accepts_proto_style_seconds() -> None:
    dt = _coerce_timestamp({"seconds": 1704067200, "nanos": 0})
    assert dt is not None
    assert dt.tzinfo is not None


def test_backtest_window_bounds_from_launch_spec() -> None:
    spec = LaunchSpec.from_payload(_backtest_payload())
    bounds = _backtest_window_bounds(spec)
    assert bounds is not None
    lo, hi = bounds
    assert lo == datetime(2025, 6, 1, 0, 0, tzinfo=timezone.utc)
    assert hi == datetime(2025, 6, 1, 23, 59, 59, tzinfo=timezone.utc)


def test_backtest_push_replay_stops_after_end_of_stream() -> None:
    payload = _backtest_payload()
    spec = LaunchSpec.from_payload(payload)
    clock = SimulatedClock()
    replay = ReplayGateway(
        get_mode_policy(WorkerMode.BACKTEST), simulated_clock=clock
    )

    def _deps() -> RuntimeDependencies:
        return RuntimeDependencies(
            clock=clock,
            manager=ManagerGateway(
                get_mode_policy(WorkerMode.BACKTEST), _FakeManager(), _identity(spec)
            ),
            risk_order_intent=None,
            replay=replay,
        )

    lifecycle = LifecycleService(
        launch_spec=spec,
        launch_payload=payload,
        worker_identity=_identity(spec),
        launch_spec_validator=LaunchSpecValidator(),
        bootstrap_pipeline=MagicMock(),
        log_binder=_log_binder,
        runtime_dependencies_initializer=_deps,
        strategy_instance_manager=None,
    )

    mid = datetime(2025, 6, 1, 12, 0, tzinfo=timezone.utc)
    ev = {
        "event_id": "e1",
        "event_type": "market.bar",
        "instrument_id": "X",
        "event_time": mid.isoformat().replace("+00:00", "Z"),
        "payload": {},
    }
    r1 = lifecycle.push_replay_context(
        {
            "replay": {"replay_session_id": "s1", "replay_cursor": "c1"},
            "events": [ev],
            "simulated_time": mid.isoformat().replace("+00:00", "Z"),
            "end_of_stream": False,
        }
    )
    assert r1["consumed_count"] == 1

    r2 = lifecycle.push_replay_context(
        {
            "replay": {"replay_session_id": "s1", "replay_cursor": "c2"},
            "events": [ev],
            "simulated_time": mid.isoformat().replace("+00:00", "Z"),
            "end_of_stream": True,
        }
    )
    assert r2["consumed_count"] == 1
    assert lifecycle.backtest_replay_complete is True

    r3 = lifecycle.push_replay_context(
        {
            "replay": {"replay_session_id": "s1", "replay_cursor": "c3"},
            "events": [ev],
            "simulated_time": mid.isoformat().replace("+00:00", "Z"),
            "end_of_stream": False,
        }
    )
    assert r3["consumed_count"] == 0


def test_backtest_skips_events_outside_ts_window() -> None:
    payload = _backtest_payload()
    spec = LaunchSpec.from_payload(payload)
    clock = SimulatedClock()
    replay = ReplayGateway(
        get_mode_policy(WorkerMode.BACKTEST), simulated_clock=clock
    )

    def _deps() -> RuntimeDependencies:
        return RuntimeDependencies(
            clock=clock,
            manager=ManagerGateway(
                get_mode_policy(WorkerMode.BACKTEST), _FakeManager(), _identity(spec)
            ),
            risk_order_intent=None,
            replay=replay,
        )

    lifecycle = LifecycleService(
        launch_spec=spec,
        launch_payload=payload,
        worker_identity=_identity(spec),
        launch_spec_validator=LaunchSpecValidator(),
        bootstrap_pipeline=MagicMock(),
        log_binder=_log_binder,
        runtime_dependencies_initializer=_deps,
        strategy_instance_manager=None,
    )

    too_early = datetime(2025, 5, 1, 0, 0, tzinfo=timezone.utc)
    ok_in = datetime(2025, 6, 1, 12, 0, tzinfo=timezone.utc)
    too_late = datetime(2025, 7, 1, 0, 0, tzinfo=timezone.utc)
    r = lifecycle.push_replay_context(
        {
            "replay": {"replay_session_id": "s1", "replay_cursor": "c1"},
            "events": [
                {
                    "event_id": "a",
                    "event_type": "market.bar",
                    "instrument_id": "X",
                    "event_time": too_early.isoformat().replace("+00:00", "Z"),
                    "payload": {},
                },
                {
                    "event_id": "b",
                    "event_type": "market.bar",
                    "instrument_id": "X",
                    "event_time": ok_in.isoformat().replace("+00:00", "Z"),
                    "payload": {},
                },
                {
                    "event_id": "c",
                    "event_type": "market.bar",
                    "instrument_id": "X",
                    "event_time": too_late.isoformat().replace("+00:00", "Z"),
                    "payload": {},
                },
            ],
            "simulated_time": ok_in.isoformat().replace("+00:00", "Z"),
            "end_of_stream": False,
        }
    )
    assert r["consumed_count"] == 1


def test_backtest_push_replay_prints_data_trace_when_enabled(
    capsys: pytest.CaptureFixture[str],
) -> None:
    payload = _backtest_payload()
    spec = LaunchSpec.from_payload(payload)
    clock = SimulatedClock()
    replay = ReplayGateway(
        get_mode_policy(WorkerMode.BACKTEST), simulated_clock=clock
    )

    def _deps() -> RuntimeDependencies:
        return RuntimeDependencies(
            clock=clock,
            manager=ManagerGateway(
                get_mode_policy(WorkerMode.BACKTEST), _FakeManager(), _identity(spec)
            ),
            risk_order_intent=None,
            replay=replay,
        )

    lifecycle = LifecycleService(
        launch_spec=spec,
        launch_payload=payload,
        worker_identity=_identity(spec),
        launch_spec_validator=LaunchSpecValidator(),
        bootstrap_pipeline=MagicMock(),
        log_binder=_log_binder,
        runtime_dependencies_initializer=_deps,
        strategy_instance_manager=None,
        worker_runtime_settings=SimpleNamespace(replay_ingress_trace_payload=True),
    )

    mid = datetime(2025, 6, 1, 12, 0, tzinfo=timezone.utc)
    lifecycle.push_replay_context(
        {
            "replay": {"replay_session_id": "s1", "replay_cursor": "c1"},
            "events": [
                {
                    "event_id": "e-trace-1",
                    "event_type": "market.bar",
                    "instrument_id": "AAPL",
                    "event_time": mid.isoformat().replace("+00:00", "Z"),
                    "payload": {
                        "open": 1.0,
                        "high": 2.0,
                        "low": 0.5,
                        "close": 1.5,
                        "volume": 100,
                    },
                }
            ],
            "simulated_time": mid.isoformat().replace("+00:00", "Z"),
            "end_of_stream": False,
        }
    )
    out = capsys.readouterr().out
    assert "Replay data (trace)" in out
    assert "replay.data.received" in out
    assert "e-trace-1" in out
    assert "payload.close" in out or "1.5" in out

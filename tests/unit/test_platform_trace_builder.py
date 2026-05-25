from __future__ import annotations

from runtime.domain.launch_spec import LaunchSpec
from runtime.domain.platform_trace_factory import build_platform_trace_spec_from_launch


def test_build_platform_trace_spec_from_launch_maps_launch_fields() -> None:
    launch = LaunchSpec.from_payload(
        {
            "runtime_id": "rt-1",
            "strategy_version_id": "sv-9",
            "mode": "PAPER",
            "launch_attempt": 1,
            "artifact_uri": "registry-local:///bundle.zip",
            "entrypoint": "sma_crossover:Strategy",
            "account_id": "acct-1",
            "correlation_id": "corr-bundle",
            "parameters": {"symbol": "AAPL"},
        }
    )
    trace = build_platform_trace_spec_from_launch(
        launch_spec=launch,
        launch_payload={
            "strategy_id": "strat-42",
            "request_id": "req-explicit",
        },
    )
    assert trace.strategy_id == "strat-42"
    assert trace.strategy_version_id == "sv-9"
    assert trace.correlation_id == "corr-bundle"
    assert trace.request_id == "req-explicit"


def test_build_platform_trace_spec_from_launch_defaults_request_id() -> None:
    launch = LaunchSpec.from_payload(
        {
            "runtime_id": "rt-2",
            "strategy_version_id": "sv-1",
            "mode": "BACKTEST",
            "launch_attempt": 3,
            "artifact_uri": "file:///x",
            "artifact_digest": "sha256:aa",
            "entrypoint": "m:s",
            "account_id": "acct-1",
            "job_id": "bt-1",
            "ts_start": "2025-01-01T00:00:00Z",
            "ts_end": "2025-01-02T00:00:00Z",
        }
    )
    trace = build_platform_trace_spec_from_launch(
        launch_spec=launch,
        launch_payload={},
    )
    assert trace.strategy_id is None
    assert trace.request_id == "rt-2:3"
    assert trace.correlation_id is None

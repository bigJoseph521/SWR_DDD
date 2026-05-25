from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

from alphovex_sdk.enums.order import OrderSide, OrderType, TimeInForce
from runtime.bootstrap.launch_spec import LaunchSpec
from runtime.bootstrap.replay_runtime_support import ReplayOrderIntent
from runtime.domain.enums import RuntimeMode
from runtime.domain.errors import RuntimeWorkerReasonCode
from runtime.domain.worker_identity import WorkerIdentity
from runtime.integration.clock import SimulatedClock
from runtime.integration.manager_gateway import ManagerGateway
from runtime.integration.oms_gateway import OmsGateway
from runtime.integration.replay_gateway import ReplayGateway
from runtime.runtime.dependencies import RuntimeDependencies
from runtime.runtime.mode_policy import get_mode_policy
from runtime.runtime.sdk_order_intent_submission import (
    build_sdk_order_intent_submitter,
    sdk_order_intent_to_backtest_grpc_payload,
    sdk_order_intent_to_oms_grpc_payload,
)


def _bt_spec() -> LaunchSpec:
    return LaunchSpec.from_payload(
        {
            "runtime_id": "rt-1",
            "tenant_id": "t1",
            "strategy_version_id": "sv1",
            "mode": "BACKTEST",
            "launch_attempt": 1,
            "artifact_uri": "file:///x",
            "artifact_digest": "sha256:aa",
            "entrypoint": "m:s",
            "account_id": "acct-bt-1",
            "job_id": "bt1",
            "ts_start": "2025-01-01T00:00:00Z",
            "ts_end": "2025-01-02T00:00:00Z",
        }
    )


def _paper_spec(*, job_id: str = "") -> LaunchSpec:
    p: dict[str, object] = {
        "runtime_id": "rt-1",
        "tenant_id": "t1",
        "strategy_version_id": "sv1",
        "mode": "PAPER",
        "launch_attempt": 1,
        "artifact_uri": "file:///x",
        "artifact_digest": "sha256:aa",
        "entrypoint": "m:s",
        "account_id": "acct-1",
        "validated_parameter_identity": "vp-1",
    }
    if job_id:
        p["job_id"] = job_id
    return LaunchSpec.from_payload(p)


def test_sdk_order_intent_to_oms_grpc_payload_includes_symbol() -> None:
    spec = LaunchSpec.from_payload(
        {
            "runtime_id": "rt-1",
            "tenant_id": "t1",
            "strategy_version_id": "sv1",
            "mode": "PAPER",
            "launch_attempt": 1,
            "artifact_uri": "file:///x",
            "artifact_digest": "sha256:aa",
            "entrypoint": "m:s",
            "account_id": "acct-1",
            "validated_parameter_identity": "vp-1",
            "symbol": "MSFT",
        }
    )
    intent = ReplayOrderIntent(
        instrument_id="MSFT",
        side=OrderSide.BUY,
        quantity=1.0,
        price=1.0,
        order_type=OrderType.MARKET,
        limit_price=None,
        stop_price=None,
        time_in_force=TimeInForce.DAY,
    )
    out = sdk_order_intent_to_oms_grpc_payload(
        intent,
        launch_spec=spec,
        _worker_identity=_identity(spec),
        correlation_id="",
    )
    assert out["symbol"] == "MSFT"


def test_sdk_order_intent_to_oms_grpc_payload_includes_job_id() -> None:
    spec = _paper_spec(job_id="job-77")
    intent = ReplayOrderIntent(
        instrument_id="AAPL",
        side=OrderSide.BUY,
        quantity=1.0,
        price=1.0,
        order_type=OrderType.MARKET,
        limit_price=None,
        stop_price=None,
        time_in_force=TimeInForce.DAY,
    )
    out = sdk_order_intent_to_oms_grpc_payload(
        intent,
        launch_spec=spec,
        _worker_identity=_identity(spec),
        correlation_id="corr-x",
    )
    assert out["job_id"] == "job-77"


def test_sdk_order_intent_to_oms_grpc_payload_prefers_launch_spec_correlation_id() -> (
    None
):
    spec = LaunchSpec.from_payload(
        {
            "runtime_id": "rt-1",
            "tenant_id": "t1",
            "strategy_version_id": "sv1",
            "mode": "PAPER",
            "launch_attempt": 1,
            "artifact_uri": "file:///x",
            "artifact_digest": "sha256:aa",
            "entrypoint": "m:s",
            "account_id": "acct-1",
            "validated_parameter_identity": "vp-1",
            "correlation_id": "corr-from-bundle",
        }
    )
    intent = ReplayOrderIntent(
        instrument_id="AAPL",
        side=OrderSide.BUY,
        quantity=1.0,
        price=1.0,
        order_type=OrderType.MARKET,
        limit_price=None,
        stop_price=None,
        time_in_force=TimeInForce.DAY,
    )
    out = sdk_order_intent_to_oms_grpc_payload(
        intent,
        launch_spec=spec,
        _worker_identity=_identity(spec),
        correlation_id="corr-from-env",
    )
    assert out["correlation_id"] == "corr-from-bundle"


def test_sdk_order_intent_to_oms_grpc_payload_correlation_id_falls_back_to_arg_then_uuid() -> (
    None
):
    spec = _paper_spec()
    intent = ReplayOrderIntent(
        instrument_id="AAPL",
        side=OrderSide.BUY,
        quantity=1.0,
        price=1.0,
        order_type=OrderType.MARKET,
        limit_price=None,
        stop_price=None,
        time_in_force=TimeInForce.DAY,
    )
    out = sdk_order_intent_to_oms_grpc_payload(
        intent,
        launch_spec=spec,
        _worker_identity=_identity(spec),
        correlation_id="corr-env-only",
    )
    assert out["correlation_id"] == "corr-env-only"

    out2 = sdk_order_intent_to_oms_grpc_payload(
        intent,
        launch_spec=spec,
        _worker_identity=_identity(spec),
        correlation_id="",
    )
    assert len(out2["correlation_id"]) == 32
    assert all(c in "0123456789abcdef" for c in out2["correlation_id"])


def test_sdk_order_intent_to_backtest_grpc_payload_falls_back_job_id() -> None:
    spec = LaunchSpec.from_payload(
        {
            "runtime_id": "rt-1",
            "tenant_id": "t1",
            "strategy_version_id": "sv1",
            "mode": "BACKTEST",
            "launch_attempt": 1,
            "artifact_uri": "file:///x",
            "artifact_digest": "sha256:aa",
            "entrypoint": "m:s",
            "account_id": "acct-from-settings",
            "job_id": "job-from-settings",
            "ts_start": "2025-01-01T00:00:00Z",
            "ts_end": "2025-01-02T00:00:00Z",
            "symbol": "QQQ",
            "correlation_id": "corr-bt",
        }
    )
    intent = ReplayOrderIntent(
        instrument_id="AAPL",
        side=OrderSide.BUY,
        quantity=1.0,
        price=1.0,
        order_type=OrderType.MARKET,
        limit_price=None,
        stop_price=None,
        time_in_force=TimeInForce.DAY,
    )
    out = sdk_order_intent_to_backtest_grpc_payload(
        intent,
        launch_spec=spec,
        _worker_identity=_identity(spec),
    )
    assert out["job_id"] == "job-from-settings"
    assert out["symbol"] == "QQQ"
    assert out["correlation_id"] == "corr-bt"
    assert out["account_id"] == "acct-from-settings"


def test_sdk_order_intent_to_backtest_grpc_payload_account_id_falls_back_to_trader() -> (
    None
):
    """Programmatic LaunchSpec may omit account_id; wire uses trader_id like OMS."""
    spec = LaunchSpec(
        runtime_id="rt-1",
        tenant_id="t1",
        strategy_version_id="sv1",
        mode=RuntimeMode.BACKTEST,
        launch_attempt=1,
        artifact_uri="file:///x",
        entrypoint="m:s",
        trader_id="tr-fallback",
        account_id=None,
        artifact_digest="sha256:aa",
        job_id="j1",
        ts_start="2025-01-01T00:00:00Z",
        ts_end="2025-01-02T00:00:00Z",
        symbol="QQQ",
        correlation_id="corr-bt",
    )
    intent = ReplayOrderIntent(
        instrument_id="AAPL",
        side=OrderSide.BUY,
        quantity=1.0,
        price=1.0,
        order_type=OrderType.MARKET,
        limit_price=None,
        stop_price=None,
        time_in_force=TimeInForce.DAY,
    )
    out = sdk_order_intent_to_backtest_grpc_payload(
        intent,
        launch_spec=spec,
        _worker_identity=_identity(spec),
    )
    assert out["account_id"] == "tr-fallback"


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


class _ReplayClientIngestOnly:
    def ingest_replay_tick(self, payload: dict[str, object]) -> dict[str, object]:
        return {"accepted": True, "replay": payload}


class _RiskGrpcClientNoSubmit:
    """Simulates misconfigured risk gRPC client (no ``submit_order_intent``)."""


def test_backtest_submitter_exists_when_replay_client_lacks_submit_if_callback_provided() -> (
    None
):
    """Regression: journal / bridge need a submitter even when risk gRPC client cannot submit."""
    spec = _bt_spec()
    clock = SimulatedClock()
    clock.set_time(datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc))
    replay_only_ingest = _ReplayClientIngestOnly()
    policy_bt = get_mode_policy(RuntimeMode.BACKTEST)

    deps = RuntimeDependencies(
        clock=clock,
        manager=ManagerGateway(policy_bt, MagicMock(), _identity(spec)),
        oms=OmsGateway(policy_bt, _RiskGrpcClientNoSubmit()),
        replay=ReplayGateway(
            policy_bt,
            replay_only_ingest,
            simulated_clock=clock,
        ),
    )

    seen: list[tuple[str, dict, dict]] = []

    def _cb(source: str, payload: dict, result: dict) -> None:
        seen.append((source, payload, result))

    submit = build_sdk_order_intent_submitter(
        dependencies=deps,
        launch_spec=spec,
        worker_identity=_identity(spec),
        on_order_intent_result=_cb,
    )
    assert submit is not None

    intent = ReplayOrderIntent(
        instrument_id="AAPL",
        side=OrderSide.BUY,
        quantity=1.0,
        price=1.0,
        order_type=OrderType.MARKET,
        limit_price=None,
        stop_price=None,
        time_in_force=TimeInForce.DAY,
    )
    out = submit(intent)
    assert (
        out.get("reason_code")
        == RuntimeWorkerReasonCode.BOUND_DEPENDENCY_UNAVAILABLE.value
    )
    assert len(seen) == 1
    assert seen[0][0] == "oms"
    assert seen[0][1].get("created_at") == datetime(
        2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc
    )
    assert isinstance(seen[0][1].get("requested_at"), datetime)


def test_backtest_created_at_prefers_latest_market_event_snapshot() -> None:
    """created_at follows the latest bar/tick/quote time when the runtime supplies it."""
    spec = _bt_spec()
    clock = SimulatedClock()
    clock.set_time(datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc))
    bar_ts = datetime(2025, 6, 1, 12, 5, 30, tzinfo=timezone.utc)
    replay_only_ingest = _ReplayClientIngestOnly()
    policy_bt = get_mode_policy(RuntimeMode.BACKTEST)
    deps = RuntimeDependencies(
        clock=clock,
        manager=ManagerGateway(policy_bt, MagicMock(), _identity(spec)),
        oms=OmsGateway(policy_bt, _RiskGrpcClientNoSubmit()),
        replay=ReplayGateway(
            policy_bt,
            replay_only_ingest,
            simulated_clock=clock,
        ),
    )
    seen: list[tuple[str, dict, dict]] = []

    def _cb(source: str, payload: dict, result: dict) -> None:
        seen.append((source, payload, result))

    submit = build_sdk_order_intent_submitter(
        dependencies=deps,
        launch_spec=spec,
        worker_identity=_identity(spec),
        on_order_intent_result=_cb,
        latest_market_event_at=lambda: bar_ts,
    )
    assert submit is not None
    intent = ReplayOrderIntent(
        instrument_id="AAPL",
        side=OrderSide.BUY,
        quantity=1.0,
        price=1.0,
        order_type=OrderType.MARKET,
        limit_price=None,
        stop_price=None,
        time_in_force=TimeInForce.DAY,
    )
    submit(intent)
    assert len(seen) == 1
    assert seen[0][1].get("created_at") == bar_ts


def test_backtest_submitter_calls_risk_grpc_when_client_supports_submit() -> None:
    spec = _bt_spec()
    clock = SimulatedClock()
    clock.set_time(datetime(2025, 6, 2, 15, 30, 0, tzinfo=timezone.utc))
    replay_client = MagicMock()
    replay_client.ingest_replay_tick = MagicMock(return_value={"accepted": True})
    risk_client = MagicMock()
    risk_client.submit_order_intent = MagicMock(return_value={"accepted": True})
    policy_bt = get_mode_policy(RuntimeMode.BACKTEST)

    deps = RuntimeDependencies(
        clock=clock,
        manager=ManagerGateway(policy_bt, MagicMock(), _identity(spec)),
        oms=OmsGateway(policy_bt, risk_client),
        replay=ReplayGateway(
            policy_bt,
            replay_client,
            simulated_clock=clock,
        ),
    )

    submit = build_sdk_order_intent_submitter(
        dependencies=deps,
        launch_spec=spec,
        worker_identity=_identity(spec),
        on_order_intent_result=None,
    )
    assert submit is not None

    intent = ReplayOrderIntent(
        instrument_id="AAPL",
        side=OrderSide.BUY,
        quantity=1.0,
        price=1.0,
        order_type=OrderType.MARKET,
        limit_price=None,
        stop_price=None,
        time_in_force=TimeInForce.DAY,
    )
    submit(intent)
    risk_client.submit_order_intent.assert_called_once()


def test_backtest_submitter_invokes_callback_when_simulated_clock_not_yet_set() -> None:
    """Orders before the first bar used to skip journaling because SimulatedClock.now() raised."""
    spec = _bt_spec()
    clock = SimulatedClock()
    replay_client = MagicMock()
    replay_client.ingest_replay_tick = MagicMock(return_value={"accepted": True})
    risk_client = MagicMock()
    risk_client.submit_order_intent = MagicMock(return_value={"accepted": True})
    policy_bt = get_mode_policy(RuntimeMode.BACKTEST)
    deps = RuntimeDependencies(
        clock=clock,
        manager=ManagerGateway(policy_bt, MagicMock(), _identity(spec)),
        oms=OmsGateway(policy_bt, risk_client),
        replay=ReplayGateway(
            policy_bt,
            replay_client,
            simulated_clock=clock,
        ),
    )
    seen: list[tuple[str, dict, dict]] = []

    def _cb(source: str, payload: dict, result: dict) -> None:
        seen.append((source, payload, result))

    submit = build_sdk_order_intent_submitter(
        dependencies=deps,
        launch_spec=spec,
        worker_identity=_identity(spec),
        on_order_intent_result=_cb,
    )
    assert submit is not None
    intent = ReplayOrderIntent(
        instrument_id="AAPL",
        side=OrderSide.BUY,
        quantity=1.0,
        price=1.0,
        order_type=OrderType.MARKET,
        limit_price=None,
        stop_price=None,
        time_in_force=TimeInForce.DAY,
    )
    submit(intent)
    assert len(seen) == 1
    assert isinstance(seen[0][1].get("requested_at"), datetime)


def test_disable_order_intent_grpc_skips_network_but_invokes_callback_for_sqlite_journal() -> (
    None
):
    spec = _bt_spec()
    clock = SimulatedClock()
    clock.set_time(datetime(2025, 7, 15, 9, 0, 0, tzinfo=timezone.utc))
    replay_client = MagicMock()
    replay_client.ingest_replay_tick = MagicMock(return_value={"accepted": True})
    risk_client = MagicMock()
    risk_client.submit_order_intent = MagicMock(return_value={"accepted": True})
    policy_bt = get_mode_policy(RuntimeMode.BACKTEST)

    deps = RuntimeDependencies(
        clock=clock,
        manager=ManagerGateway(policy_bt, MagicMock(), _identity(spec)),
        oms=OmsGateway(policy_bt, risk_client),
        replay=ReplayGateway(
            policy_bt,
            replay_client,
            simulated_clock=clock,
        ),
    )

    seen: list[tuple[str, dict, dict]] = []

    def _cb(source: str, payload: dict, result: dict) -> None:
        seen.append((source, payload, result))

    submit = build_sdk_order_intent_submitter(
        dependencies=deps,
        launch_spec=spec,
        worker_identity=_identity(spec),
        on_order_intent_result=_cb,
        disable_order_intent_grpc=True,
    )
    assert submit is not None

    intent = ReplayOrderIntent(
        instrument_id="AAPL",
        side=OrderSide.BUY,
        quantity=1.0,
        price=1.0,
        order_type=OrderType.MARKET,
        limit_price=None,
        stop_price=None,
        time_in_force=TimeInForce.DAY,
    )
    submit(intent)
    risk_client.submit_order_intent.assert_not_called()
    assert len(seen) == 1
    assert seen[0][1].get("created_at") == datetime(
        2025, 7, 15, 9, 0, 0, tzinfo=timezone.utc
    )
    assert isinstance(seen[0][1].get("requested_at"), datetime)

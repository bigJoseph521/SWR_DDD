from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from runtime.bootstrap.deployment_runtime_context_bootstrap import (
    worker_bundle_dict_from_runtime_context_response,
)
from runtime.bootstrap.launch_spec import LaunchSpec
from runtime.bootstrap.replay_sdk_bridge import (
    _seed_cash_from_launch_payload,
    build_replay_sdk_bridge,
)
from runtime.bootstrap.strategy_bundle_loader import raw_dict_to_launch_payload
from runtime.domain.enums import RuntimeMode
from runtime.domain.worker_identity import WorkerIdentity
from runtime.integration.clock import SimulatedClock
from runtime.strategy_contract.sdk_runtime_types import (
    AssetClass,
    ParameterSchema,
    StrategyMetadata,
)


def test_worker_bundle_maps_initial_cash() -> None:
    bundle = worker_bundle_dict_from_runtime_context_response(
        {
            "job_id": "dep-1",
            "initial_cash": {"amount": "95000.50", "currency": "usd"},
        },
        identity={"runtime_id": "rt-1", "strategy_version_id": "sv-1"},
    )
    assert bundle["initial_cash"] == {"amount": "95000.50", "currency": "usd"}


def test_seed_cash_from_launch_payload() -> None:
    currency, cash = _seed_cash_from_launch_payload(
        {"initial_cash": {"amount": "100000", "currency": "USD"}}
    )
    assert currency == "USD"
    assert cash == 100_000.0


def test_seed_cash_default_without_initial_cash() -> None:
    currency, cash = _seed_cash_from_launch_payload({})
    assert currency == "USD"
    assert cash == 1_000_000.0


def test_seed_cash_uppercases_currency() -> None:
    currency, cash = _seed_cash_from_launch_payload(
        {"initial_cash": {"amount": "900.00", "currency": "usd"}}
    )
    assert currency == "USD"
    assert cash == 900.0


def test_raw_dict_to_launch_payload_preserves_initial_cash() -> None:
    payload, _, _, _ = raw_dict_to_launch_payload(
        {
            "runtime_id": "rt-1",
            "strategy_version_id": "sv-1",
            "mode": "PAPER",
            "entrypoint": "s:Strategy",
            "artifact_uri": "file:///tmp/x.zip",
            "artifact_digest": "sha256:00",
            "job_id": "dep-1",
            "initial_cash": {"amount": "900.00", "currency": "USD"},
        },
        base_dir=Path("/tmp"),
    )
    assert payload["initial_cash"] == {"amount": "900.00", "currency": "USD"}


def test_build_replay_sdk_bridge_seeds_account_cash_balance() -> None:
    launch_payload, _, _, _ = raw_dict_to_launch_payload(
        {
            "runtime_id": "rt-1",
            "strategy_version_id": "sv-1",
            "mode": "PAPER",
            "entrypoint": "s:Strategy",
            "artifact_uri": "file:///tmp/x.zip",
            "artifact_digest": "sha256:00",
            "job_id": "dep-1",
            "account_id": "acct-1",
            "launch_attempt": 1,
            "initial_cash": {"amount": "900.00", "currency": "USD"},
        },
        base_dir=Path("/tmp"),
    )
    launch_spec = LaunchSpec.from_payload(launch_payload)
    assert launch_payload["initial_cash"] == {"amount": "900.00", "currency": "USD"}
    clock = SimulatedClock()
    clock.set_time(datetime(2026, 1, 1, tzinfo=timezone.utc))

    class _Strat:
        @classmethod
        def get_metadata(cls) -> StrategyMetadata:
            return StrategyMetadata(
                name="t",
                description="",
                version="1",
                author="",
                supported_asset_classes=(AssetClass.EQUITY,),
                parameter_schema=ParameterSchema(parameters={}),
            )

        @classmethod
        def build_parameter_schema(cls) -> ParameterSchema:
            return ParameterSchema(parameters={})

    bridge = build_replay_sdk_bridge(
        strategy=_Strat(),
        launch_spec=launch_spec,
        worker_identity=WorkerIdentity(
            runtime_id="rt-1",
            tenant_id="t1",
            strategy_version_id="sv-1",
            mode=RuntimeMode.PAPER,
            account_id="acct-1",
            artifact_uri="file:///tmp/x.zip",
            entrypoint="s:Strategy",
        ),
        simulated_clock=clock,
        launch_payload=launch_payload,
    )
    balance = bridge.strategy_context.account.portfolio().balance
    assert balance.cash_balance == 900.0
    assert balance.buying_power == 900.0
    assert balance.equity == 900.0

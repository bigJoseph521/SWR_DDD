from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from runtime.domain.launch_spec import LaunchSpec
from runtime.domain.platform_trace_factory import build_platform_trace_spec_from_launch
from runtime.infrastructure.config.settings import Settings
from runtime.domain.enums import WorkerMode
from runtime.domain.model.platform_trace_spec import PlatformTraceSpec
from runtime.domain.model.runtime_channel_spec import RuntimeChannelSpec
from runtime.domain.model.runtime_identity_spec import RuntimeIdentitySpec
from runtime.domain.model.strategy_artifact_spec import StrategyArtifactSpec
from runtime.domain.model.strategy_calculation_spec import StrategyCalculationSpec


@dataclass(frozen=True, slots=True)
class BuiltRuntimeSpecs:
    artifact: StrategyArtifactSpec
    calculation: StrategyCalculationSpec
    channels: RuntimeChannelSpec
    identity: RuntimeIdentitySpec
    trace: PlatformTraceSpec


def _strategy_params_from_payload(payload: Mapping[str, object]) -> Mapping[str, Any]:
    for key in ("strategy_params", "parameters"):
        raw = payload.get(key)
        if isinstance(raw, dict):
            return dict(raw)
    return {}


def build_platform_trace_from_settings(settings: Settings) -> PlatformTraceSpec:
    return build_platform_trace_spec_from_launch(
        launch_spec=settings.launch_spec,
        launch_payload=settings.launch_payload,
    )


def build_runtime_specs_from_settings(settings: Settings) -> BuiltRuntimeSpecs:
    launch = settings.launch_spec
    payload = settings.launch_payload

    artifact = StrategyArtifactSpec(
        strategy_uri=launch.artifact_uri,
        artifact_digest=launch.artifact_digest,
        entrypoint=launch.entrypoint,
    )

    params = _strategy_params_from_payload(payload)
    bar_tf = (settings.replay_bar_timeframe or "1m").strip() or "1m"
    calculation = StrategyCalculationSpec(
        params=params,
        symbol=launch.symbol,
        bar_timeframe=bar_tf,
    )

    portfolio_channel = settings.portfolio_update_channel_prefix
    if settings.market_data_redis_stream_bars_1m:
        market_channel = settings.market_data_redis_stream_bars_1m
    else:
        market_channel = None

    channels = RuntimeChannelSpec(
        market_data_channel=market_channel,
        order_intent_report_channel=settings.risk_grpc_target or None,
        portfolio_update_channel=portfolio_channel,
    )

    deployment_id = settings.deployment_id or None
    backtest_job_id = launch.job_id
    identity = RuntimeIdentitySpec(
        runtime_id=launch.runtime_id,
        mode=launch.mode,
        deployment_id=deployment_id if launch.mode is not WorkerMode.BACKTEST else None,
        backtest_job_id=backtest_job_id if launch.mode is WorkerMode.BACKTEST else None,
        account_id=launch.account_id,
        portfolio_id=None,
        risk_snapshot_id=None,
    )

    trace = build_platform_trace_from_settings(settings)

    return BuiltRuntimeSpecs(
        artifact=artifact,
        calculation=calculation,
        channels=channels,
        identity=identity,
        trace=trace,
    )

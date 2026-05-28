"""Composition root for backtest-runner subprocess (stdio JSONL child)."""

from __future__ import annotations

import inspect
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from runtime.application.backtest_runner.loaded_session import LoadedSubprocessSession
from runtime.application.backtest_runner.session import BacktestRunnerSession
from runtime.application.strategy_execution.strategy_adapter import StrategyAdapter
from runtime.bootstrap.sdk_contract_validator import (
    BootstrapPipeline,
    SdkContractValidator,
)
from runtime.domain.bootstrap_failures import (
    ArtifactFetchFailure,
    ArtifactVerificationFailure,
    BootstrapFailure,
    BootstrapStage,
    EntrypointLoadFailure,
    SDKContractFailure,
)
from runtime.domain.enums import WorkerMode
from runtime.domain.launch_spec import LaunchSpec, LaunchSpecValidationError
from runtime.domain.worker_identity import WorkerIdentity
from runtime.infrastructure.backtest_runner.order_intent_collector import (
    StdioOrderIntentCollector,
)
from runtime.infrastructure.backtest_runner.wire_adapters import (
    MarketDataWireAdapter,
    OrderIntentWireAdapter,
    PortfolioSyncAdapter,
    PortfolioWireAdapter,
)
from runtime.infrastructure.clock.clock import SimulatedClock
from runtime.infrastructure.sdk.runtime_sdk_bridge import (
    RuntimeSdkBridge,
    build_runtime_sdk_bridge,
)
from runtime.infrastructure.strategy_loader.artifact_fetcher import ArtifactFetcher
from runtime.infrastructure.strategy_loader.artifact_verifier import ArtifactVerifier
from runtime.infrastructure.strategy_loader.entrypoint_loader import EntrypointLoader


class StrategyBootstrapError(Exception):
    """Deterministic subprocess strategy load failure."""

    def __init__(
        self, code: str, message: str, *, details: Mapping[str, Any] | None = None
    ) -> None:
        normalized = code.strip().upper()
        if not normalized:
            raise ValueError("code must not be blank")
        super().__init__(message)
        self.code = normalized
        self.message = message
        self.details = dict(details or {})


class _SubprocessSdkContractValidator(SdkContractValidator):
    def _collect_mypy_validation(self, *, entrypoint: Any) -> dict[str, Any] | None:
        _ = entrypoint
        return None


def _artifact_path_to_uri(artifact_path: str) -> str:
    raw = artifact_path.strip()
    if not raw:
        raise StrategyBootstrapError(
            "INIT_VALIDATION_FAILED",
            "artifact_path must not be empty.",
        )
    if "://" in raw:
        return raw
    path = Path(raw)
    if path.is_absolute():
        return path.as_uri()
    return str(path.resolve())


def _map_bootstrap_failure(exc: BootstrapFailure) -> StrategyBootstrapError:
    if isinstance(exc, (ArtifactFetchFailure, ArtifactVerificationFailure)):
        return StrategyBootstrapError(
            "ARTIFACT_LOAD_FAILED",
            exc.message,
            details={
                "stage": exc.stage.value,
                "reason_code": exc.reason_code,
                **exc.details,
            },
        )
    if isinstance(exc, EntrypointLoadFailure):
        return StrategyBootstrapError(
            "ENTRYPOINT_LOAD_FAILED",
            exc.message,
            details={
                "stage": exc.stage.value,
                "reason_code": exc.reason_code,
                **exc.details,
            },
        )
    if isinstance(exc, SDKContractFailure):
        return StrategyBootstrapError(
            "STRATEGY_CONTRACT_INVALID",
            exc.message,
            details={
                "stage": exc.stage.value,
                "reason_code": exc.reason_code,
                **exc.details,
            },
        )
    if exc.stage is BootstrapStage.ARTIFACT_FETCH:
        return StrategyBootstrapError(
            "ARTIFACT_LOAD_FAILED", exc.message, details=dict(exc.details)
        )
    if exc.stage is BootstrapStage.ENTRYPOINT_LOAD:
        return StrategyBootstrapError(
            "ENTRYPOINT_LOAD_FAILED", exc.message, details=dict(exc.details)
        )
    if exc.stage is BootstrapStage.SDK_VALIDATE:
        return StrategyBootstrapError(
            "STRATEGY_CONTRACT_INVALID", exc.message, details=dict(exc.details)
        )
    return StrategyBootstrapError(
        "STRATEGY_INIT_FAILED",
        exc.message,
        details={
            "stage": exc.stage.value,
            "reason_code": exc.reason_code,
            **exc.details,
        },
    )


def _load_strategy_from_launch_payload(
    launch_payload: dict[str, object],
    *,
    work_root: Path | None = None,
    default_timeframe: str = "1m",
) -> LoadedSubprocessSession:
    try:
        launch_spec = LaunchSpec.from_payload(launch_payload)
    except LaunchSpecValidationError as exc:
        raise StrategyBootstrapError(
            "INIT_VALIDATION_FAILED",
            "LaunchSpec validation failed.",
            details={"reason": exc.reason, "field_errors": dict(exc.field_errors)},
        ) from exc

    root = work_root
    if root is None:
        root = Path(tempfile.mkdtemp(prefix="swr-backtest-subprocess-"))
    root.mkdir(parents=True, exist_ok=True)
    materialized_root = root / "materialized"
    materialized_root.mkdir(parents=True, exist_ok=True)

    pipeline = BootstrapPipeline(
        fetcher=ArtifactFetcher(work_root=materialized_root),
        verifier=ArtifactVerifier(),
        entrypoint_loader=EntrypointLoader(),
        sdk_validator=_SubprocessSdkContractValidator(),
    )
    bootstrap = pipeline.run(launch_spec)
    if not bootstrap.success or bootstrap.success_payload is None:
        assert bootstrap.failure is not None
        raise _map_bootstrap_failure(bootstrap.failure) from bootstrap.failure

    entrypoint = bootstrap.success_payload.entrypoint
    symbol = entrypoint.symbol
    try:
        if inspect.isclass(symbol):
            strategy = symbol()
        else:
            strategy = symbol
    except Exception as exc:
        raise StrategyBootstrapError(
            "STRATEGY_INIT_FAILED",
            "Strategy instantiation failed.",
            details={
                "entrypoint": entrypoint.entrypoint_spec,
                "exception_type": type(exc).__name__,
                "exception_message": str(exc),
            },
        ) from exc

    worker_identity = WorkerIdentity(
        runtime_id=launch_spec.runtime_id,
        tenant_id=launch_spec.tenant_id,
        strategy_version_id=launch_spec.strategy_version_id,
        mode=launch_spec.mode,
        trader_id=None,
        account_id=launch_spec.account_id,
        artifact_uri=launch_spec.artifact_uri,
        artifact_digest=launch_spec.artifact_digest,
        entrypoint=launch_spec.entrypoint,
        launch_attempt=launch_spec.launch_attempt,
    )
    order_collector = StdioOrderIntentCollector()
    params = launch_payload.get("parameters")
    timeframe = default_timeframe
    if isinstance(params, dict):
        tf = params.get("timeframe")
        if isinstance(tf, str) and tf.strip():
            timeframe = tf.strip()
    bridge: RuntimeSdkBridge = build_runtime_sdk_bridge(
        strategy=strategy,
        launch_spec=launch_spec,
        worker_identity=worker_identity,
        simulated_clock=SimulatedClock(),
        launch_payload=launch_payload,
        default_timeframe=timeframe,
        submit_sdk_order_intent=order_collector.submit,
    )
    adapter = StrategyAdapter(strategy, runtime_sdk_bridge=bridge)
    start_result = adapter.bind_and_start()
    if not start_result.ok:
        raise StrategyBootstrapError(
            "STRATEGY_INIT_FAILED",
            "Strategy initialization hook failed.",
            details={
                "reason_code": start_result.reason_code,
                "error_code": start_result.error_code,
                **dict(start_result.diagnostics),
            },
        )

    return LoadedSubprocessSession(
        launch_spec=launch_spec,
        adapter=adapter,
        sdk_bridge=bridge,
        order_intent_collector=order_collector,
        work_root=root,
    )


def load_strategy_from_cli(
    *,
    artifact_path: str,
    entrypoint: str,
    mode: str = "BACKTEST",
    work_root: Path | None = None,
) -> LoadedSubprocessSession:
    _ = mode
    entry = entrypoint.strip()
    if not entry:
        raise StrategyBootstrapError(
            "INIT_VALIDATION_FAILED",
            "entrypoint must not be empty.",
        )
    launch_payload: dict[str, object] = {
        "runtime_id": "cli-bootstrap",
        "tenant_id": "stdio-backtest",
        "strategy_version_id": "cli-bootstrap",
        "mode": WorkerMode.BACKTEST.value,
        "launch_attempt": 1,
        "artifact_uri": _artifact_path_to_uri(artifact_path),
        "entrypoint": entry,
        "account_id": "stdio-backtest-account",
        "job_id": "cli-bootstrap",
        "parameters": {},
        "ts_start": "1970-01-01T00:00:00Z",
        "ts_end": "1970-01-02T00:00:00Z",
    }
    return _load_strategy_from_launch_payload(launch_payload, work_root=work_root)


@dataclass(frozen=True, slots=True)
class SubprocessWiring:
    market_data_wire: MarketDataWireAdapter
    order_intent_wire: OrderIntentWireAdapter
    portfolio_wire: PortfolioWireAdapter


def build_subprocess_wiring(loaded: LoadedSubprocessSession) -> SubprocessWiring:
    return SubprocessWiring(
        market_data_wire=MarketDataWireAdapter(),
        order_intent_wire=OrderIntentWireAdapter(),
        portfolio_wire=PortfolioWireAdapter(),
    )


def build_runner_session(loaded: LoadedSubprocessSession) -> BacktestRunnerSession:
    wiring = build_subprocess_wiring(loaded)
    return BacktestRunnerSession.bootstrap_from_loaded(
        loaded,
        market_data_wire=wiring.market_data_wire,
        order_intent_wire=wiring.order_intent_wire,
        portfolio_sync=PortfolioSyncAdapter(loaded.sdk_bridge),
        portfolio_wire=wiring.portfolio_wire,
    )

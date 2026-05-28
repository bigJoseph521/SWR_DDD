"""Stdio backtest session state machine (backtest-runner subprocess)."""

from __future__ import annotations

from pathlib import Path

from runtime.application.backtest_runner.handle_market_data import (
    MarketDataDispatchError,
    dispatch_market_data_event,
)
from runtime.application.backtest_runner.loaded_session import LoadedSubprocessSession
from runtime.application.backtest_runner.messages import (
    InboundMessage,
    InitMessage,
    MarketDataEventMessage,
    NoOpMessage,
    OpenOrdersSnapshotMessage,
    OutboundMessage,
    PortfolioSnapshotMessage,
    StrategyErrorMessage,
)
from runtime.application.backtest_runner.ports import (
    MarketDataWirePort,
    OrderIntentWirePort,
    PortfolioSyncPort,
    PortfolioWirePort,
)
from runtime.application.strategy_execution.strategy_adapter import StrategyAdapter
from runtime.domain.backtest_runner.runtime_context import BacktestRuntimeContext
from runtime.domain.backtest_runner.session_state import SessionState


class BacktestRunnerSession:
    """
    Handles replay frames for one runner↔worker stdio session.

    Strategy load happens via CLI bootstrap or legacy ``INIT`` (tests).
    """

    __slots__ = (
        "_backtest_job_id",
        "_loaded",
        "_market_data_wire",
        "_order_intent_wire",
        "_portfolio_sync",
        "_portfolio_wire",
        "_runtime_ctx",
        "_state",
        "_strategy_adapter",
        "_work_root",
    )

    def __init__(
        self,
        *,
        market_data_wire: MarketDataWirePort,
        order_intent_wire: OrderIntentWirePort,
        portfolio_sync: PortfolioSyncPort,
        portfolio_wire: PortfolioWirePort,
        work_root: Path | None = None,
    ) -> None:
        self._state = SessionState.CREATED
        self._backtest_job_id: str | None = None
        self._work_root = work_root
        self._loaded: LoadedSubprocessSession | None = None
        self._strategy_adapter: StrategyAdapter | None = None
        self._runtime_ctx = BacktestRuntimeContext()
        self._market_data_wire = market_data_wire
        self._order_intent_wire = order_intent_wire
        self._portfolio_sync = portfolio_sync
        self._portfolio_wire = portfolio_wire

    @classmethod
    def bootstrap_from_loaded(
        cls,
        loaded: LoadedSubprocessSession,
        *,
        market_data_wire: MarketDataWirePort,
        order_intent_wire: OrderIntentWirePort,
        portfolio_sync: PortfolioSyncPort,
        portfolio_wire: PortfolioWirePort,
        backtest_job_id: str | None = None,
    ) -> BacktestRunnerSession:
        spec = loaded.launch_spec
        default_job = spec.job_id or spec.runtime_id or "cli-bootstrap"
        job_id = (backtest_job_id or default_job).strip()
        session = cls(
            market_data_wire=market_data_wire,
            order_intent_wire=order_intent_wire,
            portfolio_sync=portfolio_sync,
            portfolio_wire=portfolio_wire,
            work_root=loaded.work_root,
        )
        session._loaded = loaded
        session._strategy_adapter = loaded.adapter
        session._backtest_job_id = job_id
        session._runtime_ctx.backtest_job_id = job_id
        session._runtime_ctx.strategy_version_id = (
            loaded.launch_spec.strategy_version_id
        )
        session._state = SessionState.READY
        session._ensure_default_portfolio_snapshot()
        return session

    def _ensure_default_portfolio_snapshot(self) -> None:
        if self._runtime_ctx.portfolio_snapshot_received:
            return
        if self._loaded is None:
            self._runtime_ctx.replace_portfolio_snapshot(
                self._portfolio_wire.default_runner_portfolio_wire()
            )
            return
        wire = self._portfolio_wire.default_runner_portfolio_wire()
        try:
            self._portfolio_wire.validate_portfolio_wire(wire)
            self._portfolio_sync.apply_portfolio_snapshot(
                wire, launch_spec=self._loaded.launch_spec
            )
            self._runtime_ctx.replace_portfolio_snapshot(wire)
        except Exception:
            self._runtime_ctx.replace_portfolio_snapshot(wire)

    @property
    def state(self) -> SessionState:
        return self._state

    @property
    def backtest_job_id(self) -> str | None:
        return self._backtest_job_id

    @property
    def strategy_adapter(self) -> StrategyAdapter | None:
        return self._strategy_adapter

    @property
    def runtime_context(self) -> BacktestRuntimeContext:
        return self._runtime_ctx

    def handle(self, message: InboundMessage) -> OutboundMessage:
        if self._state is SessionState.FAILED:
            return self._error(
                code="SESSION_FAILED",
                message="session is already in FAILED state",
            )
        if self._state is SessionState.COMPLETED:
            return self._error(
                code="SESSION_COMPLETED",
                message="session is already COMPLETED",
            )

        if isinstance(message, InitMessage):
            return self._error(
                code="UNSUPPORTED_INBOUND_MESSAGE",
                message="INIT requires bootstrap subprocess wiring",
            )
        if isinstance(message, PortfolioSnapshotMessage):
            return self._handle_portfolio_snapshot(message)
        if isinstance(message, OpenOrdersSnapshotMessage):
            return self._handle_open_orders_snapshot(message)
        if isinstance(message, MarketDataEventMessage):
            return self._handle_market_data(message)

        return self._error(
            code="UNSUPPORTED_INBOUND_MESSAGE",
            message=f"unsupported inbound message: {type(message).__name__}",
        )

    def _require_session_ready(self, inbound_type: str) -> StrategyErrorMessage | None:
        if self._state is SessionState.CREATED:
            return self._fail(
                code="SESSION_NOT_READY",
                message=(
                    f"{inbound_type} requires strategy bootstrap "
                    "(session has not completed initialization)"
                ),
            )
        return None

    def _handle_portfolio_snapshot(
        self, message: PortfolioSnapshotMessage
    ) -> OutboundMessage:
        not_ready = self._require_session_ready(message.type)
        if not_ready is not None:
            return not_ready
        if self._loaded is None:
            return self._fail(
                code="SESSION_NOT_READY",
                message="PORTFOLIO_SNAPSHOT requires a loaded strategy session",
            )

        try:
            self._portfolio_wire.validate_portfolio_wire(message.portfolio)
            self._portfolio_sync.apply_portfolio_snapshot(
                message.portfolio,
                launch_spec=self._loaded.launch_spec,
            )
            self._runtime_ctx.replace_portfolio_snapshot(message.portfolio)
        except ValueError as exc:
            return self._fail(
                code="PORTFOLIO_SNAPSHOT_INVALID",
                message=str(exc),
            )
        except Exception as exc:
            return self._fail(
                code="PORTFOLIO_SNAPSHOT_INVALID",
                message=f"failed to apply portfolio snapshot: {exc}",
            )

        return NoOpMessage()

    def _handle_open_orders_snapshot(
        self, message: OpenOrdersSnapshotMessage
    ) -> OutboundMessage:
        not_ready = self._require_session_ready(message.type)
        if not_ready is not None:
            return not_ready
        self._ensure_default_portfolio_snapshot()
        try:
            self._portfolio_wire.validate_open_orders_wire(message.orders)
            self._runtime_ctx.replace_open_orders_snapshot(message.orders)
        except ValueError as exc:
            return self._fail(
                code="OPEN_ORDERS_SNAPSHOT_INVALID",
                message=str(exc),
            )
        return NoOpMessage()

    def _handle_market_data(self, message: MarketDataEventMessage) -> OutboundMessage:
        not_ready = self._require_session_ready("MARKET_DATA_EVENT")
        if not_ready is not None:
            return not_ready
        if not self._runtime_ctx.portfolio_snapshot_received:
            return self._fail(
                code="REQUIRED_CONTEXT_MISSING",
                message=(
                    "MARKET_DATA_EVENT requires PORTFOLIO_SNAPSHOT before replay frames"
                ),
            )
        if self._loaded is None or self._strategy_adapter is None:
            return self._fail(
                code="SESSION_NOT_READY",
                message="MARKET_DATA_EVENT requires a loaded strategy session",
            )
        if self._state is SessionState.READY:
            self._state = SessionState.RUNNING
        if self._state is not SessionState.RUNNING:
            return self._fail(
                code="INVALID_STATE",
                message=(
                    f"MARKET_DATA_EVENT is not accepted in state={self._state.value}"
                ),
            )
        try:
            return dispatch_market_data_event(
                adapter=self._strategy_adapter,
                launch_spec=self._loaded.launch_spec,
                collector=self._loaded.order_intent_collector,
                message=message,
                market_data_wire=self._market_data_wire,
                order_intent_wire=self._order_intent_wire,
            )
        except MarketDataDispatchError as exc:
            return self._fail(code=exc.code, message=exc.message)

    def _error(self, *, code: str, message: str) -> StrategyErrorMessage:
        return StrategyErrorMessage(code=code, message=message)

    def _fail(self, *, code: str, message: str) -> StrategyErrorMessage:
        self._state = SessionState.FAILED
        return self._error(code=code, message=message)

from __future__ import annotations

from typing import Any, Mapping

from runtime.application.strategy_execution.strategy_adapter import StrategyAdapter
from runtime.application.strategy_execution.strategy_error_boundary import StrategyCallResult
from runtime.domain.model.strategy_calculation_spec import StrategyCalculationSpec


class StrategyExecutionService:
    """
    Mode-agnostic strategy execution boundary.

    User strategy code is invoked only through :class:`StrategyAdapter`; this service
    does not know whether events originated from BACKTEST replay ingress, Redis, or elsewhere.
    """

    def __init__(
        self,
        *,
        adapter: StrategyAdapter,
        calculation_spec: StrategyCalculationSpec | None = None,
    ) -> None:
        self._adapter = adapter
        self._calculation_spec = calculation_spec

    @property
    def calculation_spec(self) -> StrategyCalculationSpec | None:
        return self._calculation_spec

    @property
    def adapter(self) -> StrategyAdapter:
        return self._adapter

    def bind_and_start(self) -> StrategyCallResult[Any]:
        return self._adapter.bind_and_start()

    def on_raw_event(self, raw_event: Mapping[str, Any]) -> StrategyCallResult[Any]:
        return self._adapter.on_event(raw_event)

    def stop(self) -> StrategyCallResult[Any]:
        return self._adapter.stop()

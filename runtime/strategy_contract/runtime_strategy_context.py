from __future__ import annotations

from typing import Any

from alphovex_sdk.context import StrategyContext as SdkStrategyContext
from alphovex_sdk.context.account_context import AccountContext
from alphovex_sdk.context.data_context import DataContext
from alphovex_sdk.context.indicator_context import IndicatorContext
from alphovex_sdk.context.logging_context import LoggingContext
from alphovex_sdk.context.orders_context import OrdersContext
from alphovex_sdk.context.params_context import ParamsContext
from alphovex_sdk.context.time_context import TimeContext
from runtime.strategy_contract.runtime_account_context import (
    RuntimeAccountContext,
)
from runtime.strategy_contract.runtime_data_context import (
    RuntimeDataContext,
)
from runtime.strategy_contract.runtime_indicator_context import (
    RuntimeIndicatorContext,
)
from runtime.strategy_contract.runtime_logging_context import (
    RuntimeLoggingContext,
)
from runtime.strategy_contract.runtime_orders_context import (
    RuntimeOrdersContext,
)
from runtime.strategy_contract.runtime_params_context import (
    RuntimeParamsContext,
)
from runtime.strategy_contract.runtime_time_context import (
    RuntimeTimeContext,
)


class RuntimeStrategyContext(SdkStrategyContext):
    """
    Replay/backtest implementation of :class:`alphovex_sdk.context.StrategyContext`.

    Exposes the SDK sub-context facades plus legacy attributes still used by some
    artifacts (``risk``, ``deployment``, ``clock``, ``log``, ``indicators``, …).
    """

    __slots__ = (
        "_account",
        "_data",
        "_orders",
        "_logging",
        "_params",
        "_time",
        "_indicator",
        "_risk",
        "_metrics",
        "_deployment",
        "_run_id",
        "_strategy_id",
        "_clock",
        "_log_backend",
        "_indicators_service",
    )

    def __init__(
        self,
        *,
        account: RuntimeAccountContext,
        data: RuntimeDataContext,
        orders: RuntimeOrdersContext,
        logging_facade: RuntimeLoggingContext,
        params: RuntimeParamsContext,
        time_ctx: RuntimeTimeContext,
        indicator: RuntimeIndicatorContext,
        risk: object,
        metrics: object,
        deployment: object,
        run_id: str,
        strategy_id: str,
        clock: object,
        log_backend: object,
        indicators_service: object,
    ) -> None:
        self._account = account
        self._data = data
        self._orders = orders
        self._logging = logging_facade
        self._params = params
        self._time = time_ctx
        self._indicator = indicator
        self._risk = risk
        self._metrics = metrics
        self._deployment = deployment
        self._run_id = run_id
        self._strategy_id = strategy_id
        self._clock = clock
        self._log_backend = log_backend
        self._indicators_service = indicators_service

    @property
    def account(self) -> AccountContext:
        return self._account

    @property
    def data(self) -> DataContext:
        return self._data

    @property
    def orders(self) -> OrdersContext:
        return self._orders

    @property
    def logging(self) -> LoggingContext:
        return self._logging

    @property
    def params(self) -> ParamsContext:
        return self._params

    @property
    def time(self) -> TimeContext:
        return self._time

    @property
    def indicator(self) -> IndicatorContext:
        return self._indicator

    @property
    def risk(self) -> Any:
        return self._risk

    @property
    def metrics(self) -> Any:
        return self._metrics

    @property
    def deployment(self) -> Any:
        return self._deployment

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def strategy_id(self) -> str:
        return self._strategy_id

    @property
    def clock(self) -> Any:
        return self._clock

    @property
    def log(self) -> Any:
        return self._log_backend

    @property
    def indicators(self) -> Any:
        return self._indicators_service

from __future__ import annotations

from alphovex_sdk.context.indicator_context import DataSourceEnum, IndicatorContext
from alphovex_sdk.indicators.base import Indicator
from alphovex_sdk.typedefs import Numeric


class RuntimeIndicatorContext(IndicatorContext):
    """Delegates to SDK :class:`IndicatorService` (or compatible)."""

    __slots__ = ("_svc",)

    def __init__(self, *, indicator_service: object) -> None:
        self._svc = indicator_service

    def register_indicator(
        self,
        indicator_name: str,
        indicator: Indicator,
        data_source: DataSourceEnum,
    ) -> None:
        fn = getattr(self._svc, "register_indicator", None)
        if not callable(fn):
            raise NotImplementedError("indicator backend has no register_indicator()")
        fn(indicator_name, indicator, data_source)

    def get_indicator_value(self, indicator_id: str) -> Numeric:
        fn = getattr(self._svc, "get_indicator_value", None)
        if not callable(fn):
            raise NotImplementedError("indicator backend has no get_indicator_value()")
        raw = fn(indicator_id)
        if raw is None:
            return 0.0
        return raw

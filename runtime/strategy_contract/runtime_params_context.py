from __future__ import annotations

from typing import Any

from alphovex_sdk.context.params_context import ParamsContext
from runtime.strategy_contract.sdk_runtime_types import ParameterSchema


class RuntimeParamsContext(ParamsContext):
    """Reads resolved parameter values from :class:`ParameterSchema`.

    For replay/backtest, ``ParameterSchema`` is populated from the strategy's optional
    ``build_parameter_schema`` hook and merged with ``properties`` (JSON Schema) or legacy
    ``indicator_params`` from ``{module_stem}.yaml`` next to the strategy module (see
    :func:`~runtime.strategy_contract.strategy_params_yaml.merge_parameter_schema_with_adjacent_params_yaml`).
    """

    __slots__ = ("_schema",)

    def __init__(self, *, schema: ParameterSchema) -> None:
        self._schema = schema

    def get(self, key: str) -> Any:
        spec = self._schema.parameters.get(key)
        if spec is None:
            raise KeyError(key)
        if isinstance(spec, dict):
            if "default" in spec:
                return spec["default"]
            if "value" in spec:
                return spec["value"]
            return None
        for attr in ("default", "value", "default_value"):
            if hasattr(spec, attr):
                val = getattr(spec, attr)
                if val is not None:
                    return val
        return None

    def get_params(self, *keys: str) -> tuple[Any, ...]:
        return tuple(self.get(k) for k in keys)

from __future__ import annotations

from runtime.infrastructure.sdk.runtime_sdk_bridge import (
    merge_parameter_schema_with_strategy_overrides,
    strategy_parameter_overrides_from_launch_payload,
)
from runtime.infrastructure.sdk.sdk_runtime_types import ParameterSchema


def test_overrides_prefer_top_level_strategy_params() -> None:
    lp = {
        "strategy_params": {"a": 1},
        "parameters": {"a": 99, "symbol": "SPY"},
    }
    o = strategy_parameter_overrides_from_launch_payload(lp)
    assert o == {"a": 1}


def test_overrides_from_parameters_excludes_runtime_keys() -> None:
    lp = {
        "parameters": {
            "lookback": 20,
            "symbol": "IWM",
            "bar_timeframe": "1m",
            "data_source": ["bars"],
            "instrument_id": "inst_x",
        }
    }
    o = strategy_parameter_overrides_from_launch_payload(lp)
    assert o == {"lookback": 20}


def test_merge_overrides_existing_spec_default() -> None:
    schema = ParameterSchema(parameters={"lookback": {"type": "int", "default": 8}})
    merged = merge_parameter_schema_with_strategy_overrides(schema, {"lookback": 20})
    assert merged.parameters["lookback"]["default"] == 20
    assert merged.parameters["lookback"]["type"] == "int"


def test_merge_adds_missing_key_as_default_only() -> None:
    schema = ParameterSchema(parameters={})
    merged = merge_parameter_schema_with_strategy_overrides(schema, {"lookback": 20})
    assert merged.parameters["lookback"] == {"default": 20}

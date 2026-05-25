from __future__ import annotations

import importlib.util
import sys
import uuid
from pathlib import Path

import pytest

pytest.importorskip("yaml")

from runtime.infrastructure.sdk.sdk_runtime_types import ParameterSchema
from runtime.infrastructure.sdk.strategy_params_yaml import (
    discover_adjacent_params_yaml,
    discover_params_yaml_adjacent_to_module_file,
    load_indicator_params_from_params_yaml,
    load_warmup_bars_default_for_strategy,
    load_warmup_bars_default_from_params_yaml,
    merge_parameter_schema_with_adjacent_params_yaml,
    params_yaml_filename_for_module,
)


def test_load_warmup_bars_prefers_warmup_bars_over_lookback_days(
    tmp_path: Path,
) -> None:
    yaml = tmp_path / "params.yaml"
    yaml.write_text(
        """
warmup_params:
  warmup_bars:
    type: int
    default: 42
  warmup_lookback_days:
    type: int
    default: 9
""",
        encoding="utf-8",
    )
    assert load_warmup_bars_default_from_params_yaml(yaml) == 42


def test_load_warmup_bars_fallback_to_lookback_days(tmp_path: Path) -> None:
    yaml = tmp_path / "params.yaml"
    yaml.write_text(
        """
warmup_params:
  warmup_lookback_days:
    type: int
    default: 11
""",
        encoding="utf-8",
    )
    assert load_warmup_bars_default_from_params_yaml(yaml) == 11


def test_load_warmup_bars_for_strategy_from_adjacent_yaml(tmp_path: Path) -> None:
    cls, mod_name = _load_class_from_temp_module(
        tmp_path,
        py_body="class S:\n    pass\n",
        yaml_body=("warmup_params:\n  warmup_bars:\n    type: int\n    default: 7\n"),
    )
    try:
        assert load_warmup_bars_default_for_strategy(cls()) == 7
    finally:
        sys.modules.pop(mod_name, None)


def test_load_indicator_params_from_params_yaml_properties(tmp_path: Path) -> None:
    yaml = tmp_path / "params.yaml"
    yaml.write_text(
        """
type: object
properties:
  fast_period:
    type: int
    default: 10
  slow_period:
    type: int
    default: 30
""",
        encoding="utf-8",
    )
    specs = load_indicator_params_from_params_yaml(yaml)
    assert specs["fast_period"]["default"] == 10
    assert specs["slow_period"]["default"] == 30


def test_load_indicator_params_prefers_properties_over_indicator_params(
    tmp_path: Path,
) -> None:
    yaml = tmp_path / "params.yaml"
    yaml.write_text(
        """
properties:
  a:
    default: from_properties
indicator_params:
  a:
    default: from_indicator
  b:
    default: 2
""",
        encoding="utf-8",
    )
    specs = load_indicator_params_from_params_yaml(yaml)
    assert specs["a"]["default"] == "from_properties"
    assert "b" not in specs


def test_load_indicator_params_fallback_when_properties_empty(tmp_path: Path) -> None:
    yaml = tmp_path / "params.yaml"
    yaml.write_text(
        """
properties: {}
indicator_params:
  x:
    default: 1
""",
        encoding="utf-8",
    )
    specs = load_indicator_params_from_params_yaml(yaml)
    assert specs["x"]["default"] == 1


def test_load_indicator_params_from_params_yaml_legacy_indicator_params(
    tmp_path: Path,
) -> None:
    yaml = tmp_path / "params.yaml"
    yaml.write_text(
        """
indicator_params:
  fast_period:
    type: int
    default: 7
  slow_period:
    type: int
    default: 21
""",
        encoding="utf-8",
    )
    specs = load_indicator_params_from_params_yaml(yaml)
    assert specs["fast_period"]["default"] == 7
    assert specs["slow_period"]["default"] == 21


def _load_class_from_temp_module(
    tmp_path: Path, py_body: str, yaml_body: str
) -> tuple[type, str]:
    strat_py = tmp_path / "temp_strategy.py"
    strat_py.write_text(py_body, encoding="utf-8")
    (tmp_path / params_yaml_filename_for_module(strat_py)).write_text(
        yaml_body, encoding="utf-8"
    )
    mod_name = f"_swr_temp_strategy_{uuid.uuid4().hex}"
    spec = importlib.util.spec_from_file_location(mod_name, strat_py)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)
    return mod.S, mod_name  # type: ignore[attr-defined]


def test_merge_adjacent_params_yaml_and_class_overrides(tmp_path: Path) -> None:
    cls, mod_name = _load_class_from_temp_module(
        tmp_path,
        py_body="class S:\n    pass\n",
        yaml_body=("properties:\n  a:\n    default: 1\n  b:\n    default: 2\n"),
    )
    try:
        instance = cls()
        assert discover_adjacent_params_yaml(instance) is not None

        merged = merge_parameter_schema_with_adjacent_params_yaml(
            instance, ParameterSchema(parameters={})
        )
        assert merged.parameters["a"]["default"] == 1
        assert merged.parameters["b"]["default"] == 2

        override = ParameterSchema(parameters={"a": {"type": "int", "default": 99}})
        merged2 = merge_parameter_schema_with_adjacent_params_yaml(instance, override)
        assert merged2.parameters["a"]["default"] == 99
        assert merged2.parameters["b"]["default"] == 2
    finally:
        sys.modules.pop(mod_name, None)


def test_discover_adjacent_params_yaml_none_when_missing() -> None:
    class X:
        pass

    assert discover_adjacent_params_yaml(X()) is None


def test_discover_prefers_stem_yaml_over_legacy_params_yaml(tmp_path: Path) -> None:
    strat_py = tmp_path / "sma_crossover.py"
    strat_py.write_text("class S:\n    pass\n", encoding="utf-8")
    (tmp_path / "sma_crossover.yaml").write_text(
        "properties:\n  fast_period:\n    default: 10\n", encoding="utf-8"
    )
    (tmp_path / "params.yaml").write_text(
        "properties:\n  fast_period:\n    default: 99\n", encoding="utf-8"
    )
    mod_name = f"_swr_temp_strategy_{uuid.uuid4().hex}"
    spec = importlib.util.spec_from_file_location(mod_name, strat_py)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)
    try:
        path = discover_params_yaml_adjacent_to_module_file(strat_py)
        assert path is not None
        assert path.name == "sma_crossover.yaml"
        merged = merge_parameter_schema_with_adjacent_params_yaml(
            mod.S(), ParameterSchema(parameters={})
        )
        assert merged.parameters["fast_period"]["default"] == 10
    finally:
        sys.modules.pop(mod_name, None)

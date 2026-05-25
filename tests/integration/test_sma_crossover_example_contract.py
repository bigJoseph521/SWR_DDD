from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
from runtime.bootstrap.entrypoint_loader import EntrypointLoadResult
from runtime.bootstrap.sdk_contract_validator import (
    SdkContractValidator,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _sma_crossover_example_path() -> Path:
    return (
        _repo_root()
        / "alphovex_sdk"
        / "strategy"
        / "examples"
        / "sma_crossover"
        / "sma_crossover.py"
    )


def _load_sma_crossover_module() -> object:
    path = _sma_crossover_example_path()
    if not path.is_file():
        pytest.skip(f"SMA crossover example not present at {path}")
    spec = importlib.util.spec_from_file_location(
        "sma_crossover_example_under_test",
        path,
    )
    if spec is None or spec.loader is None:
        pytest.skip("importlib could not build spec for sma_crossover example")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_sma_crossover_example_passes_default_sdk_validator_when_loaded_by_path() -> (
    None
):
    """
    Load ``alphovex_sdk/strategy/examples/sma_crossover/sma_crossover.py`` by file path
    (no edits under ``alphovex_sdk``) and assert the default SDK contract validator accepts it.
    """
    mod = _load_sma_crossover_module()
    strategy_cls = getattr(mod, "SMACrossOver", None)
    if strategy_cls is None:
        pytest.fail("loaded module missing SMACrossOver class")

    result = SdkContractValidator().validate(
        EntrypointLoadResult(
            entrypoint_spec="examples.sma_crossover.sma_crossover:SMACrossOver",
            module_name="sma_crossover_example_under_test",
            symbol_name="SMACrossOver",
            symbol=strategy_cls,
            details={},
        )
    )
    assert result.is_valid is True
    assert result.validated_type == "SMACrossOver"

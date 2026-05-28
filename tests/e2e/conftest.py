"""E2E fixtures: bootstrap tests validate lifecycle wiring, not mypy on fixture strategies."""

from __future__ import annotations

from typing import Any

import pytest

from runtime.bootstrap.sdk_contract_validator import SdkContractValidator
from runtime.infrastructure.strategy_loader.entrypoint_loader import (
    EntrypointLoadResult,
)


def _mypy_passes(
    self: SdkContractValidator, *, entrypoint: EntrypointLoadResult
) -> dict[str, Any]:
    return {
        "entrypoint": entrypoint.entrypoint_spec,
        "mypy_exit_code": 0,
        "mypy_output": "",
        "mypy_result_path": str(self._work_root / "mypy_result.txt"),
    }


@pytest.fixture(autouse=True)
def _stub_mypy_for_bootstrap_e2e(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(SdkContractValidator, "_collect_mypy_validation", _mypy_passes)

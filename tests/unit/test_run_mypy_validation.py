from __future__ import annotations

from pathlib import Path

from runtime.infrastructure.strategy_loader.run_mypy_validation import (
    mypy_result_path_for,
)


def test_mypy_result_path_for_uses_work_root(tmp_path: Path) -> None:
    work_root = tmp_path / "dep-abc-123"
    assert mypy_result_path_for(work_root=work_root) == work_root / "mypy_result.txt"

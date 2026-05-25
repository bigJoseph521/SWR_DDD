"""
Test import paths: the ``runtime`` package lives under ``runtime/`` at repo root;
``alphovex_sdk`` may be vendored at the repository root. Prepend repo root before collection.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_root = Path(__file__).resolve().parents[1]
_s = str(_root)
if _s not in sys.path:
    sys.path.insert(0, _s)

# ``runtime_backtest`` is not shipped; skip its tests until the package is added back.
collect_ignore = [
    "unit/test_runtime_backtest_mode_guard.py",
    "unit/test_runtime_backtest_stdio_host.py",
    "unit/test_runtime_backtest_stdio_market_data.py",
    "unit/test_runtime_backtest_stdio_protocol.py",
    "unit/test_runtime_backtest_stdio_snapshots.py",
    "unit/test_runtime_backtest_stdio_strategy_init.py",
    "integration/test_runtime_backtest_stdio_subprocess.py",
]


@pytest.fixture(autouse=True)
def _use_env_connectivity_in_tests(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWR_USE_MODULE_CONNECTIVITY_SETTINGS", "0")

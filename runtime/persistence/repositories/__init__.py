from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any


def _load_impl_module() -> ModuleType:
    module_path = Path(__file__).resolve().parents[1] / "repositories.py"
    spec = importlib.util.spec_from_file_location(
        "_runtime_persistence_repositories_impl",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(
            f"Unable to load repository implementation from {module_path}"
        )
    module = importlib.util.module_from_spec(spec)
    module_name = spec.name
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    return module


_impl: Any = _load_impl_module()

WorkerInstanceRecord = _impl.WorkerInstanceRecord
LaunchAttemptRecord = _impl.LaunchAttemptRecord
HeartbeatObservationRecord = _impl.HeartbeatObservationRecord
WorkerEventRecord = _impl.WorkerEventRecord
DiagnosticRecord = _impl.DiagnosticRecord
RuntimeStateJournalRecord = _impl.RuntimeStateJournalRecord
OrderIntentJournalRecord = _impl.OrderIntentJournalRecord

WorkerInstanceRepository = _impl.WorkerInstanceRepository
LaunchAttemptRepository = _impl.LaunchAttemptRepository
HeartbeatRepository = _impl.HeartbeatRepository
WorkerEventRepository = _impl.WorkerEventRepository
DiagnosticRepository = _impl.DiagnosticRepository

SQLiteWorkerInstanceRepository = _impl.SQLiteWorkerInstanceRepository
SQLiteLaunchAttemptRepository = _impl.SQLiteLaunchAttemptRepository
SQLiteHeartbeatRepository = _impl.SQLiteHeartbeatRepository
SQLiteWorkerEventRepository = _impl.SQLiteWorkerEventRepository
SQLiteDiagnosticRepository = _impl.SQLiteDiagnosticRepository
SQLiteRuntimeStateJournalRepository = _impl.SQLiteRuntimeStateJournalRepository
SQLiteOrderIntentJournalRepository = _impl.SQLiteOrderIntentJournalRepository

__all__ = [
    "DiagnosticRecord",
    "DiagnosticRepository",
    "HeartbeatObservationRecord",
    "HeartbeatRepository",
    "LaunchAttemptRecord",
    "LaunchAttemptRepository",
    "OrderIntentJournalRecord",
    "RuntimeStateJournalRecord",
    "SQLiteDiagnosticRepository",
    "SQLiteHeartbeatRepository",
    "SQLiteLaunchAttemptRepository",
    "SQLiteOrderIntentJournalRepository",
    "SQLiteRuntimeStateJournalRepository",
    "SQLiteWorkerEventRepository",
    "SQLiteWorkerInstanceRepository",
    "WorkerEventRecord",
    "WorkerEventRepository",
    "WorkerInstanceRecord",
    "WorkerInstanceRepository",
]

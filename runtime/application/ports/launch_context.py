from __future__ import annotations

from typing import Protocol

from runtime.domain.enums import WorkerMode


class LaunchContext(Protocol):
    """Launch metadata used by application lifecycle orchestration."""

    runtime_id: str
    tenant_id: str
    strategy_version_id: str
    mode: WorkerMode
    launch_attempt: int
    trader_id: str | None
    account_id: str | None
    job_id: str | None
    ts_start: str | None
    ts_end: str | None
    symbol: str | None

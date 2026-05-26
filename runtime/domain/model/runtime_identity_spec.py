from __future__ import annotations

from dataclasses import dataclass

from runtime.domain.enums import WorkerMode


@dataclass(frozen=True, slots=True)
class RuntimeIdentitySpec:
    """Runtime lifecycle and service integration identity."""

    runtime_id: str
    mode: WorkerMode
    deployment_id: str | None
    backtest_job_id: str | None
    account_id: str | None
    portfolio_id: str | None

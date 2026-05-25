from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from runtime.domain.enums import RuntimeMode
from runtime.domain.errors import InvalidLaunchContextError
from runtime.domain.worker_identity import WorkerIdentity


def _require_non_empty(value: str, field_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise InvalidLaunchContextError(
            field_name=field_name, reason="must_not_be_blank"
        )
    return normalized


def _normalize_optional(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        raise InvalidLaunchContextError(
            field_name=field_name,
            reason="must_not_be_blank_if_provided",
        )
    return normalized


@dataclass(frozen=True, slots=True)
class LaunchContext:
    runtime_id: str
    tenant_id: str
    strategy_version_id: str
    mode: RuntimeMode
    artifact_uri: str
    entrypoint: str
    launch_attempt: int
    parameter_hash: str | None = None
    artifact_reference: str | None = None
    trader_id: str | None = None
    account_id: str | None = None
    artifact_digest: str | None = None
    job_id: str | None = None
    ts_start: str | None = None
    ts_end: str | None = None
    correlation_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "runtime_id", _require_non_empty(self.runtime_id, "runtime_id")
        )
        # tenant_id is optional at the manager; user-scoped runs use "" (matches SQL NULL).
        object.__setattr__(self, "tenant_id", self.tenant_id.strip())
        object.__setattr__(
            self,
            "strategy_version_id",
            _require_non_empty(self.strategy_version_id, "strategy_version_id"),
        )
        object.__setattr__(
            self,
            "artifact_uri",
            _require_non_empty(self.artifact_uri, "artifact_uri"),
        )
        object.__setattr__(
            self,
            "parameter_hash",
            _normalize_optional(self.parameter_hash, "parameter_hash"),
        )
        object.__setattr__(
            self,
            "artifact_reference",
            _normalize_optional(self.artifact_reference, "artifact_reference"),
        )
        if self.artifact_reference is None:
            object.__setattr__(self, "artifact_reference", self.artifact_uri)
        object.__setattr__(
            self, "entrypoint", _require_non_empty(self.entrypoint, "entrypoint")
        )

        optionals = (
            "trader_id",
            "account_id",
            "artifact_digest",
            "job_id",
            "ts_start",
            "ts_end",
            "correlation_id",
        )
        for field_name in optionals:
            value = getattr(self, field_name)
            object.__setattr__(self, field_name, _normalize_optional(value, field_name))

        mode = self.mode
        if isinstance(mode, str):
            try:
                mode = RuntimeMode(mode)
            except ValueError as exc:
                raise InvalidLaunchContextError(
                    field_name="mode",
                    reason=f"invalid_mode:{self.mode}",
                ) from exc
            object.__setattr__(self, "mode", mode)

        if self.launch_attempt < 1:
            raise InvalidLaunchContextError(
                field_name="launch_attempt",
                reason="must_be_greater_or_equal_to_1",
            )

        has_trader = self.trader_id is not None
        has_account = self.account_id is not None
        if has_trader == has_account:
            raise InvalidLaunchContextError(
                field_name="scope",
                reason="exactly_one_of_trader_id_or_account_id_required",
            )

        if self.mode is not RuntimeMode.BACKTEST and self.job_id is not None:
            raise InvalidLaunchContextError(
                field_name="job_id",
                reason="only_allowed_for_backtest_mode",
            )
        if self.mode is not RuntimeMode.BACKTEST and (
            self.ts_start is not None or self.ts_end is not None
        ):
            raise InvalidLaunchContextError(
                field_name="parameters",
                reason="ts_start_ts_end_only_allowed_for_backtest_mode",
            )
        if self.mode is RuntimeMode.BACKTEST and (
            self.ts_start is None or self.ts_end is None
        ):
            raise InvalidLaunchContextError(
                field_name="parameters",
                reason="ts_start_and_ts_end_required_for_backtest_mode",
            )

    def to_worker_identity(self) -> WorkerIdentity:
        return WorkerIdentity(
            runtime_id=self.runtime_id,
            tenant_id=self.tenant_id,
            strategy_version_id=self.strategy_version_id,
            mode=self.mode,
            trader_id=self.trader_id,
            account_id=self.account_id,
            artifact_uri=self.artifact_uri,
            validated_parameter_identity=self.parameter_hash,
            artifact_reference=self.artifact_reference,
            artifact_digest=self.artifact_digest,
            entrypoint=self.entrypoint,
            launch_attempt=self.launch_attempt,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "runtime_id": self.runtime_id,
            "tenant_id": self.tenant_id,
            "trader_id": self.trader_id,
            "account_id": self.account_id,
            "strategy_version_id": self.strategy_version_id,
            "mode": self.mode.value,
            "artifact_uri": self.artifact_uri,
            "artifact_reference": self.artifact_reference,
            "parameter_hash": self.parameter_hash,
            "artifact_digest": self.artifact_digest,
            "entrypoint": self.entrypoint,
            "launch_attempt": self.launch_attempt,
            "job_id": self.job_id,
            "ts_start": self.ts_start,
            "ts_end": self.ts_end,
            "correlation_id": self.correlation_id,
        }

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from runtime.domain.enums import WorkerMode
from runtime.domain.errors import (
    MalformedWorkerIdentityError,
    SingleAssignmentViolationError,
)


def _require_non_empty(value: str, field_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise MalformedWorkerIdentityError(
            field_name=field_name, reason="must_not_be_blank"
        )
    return normalized


def _normalize_optional(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        raise MalformedWorkerIdentityError(
            field_name=field_name,
            reason="must_not_be_blank_if_provided",
        )
    return normalized


@dataclass(frozen=True, slots=True)
class WorkerIdentity:
    runtime_id: str
    tenant_id: str
    strategy_version_id: str
    mode: WorkerMode
    trader_id: str | None = None
    account_id: str | None = None
    validated_parameter_identity: str | None = None
    artifact_reference: str | None = None
    artifact_uri: str = ""
    artifact_digest: str | None = None
    entrypoint: str = ""
    launch_attempt: int = 1
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
        uri_candidate = (self.artifact_uri or "").strip()
        if not uri_candidate and self.artifact_reference:
            ref_cand = (self.artifact_reference or "").strip()
            if ref_cand:
                uri_candidate = ref_cand
        object.__setattr__(
            self,
            "artifact_uri",
            _require_non_empty(uri_candidate, "artifact_uri"),
        )
        object.__setattr__(
            self, "entrypoint", _require_non_empty(self.entrypoint, "entrypoint")
        )

        object.__setattr__(
            self, "trader_id", _normalize_optional(self.trader_id, "trader_id")
        )
        object.__setattr__(
            self, "account_id", _normalize_optional(self.account_id, "account_id")
        )
        object.__setattr__(
            self,
            "validated_parameter_identity",
            _normalize_optional(
                self.validated_parameter_identity, "validated_parameter_identity"
            ),
        )
        object.__setattr__(
            self,
            "correlation_id",
            _normalize_optional(self.correlation_id, "correlation_id"),
        )
        object.__setattr__(
            self,
            "artifact_reference",
            _normalize_optional(self.artifact_reference, "artifact_reference"),
        )
        object.__setattr__(
            self,
            "artifact_digest",
            _normalize_optional(self.artifact_digest, "artifact_digest"),
        )

        mode = self.mode
        if isinstance(mode, str):
            try:
                mode = WorkerMode(mode)
            except ValueError as exc:
                raise MalformedWorkerIdentityError(
                    field_name="mode",
                    reason=f"invalid_mode:{self.mode}",
                ) from exc
            object.__setattr__(self, "mode", mode)

        has_trader = self.trader_id is not None
        has_account = self.account_id is not None
        if has_trader == has_account:
            raise SingleAssignmentViolationError(
                reason="exactly_one_of_trader_id_or_account_id_required"
            )

        if self.launch_attempt < 1:
            raise MalformedWorkerIdentityError(
                field_name="launch_attempt",
                reason="must_be_greater_or_equal_to_1",
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "runtime_id": self.runtime_id,
            "tenant_id": self.tenant_id,
            "strategy_version_id": self.strategy_version_id,
            "mode": self.mode.value,
            "trader_id": self.trader_id,
            "account_id": self.account_id,
            "validated_parameter_identity": self.validated_parameter_identity,
            "artifact_reference": self.artifact_reference,
            "artifact_uri": self.artifact_uri,
            "artifact_digest": self.artifact_digest,
            "entrypoint": self.entrypoint,
            "launch_attempt": self.launch_attempt,
            "correlation_id": self.correlation_id,
        }

    @property
    def parameter_hash(self) -> str | None:
        # Compatibility alias for legacy naming.
        return self.validated_parameter_identity

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Mapping


class BootstrapStage(StrEnum):
    ARTIFACT_FETCH = "artifact_fetch"
    ARTIFACT_VERIFY = "artifact_verify"
    ENTRYPOINT_LOAD = "entrypoint_load"
    SDK_VALIDATE = "sdk_validate"


@dataclass(slots=True)
class BootstrapFailure(Exception):
    stage: BootstrapStage
    reason_code: str
    retryable: bool
    details: Mapping[str, Any] = field(default_factory=dict)
    message: str = "Bootstrap stage failed."

    def __post_init__(self) -> None:
        rc = str(self.reason_code).strip().upper()
        if not rc:
            raise ValueError("reason_code must not be blank")
        object.__setattr__(self, "reason_code", rc)
        Exception.__init__(self, self.message)


class ArtifactFetchFailure(BootstrapFailure):
    def __init__(
        self,
        *,
        reason_code: str,
        retryable: bool,
        details: Mapping[str, Any] | None = None,
        message: str = "Artifact fetch failed.",
    ) -> None:
        super().__init__(
            stage=BootstrapStage.ARTIFACT_FETCH,
            reason_code=reason_code,
            retryable=retryable,
            details=dict(details or {}),
            message=message,
        )


class ArtifactVerificationFailure(BootstrapFailure):
    def __init__(
        self,
        *,
        reason_code: str,
        retryable: bool,
        details: Mapping[str, Any] | None = None,
        message: str = "Artifact verification failed.",
    ) -> None:
        super().__init__(
            stage=BootstrapStage.ARTIFACT_VERIFY,
            reason_code=reason_code,
            retryable=retryable,
            details=dict(details or {}),
            message=message,
        )


class EntrypointLoadFailure(BootstrapFailure):
    def __init__(
        self,
        *,
        reason_code: str,
        retryable: bool,
        details: Mapping[str, Any] | None = None,
        message: str = "Entrypoint load failed.",
    ) -> None:
        super().__init__(
            stage=BootstrapStage.ENTRYPOINT_LOAD,
            reason_code=reason_code,
            retryable=retryable,
            details=dict(details or {}),
            message=message,
        )


class SDKContractFailure(BootstrapFailure):
    def __init__(
        self,
        *,
        reason_code: str,
        retryable: bool,
        details: Mapping[str, Any] | None = None,
        message: str = "SDK contract validation failed.",
    ) -> None:
        super().__init__(
            stage=BootstrapStage.SDK_VALIDATE,
            reason_code=reason_code,
            retryable=retryable,
            details=dict(details or {}),
            message=message,
        )

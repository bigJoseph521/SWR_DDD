"""Re-export digest policy from domain (infrastructure compatibility shim)."""

from runtime.domain.artifact_digest_policy import (
    SKIP_ARTIFACT_DIGEST_VALIDATION_ENV,
    skip_artifact_digest_validation,
)

__all__ = ["SKIP_ARTIFACT_DIGEST_VALIDATION_ENV", "skip_artifact_digest_validation"]

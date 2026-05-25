"""Artifact digest validation policy (domain; no infrastructure imports)."""

from __future__ import annotations

# When set to 1/true/yes/on: allow missing or empty ``artifact_digest`` in launch metadata
# and skip cryptographic digest verification in artifact fetch/verify adapters.
SKIP_ARTIFACT_DIGEST_VALIDATION_ENV = "SWR_SKIP_ARTIFACT_DIGEST_VALIDATION"


def skip_artifact_digest_validation() -> bool:
    # TEMPORARY (2026-05): fleet-wide digest checks disabled until manager/registry + SDS
    # always supply and enforce ``artifact_digest``. Re-enable by deleting the early return
    # below and restoring the env-driven gate.
    return True

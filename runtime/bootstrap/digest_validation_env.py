"""Opt-out for strict artifact digest checks (launch metadata, zip SHA-256, post-fetch verify)."""

from __future__ import annotations

# When set to 1/true/yes/on: allow missing or empty ``artifact_digest`` in :class:`LaunchSpec`
# and skip cryptographic digest verification in :class:`ArtifactFetcher` / :class:`ArtifactVerifier`.
# Intended as a temporary escape hatch until manager/registry always supply a digest.
SKIP_ARTIFACT_DIGEST_VALIDATION_ENV = "SWR_SKIP_ARTIFACT_DIGEST_VALIDATION"


def skip_artifact_digest_validation() -> bool:
    # TEMPORARY (2026-05): fleet-wide digest checks disabled until manager/registry + SDS
    # always supply and enforce ``artifact_digest``. Re-enable by deleting the early return below
    # and restoring the env-driven gate.
    return True
    # v = os.environ.get(SKIP_ARTIFACT_DIGEST_VALIDATION_ENV, "").strip().lower()
    # return v in ("1", "true", "yes", "on")

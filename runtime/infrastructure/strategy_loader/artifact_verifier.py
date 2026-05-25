from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from runtime.infrastructure.strategy_loader.artifact_fetcher import ArtifactFetchResult
from runtime.infrastructure.strategy_loader.digest_validation_env import skip_artifact_digest_validation
from runtime.domain.bootstrap_failures import ArtifactVerificationFailure


@dataclass(frozen=True, slots=True)
class ArtifactVerificationResult:
    verified: bool
    algorithm: str | None
    expected_digest: str | None
    actual_digest: str | None
    details: Mapping[str, Any] = field(default_factory=dict)


class ArtifactVerifier:
    def verify(self, fetch_result: ArtifactFetchResult) -> ArtifactVerificationResult:
        if skip_artifact_digest_validation():
            return ArtifactVerificationResult(
                verified=True,
                algorithm=None,
                expected_digest=fetch_result.expected_digest,
                actual_digest=None,
                details={
                    "artifact_reference": fetch_result.artifact_reference,
                    "digest_validation_skipped": True,
                },
            )

        expected_digest = fetch_result.expected_digest
        if expected_digest is None:
            raise ArtifactVerificationFailure(
                reason_code="expected_digest_missing",
                message="Expected artifact digest is required for verification.",
                retryable=False,
                details={"artifact_reference": fetch_result.artifact_reference},
            )

        if fetch_result.preverified_digest:
            algorithm, expected_hex = self._parse_digest(expected_digest)
            return ArtifactVerificationResult(
                verified=True,
                algorithm=algorithm,
                expected_digest=expected_digest,
                actual_digest=expected_digest,
                details={
                    "artifact_reference": fetch_result.artifact_reference,
                    "preverified": True,
                },
            )

        algorithm, expected_hex = self._parse_digest(expected_digest)
        actual_hex = self._compute_digest(
            root=fetch_result.materialized_root,
            algorithm=algorithm,
            relative_paths=fetch_result.digest_relative_paths,
        )
        actual_digest = f"{algorithm}:{actual_hex}"

        if expected_hex != actual_hex:
            raise ArtifactVerificationFailure(
                reason_code="digest_mismatch",
                message="Artifact digest verification failed.",
                retryable=False,
                details={
                    "artifact_reference": fetch_result.artifact_reference,
                    "expected_digest": expected_digest,
                    "actual_digest": actual_digest,
                    "algorithm": algorithm,
                },
            )

        return ArtifactVerificationResult(
            verified=True,
            algorithm=algorithm,
            expected_digest=expected_digest,
            actual_digest=actual_digest,
            details={"artifact_reference": fetch_result.artifact_reference},
        )

    def _parse_digest(self, digest: str) -> tuple[str, str]:
        if digest.count(":") != 1:
            self._unsupported_digest_failure(
                digest=digest,
                reason="expected_format_algorithm_colon_hex",
            )
        algorithm, encoded = digest.split(":", 1)
        normalized_algorithm = algorithm.strip().lower()
        normalized_hex = encoded.strip().lower()
        if not normalized_algorithm or not normalized_hex:
            self._unsupported_digest_failure(
                digest=digest,
                reason="algorithm_or_hex_missing",
            )
        if any(char not in "0123456789abcdef" for char in normalized_hex):
            self._unsupported_digest_failure(
                digest=digest,
                reason="digest_hex_not_lowercase_hexadecimal",
            )
        try:
            hashlib.new(normalized_algorithm)
        except ValueError:
            self._unsupported_digest_failure(
                digest=digest,
                reason="unsupported_hash_algorithm",
                algorithm=normalized_algorithm,
            )
        return normalized_algorithm, normalized_hex

    def _compute_digest(
        self,
        *,
        root: Path,
        algorithm: str,
        relative_paths: frozenset[str] | None = None,
    ) -> str:
        hasher = hashlib.new(algorithm)
        if root.is_file():
            self._hash_file(hasher=hasher, path=root)
            return hasher.hexdigest()

        if not root.exists():
            raise ArtifactVerificationFailure(
                reason_code="artifact_materialization_missing",
                message="Materialized artifact path is missing.",
                retryable=False,
                details={"materialized_root": str(root)},
            )

        # Scoped paths (single-file artifact): hash each ``rel`` in sorted order — typically
        # the entrypoint module and ``params.yaml`` when both are part of the signed blob.
        if relative_paths is not None:
            for rel in sorted(relative_paths):
                path = root / rel
                if not path.is_file():
                    raise ArtifactVerificationFailure(
                        reason_code="artifact_materialization_missing",
                        message="Materialized artifact file missing for digest scope.",
                        retryable=False,
                        details={"materialized_root": str(root), "relative_path": rel},
                    )
                rel_b = Path(rel).as_posix().encode("utf-8")
                hasher.update(rel_b)
                hasher.update(b"\x00")
                self._hash_file(hasher=hasher, path=path)
            return hasher.hexdigest()

        # Include relative file path and content bytes in sorted order.
        for file_path in sorted(
            (item for item in root.rglob("*") if item.is_file()), key=lambda p: str(p)
        ):
            rel_bytes = file_path.relative_to(root).as_posix().encode("utf-8")
            hasher.update(rel_bytes)
            hasher.update(b"\x00")
            self._hash_file(hasher=hasher, path=file_path)
        return hasher.hexdigest()

    def _hash_file(self, *, hasher: Any, path: Path) -> None:
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(64 * 1024)
                if not chunk:
                    break
                hasher.update(chunk)

    def _unsupported_digest_failure(
        self,
        *,
        digest: str,
        reason: str,
        algorithm: str | None = None,
    ) -> None:
        raise ArtifactVerificationFailure(
            reason_code="digest_unsupported",
            message="Digest format or algorithm is not supported.",
            retryable=False,
            details={
                "expected_digest": digest,
                "reason": reason,
                "algorithm": algorithm,
            },
        )

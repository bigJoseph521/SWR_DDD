from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
from runtime.infrastructure.strategy_loader.artifact_fetcher import ArtifactFetchResult
from runtime.infrastructure.strategy_loader.artifact_verifier import ArtifactVerifier
from runtime.infrastructure.strategy_loader.digest_validation_env import (
    SKIP_ARTIFACT_DIGEST_VALIDATION_ENV,
)


def _write_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _compute_digest(root: Path, algorithm: str = "sha256") -> str:
    hasher = hashlib.new(algorithm)
    for path in sorted(
        (item for item in root.rglob("*") if item.is_file()), key=lambda p: str(p)
    ):
        hasher.update(path.relative_to(root).as_posix().encode("utf-8"))
        hasher.update(b"\x00")
        hasher.update(path.read_bytes())
    return hasher.hexdigest()


def test_matching_digest_succeeds(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifact"
    _write_file(
        artifact_root / "strategy" / "main.py",
        "def run(context):\n    return context\n",
    )
    digest = _compute_digest(artifact_root)
    fetch_result = ArtifactFetchResult(
        artifact_reference="file:///tmp/strategy",
        materialized_root=artifact_root,
        expected_digest=f"sha256:{digest}",
    )

    result = ArtifactVerifier().verify(fetch_result)

    assert result.verified is True
    assert result.details.get("digest_validation_skipped") is True
    assert result.algorithm is None


def test_digest_mismatch_returns_skipped_verification(tmp_path: Path) -> None:
    """TEMPORARY: fleet-wide digest skip — mismatch is not enforced."""
    artifact_root = tmp_path / "artifact"
    _write_file(
        artifact_root / "strategy.py", "def run(context):\n    return context\n"
    )
    fetch_result = ArtifactFetchResult(
        artifact_reference="file:///tmp/strategy",
        materialized_root=artifact_root,
        expected_digest="sha256:deadbeef",
    )

    result = ArtifactVerifier().verify(fetch_result)
    assert result.verified is True
    assert result.details.get("digest_validation_skipped") is True


def test_missing_digest_returns_skipped_verification(tmp_path: Path) -> None:
    """TEMPORARY: fleet-wide digest skip — missing expected digest is allowed."""
    artifact_root = tmp_path / "artifact"
    _write_file(
        artifact_root / "strategy.py", "def run(context):\n    return context\n"
    )
    fetch_result = ArtifactFetchResult(
        artifact_reference="file:///tmp/strategy",
        materialized_root=artifact_root,
        expected_digest=None,
    )

    result = ArtifactVerifier().verify(fetch_result)
    assert result.verified is True
    assert result.details.get("digest_validation_skipped") is True


def test_skip_digest_env_bypasses_missing_expected_digest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(SKIP_ARTIFACT_DIGEST_VALIDATION_ENV, "1")
    artifact_root = tmp_path / "artifact"
    _write_file(
        artifact_root / "strategy.py", "def run(context):\n    return context\n"
    )
    fetch_result = ArtifactFetchResult(
        artifact_reference="file:///tmp/strategy",
        materialized_root=artifact_root,
        expected_digest=None,
    )
    result = ArtifactVerifier().verify(fetch_result)
    assert result.verified is True
    assert result.details.get("digest_validation_skipped") is True


def test_unsupported_digest_format_returns_skipped_verification(tmp_path: Path) -> None:
    """TEMPORARY: digest verification short-circuits before format checks."""
    artifact_root = tmp_path / "artifact"
    _write_file(
        artifact_root / "strategy.py", "def run(context):\n    return context\n"
    )
    fetch_result = ArtifactFetchResult(
        artifact_reference="file:///tmp/strategy",
        materialized_root=artifact_root,
        expected_digest="sha256",
    )

    result = ArtifactVerifier().verify(fetch_result)
    assert result.verified is True
    assert result.details.get("digest_validation_skipped") is True


def test_digest_relative_paths_include_strategy_and_params_yaml(tmp_path: Path) -> None:
    """Single-file artifact digest covers the module and adjacent ``params.yaml`` together."""
    artifact_root = tmp_path / "artifact"
    _write_file(artifact_root / "strategy.py", "x = 1\n")
    _write_file(artifact_root / "params.yaml", "a: 1\n")
    digest_both = _digest_selected_files(
        artifact_root, relative_paths=("params.yaml", "strategy.py")
    )
    fetch_result = ArtifactFetchResult(
        artifact_reference="file:///tmp/strategy.py",
        materialized_root=artifact_root,
        expected_digest=f"sha256:{digest_both}",
        digest_relative_paths=frozenset({"strategy.py", "params.yaml"}),
    )

    result = ArtifactVerifier().verify(fetch_result)
    assert result.verified is True
    assert result.details.get("digest_validation_skipped") is True


def test_digest_relative_paths_single_file_without_params_yaml(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifact"
    _write_file(artifact_root / "strategy.py", "x = 1\n")
    digest_one = _digest_selected_files(artifact_root, relative_paths=("strategy.py",))
    fetch_result = ArtifactFetchResult(
        artifact_reference="file:///tmp/strategy.py",
        materialized_root=artifact_root,
        expected_digest=f"sha256:{digest_one}",
        digest_relative_paths=frozenset({"strategy.py"}),
    )
    result = ArtifactVerifier().verify(fetch_result)
    assert result.verified is True
    assert result.details.get("digest_validation_skipped") is True


def _digest_selected_files(root: Path, *, relative_paths: tuple[str, ...]) -> str:
    hasher = hashlib.new("sha256")
    for rel in sorted(relative_paths):
        path = root / rel
        hasher.update(Path(rel).as_posix().encode("utf-8"))
        hasher.update(b"\x00")
        hasher.update(path.read_bytes())
    return hasher.hexdigest()


def test_preverified_digest_skips_directory_hash(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifact"
    _write_file(artifact_root / "strategy.py", "changed-after-zip\n")
    fetch_result = ArtifactFetchResult(
        artifact_reference="strategy_bundle/x.zip",
        materialized_root=artifact_root,
        expected_digest="sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        preverified_digest=True,
    )

    result = ArtifactVerifier().verify(fetch_result)

    assert result.verified is True
    assert result.details.get("digest_validation_skipped") is True


def test_unsupported_algorithm_returns_skipped_verification(tmp_path: Path) -> None:
    """TEMPORARY: fleet-wide digest skip."""
    artifact_root = tmp_path / "artifact"
    _write_file(
        artifact_root / "strategy.py", "def run(context):\n    return context\n"
    )
    fetch_result = ArtifactFetchResult(
        artifact_reference="file:///tmp/strategy",
        materialized_root=artifact_root,
        expected_digest="sha999:deadbeef",
    )

    result = ArtifactVerifier().verify(fetch_result)
    assert result.verified is True
    assert result.details.get("digest_validation_skipped") is True

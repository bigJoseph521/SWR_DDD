from __future__ import annotations

from pathlib import Path

import pytest
from runtime.infrastructure.strategy_loader.artifact_fetcher import ArtifactFetchResult
from runtime.infrastructure.strategy_loader.entrypoint_loader import EntrypointLoader
from runtime.domain.bootstrap_failures import (
    BootstrapStage,
    EntrypointLoadFailure,
)
from runtime.domain.launch_spec import LaunchSpec


def _build_launch_spec(
    *, entrypoint: str, artifact_reference: str = "file:///tmp/strategy"
) -> LaunchSpec:
    payload = {
        "runtime_id": "rt-entrypoint",
        "tenant_id": "tenant-1",
        "account_id": "acct-1",
        "strategy_version_id": "sv-1",
        "mode": "PAPER",
        "validated_parameter_identity": "vp-1",
        "artifact_reference": artifact_reference,
        "artifact_digest": "sha256:abc",
        "entrypoint": entrypoint,
        "launch_attempt": 1,
        "correlation_id": "corr-1",
    }
    return LaunchSpec.from_payload(payload)


def _write_module(base: Path, *, package: str, module: str, content: str) -> None:
    package_dir = base / package
    package_dir.mkdir(parents=True, exist_ok=True)
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (package_dir / f"{module}.py").write_text(content, encoding="utf-8")


def test_valid_module_symbol_resolves(tmp_path: Path) -> None:
    _write_module(
        tmp_path,
        package="entrypoint_valid_pkg",
        module="main",
        content="def run(context):\n    return context\n",
    )
    fetch_result = ArtifactFetchResult(
        artifact_reference="file:///tmp/strategy",
        materialized_root=tmp_path,
        expected_digest=None,
    )
    launch_spec = _build_launch_spec(entrypoint="entrypoint_valid_pkg.main:run")

    result = EntrypointLoader().load(launch_spec=launch_spec, fetch_result=fetch_result)

    assert result.module_name == "entrypoint_valid_pkg.main"
    assert result.symbol_name == "run"
    assert callable(result.symbol)


def test_valid_class_symbol_resolves(tmp_path: Path) -> None:
    _write_module(
        tmp_path,
        package="entrypoint_valid_class_pkg",
        module="main",
        content="class Strategy:\n    def run(self, context):\n        return context\n",
    )
    fetch_result = ArtifactFetchResult(
        artifact_reference="file:///tmp/strategy",
        materialized_root=tmp_path,
        expected_digest=None,
    )
    launch_spec = _build_launch_spec(
        entrypoint="entrypoint_valid_class_pkg.main:Strategy"
    )

    result = EntrypointLoader().load(launch_spec=launch_spec, fetch_result=fetch_result)

    assert result.module_name == "entrypoint_valid_class_pkg.main"
    assert result.symbol_name == "Strategy"
    assert isinstance(result.symbol, type)


def test_malformed_entrypoint_fails(tmp_path: Path) -> None:
    fetch_result = ArtifactFetchResult(
        artifact_reference="file:///tmp/strategy",
        materialized_root=tmp_path,
        expected_digest=None,
    )
    launch_spec = _build_launch_spec(entrypoint="bad_entrypoint")

    with pytest.raises(EntrypointLoadFailure) as exc_info:
        EntrypointLoader().load(launch_spec=launch_spec, fetch_result=fetch_result)

    failure = exc_info.value
    assert failure.stage is BootstrapStage.ENTRYPOINT_LOAD
    assert failure.reason_code == "ENTRYPOINT_INVALID"


def test_import_failure_fails(tmp_path: Path) -> None:
    fetch_result = ArtifactFetchResult(
        artifact_reference="file:///tmp/strategy",
        materialized_root=tmp_path,
        expected_digest=None,
    )
    launch_spec = _build_launch_spec(entrypoint="module_does_not_exist.main:run")

    with pytest.raises(EntrypointLoadFailure) as exc_info:
        EntrypointLoader().load(launch_spec=launch_spec, fetch_result=fetch_result)

    failure = exc_info.value
    assert failure.stage is BootstrapStage.ENTRYPOINT_LOAD
    assert failure.reason_code == "ENTRYPOINT_IMPORT_FAILED"


def test_missing_symbol_fails(tmp_path: Path) -> None:
    _write_module(
        tmp_path,
        package="entrypoint_missing_symbol_pkg",
        module="main",
        content="VALUE = 1\n",
    )
    fetch_result = ArtifactFetchResult(
        artifact_reference="file:///tmp/strategy",
        materialized_root=tmp_path,
        expected_digest=None,
    )
    launch_spec = _build_launch_spec(
        entrypoint="entrypoint_missing_symbol_pkg.main:run"
    )

    with pytest.raises(EntrypointLoadFailure) as exc_info:
        EntrypointLoader().load(launch_spec=launch_spec, fetch_result=fetch_result)

    failure = exc_info.value
    assert failure.stage is BootstrapStage.ENTRYPOINT_LOAD
    assert failure.reason_code == "ENTRYPOINT_SYMBOL_NOT_FOUND"


def test_invalid_symbol_type_fails(tmp_path: Path) -> None:
    _write_module(
        tmp_path,
        package="entrypoint_invalid_symbol_pkg",
        module="main",
        content="VALUE = {'run': 'not callable'}\n",
    )
    fetch_result = ArtifactFetchResult(
        artifact_reference="file:///tmp/strategy",
        materialized_root=tmp_path,
        expected_digest=None,
    )
    launch_spec = _build_launch_spec(
        entrypoint="entrypoint_invalid_symbol_pkg.main:VALUE"
    )

    with pytest.raises(EntrypointLoadFailure) as exc_info:
        EntrypointLoader().load(launch_spec=launch_spec, fetch_result=fetch_result)

    failure = exc_info.value
    assert failure.stage is BootstrapStage.ENTRYPOINT_LOAD
    assert failure.reason_code == "ENTRYPOINT_SYMBOL_INVALID"

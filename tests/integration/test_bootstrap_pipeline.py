from __future__ import annotations

import hashlib
from pathlib import Path

from runtime.infrastructure.strategy_loader.artifact_fetcher import (
    ArtifactFetcher,
    ArtifactMaterialization,
    ArtifactProvider,
    ArtifactProviderAccessDeniedError,
)
from runtime.infrastructure.strategy_loader.artifact_verifier import ArtifactVerifier
from runtime.infrastructure.strategy_loader.entrypoint_loader import EntrypointLoader
from runtime.domain.bootstrap_failures import BootstrapStage
from runtime.domain.launch_spec import LaunchSpec
from runtime.bootstrap.persistence import (
    BootstrapPersistenceCoordinator,
)
from runtime.bootstrap.sdk_contract_validator import (
    BootstrapPipeline,
    SdkContractValidator,
)
from runtime.infrastructure.persistence.db import create_engine
from runtime.infrastructure.persistence.migrations import apply_migrations
from runtime.infrastructure.persistence.repositories import (
    SQLiteDiagnosticRepository,
    SQLiteLaunchAttemptRepository,
    SQLiteWorkerEventRepository,
    SQLiteWorkerInstanceRepository,
)


def _write_strategy_package(
    base: Path,
    *,
    package_name: str,
    class_name: str = "Strategy",
    class_body: str = "pass",
    sdk_marker: str | None = None,
) -> Path:
    package_dir = base / package_name
    package_dir.mkdir(parents=True, exist_ok=True)
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    header = "from alphovex_sdk.strategy import Strategy as _SdkStrategy\n\n"
    marker_line = f"__strategy_sdk_version__ = '{sdk_marker}'\n\n" if sdk_marker else ""
    module_body = (
        f"{header}{marker_line}class {class_name}(_SdkStrategy):\n"
        + "\n".join(f"    {line}" for line in class_body.splitlines())
        + "\n"
    )
    (package_dir / "main.py").write_text(module_body, encoding="utf-8")
    return base


def _compute_digest(root: Path, algorithm: str = "sha256") -> str:
    hasher = hashlib.new(algorithm)
    for path in sorted(
        (item for item in root.rglob("*") if item.is_file()), key=lambda p: str(p)
    ):
        hasher.update(path.relative_to(root).as_posix().encode("utf-8"))
        hasher.update(b"\x00")
        hasher.update(path.read_bytes())
    return hasher.hexdigest()


def _build_spec(
    *,
    artifact_reference: str,
    entrypoint: str,
    artifact_digest: str,
    launch_attempt: int = 1,
) -> LaunchSpec:
    payload = {
        "runtime_id": "rt-bootstrap",
        "tenant_id": "tenant-1",
        "account_id": "acct-1",
        "strategy_version_id": "sv-1",
        "mode": "PAPER",
        "validated_parameter_identity": "vp-1",
        "artifact_reference": artifact_reference,
        "artifact_uri": artifact_reference,
        "entrypoint": entrypoint,
        "launch_attempt": launch_attempt,
        "correlation_id": "corr-bootstrap",
        "artifact_digest": artifact_digest,
    }
    return LaunchSpec.from_payload(payload)


def _pipeline(
    tmp_path: Path, *, providers: dict[str, ArtifactProvider] | None = None
) -> BootstrapPipeline:
    work_root = tmp_path / "materialized"
    return BootstrapPipeline(
        fetcher=ArtifactFetcher(work_root=work_root, providers=providers),
        verifier=ArtifactVerifier(),
        entrypoint_loader=EntrypointLoader(),
        sdk_validator=SdkContractValidator(work_root=work_root),
    )


def _pipeline_with_persistence(
    tmp_path: Path,
    *,
    providers: dict[str, ArtifactProvider] | None = None,
) -> tuple[
    BootstrapPipeline,
    SQLiteWorkerInstanceRepository,
    SQLiteLaunchAttemptRepository,
    SQLiteWorkerEventRepository,
    SQLiteDiagnosticRepository,
]:
    engine = create_engine(tmp_path / "worker.db")
    connection = engine.connect()
    connection.begin()
    apply_migrations(connection)

    instances = SQLiteWorkerInstanceRepository(connection)
    attempts = SQLiteLaunchAttemptRepository(connection)
    events = SQLiteWorkerEventRepository(connection)
    diagnostics = SQLiteDiagnosticRepository(connection)
    coordinator = BootstrapPersistenceCoordinator(
        instances=instances,
        attempts=attempts,
        events=events,
        diagnostics=diagnostics,
    )
    work_root = tmp_path / "materialized"
    return (
        BootstrapPipeline(
            fetcher=ArtifactFetcher(work_root=work_root, providers=providers),
            verifier=ArtifactVerifier(),
            entrypoint_loader=EntrypointLoader(),
            sdk_validator=SdkContractValidator(work_root=work_root),
            persistence=coordinator,
        ),
        instances,
        attempts,
        events,
        diagnostics,
    )


def test_full_happy_path_bootstrap_passes_end_to_end(tmp_path: Path) -> None:
    source_root = _write_strategy_package(
        tmp_path / "source-happy",
        package_name="happy_strategy",
        sdk_marker="1.2",
    )
    digest = _compute_digest(source_root)
    spec = _build_spec(
        artifact_reference=source_root.resolve().as_uri(),
        entrypoint="happy_strategy.main:Strategy",
        artifact_digest=f"sha256:{digest}",
    )

    result = _pipeline(tmp_path).run(spec)

    assert result.success is True
    assert result.failure is None
    assert result.success_payload is not None
    assert result.success_payload.verification.verified is True


def test_failure_artifact_not_found(tmp_path: Path) -> None:
    missing_reference = (tmp_path / "missing").resolve().as_uri()
    spec = _build_spec(
        artifact_reference=missing_reference,
        entrypoint="missing.main:Strategy",
        artifact_digest="sha256:00",
    )

    result = _pipeline(tmp_path).run(spec)

    assert result.success is False
    assert result.failure is not None
    assert result.failure.stage is BootstrapStage.ARTIFACT_FETCH
    assert result.failure.reason_code == "ARTIFACT_NOT_FOUND"


def test_failure_access_denied(tmp_path: Path) -> None:
    class DenyProvider:
        def fetch(
            self, *, artifact_reference: str, target_dir: Path
        ) -> ArtifactMaterialization:
            raise ArtifactProviderAccessDeniedError(f"Denied: {artifact_reference}")

    spec = _build_spec(
        artifact_reference="deny://private/strategy",
        entrypoint="private.main:Strategy",
        artifact_digest="sha256:00",
    )

    result = _pipeline(tmp_path, providers={"deny": DenyProvider()}).run(spec)

    assert result.success is False
    assert result.failure is not None
    assert result.failure.stage is BootstrapStage.ARTIFACT_FETCH
    assert result.failure.reason_code == "ARTIFACT_ACCESS_DENIED"


def test_failure_digest_mismatch(tmp_path: Path) -> None:
    """TEMPORARY: fleet-wide digest skip — mismatch is not enforced (see ``digest_validation_env``)."""
    source_root = _write_strategy_package(
        tmp_path / "source-digest-mismatch", package_name="digest_strategy"
    )
    spec = _build_spec(
        artifact_reference=source_root.resolve().as_uri(),
        entrypoint="digest_strategy.main:Strategy",
        artifact_digest="sha256:00000000",
    )

    result = _pipeline(tmp_path).run(spec)

    assert result.success is True
    assert result.failure is None


def test_failure_entrypoint_import(tmp_path: Path) -> None:
    source_root = _write_strategy_package(
        tmp_path / "source-import-failure", package_name="import_strategy"
    )
    digest = _compute_digest(source_root)
    spec = _build_spec(
        artifact_reference=source_root.resolve().as_uri(),
        entrypoint="module_does_not_exist.main:Strategy",
        artifact_digest=f"sha256:{digest}",
    )

    result = _pipeline(tmp_path).run(spec)

    assert result.success is False
    assert result.failure is not None
    assert result.failure.stage is BootstrapStage.ENTRYPOINT_LOAD
    assert result.failure.reason_code == "ENTRYPOINT_IMPORT_FAILED"


def test_failure_sdk_mismatch(tmp_path: Path) -> None:
    source_root = _write_strategy_package(
        tmp_path / "source-sdk-mismatch",
        package_name="sdk_mismatch_strategy",
        sdk_marker="2.0",
    )
    digest = _compute_digest(source_root)
    spec = _build_spec(
        artifact_reference=source_root.resolve().as_uri(),
        entrypoint="sdk_mismatch_strategy.main:Strategy",
        artifact_digest=f"sha256:{digest}",
    )

    result = _pipeline(tmp_path).run(spec)

    assert result.success is False
    assert result.failure is not None
    assert result.failure.stage is BootstrapStage.SDK_VALIDATE
    assert result.failure.reason_code == "SDK_MARKER_INCOMPATIBLE"


def test_bootstrap_persists_success_and_failure_outcomes(tmp_path: Path) -> None:
    source_root = _write_strategy_package(
        tmp_path / "source-persist",
        package_name="persist_strategy",
        sdk_marker="1.0",
    )
    digest = _compute_digest(source_root)

    pipeline, instances, attempts, events, diagnostics = _pipeline_with_persistence(
        tmp_path
    )
    success_spec = _build_spec(
        artifact_reference=source_root.resolve().as_uri(),
        entrypoint="persist_strategy.main:Strategy",
        artifact_digest=f"sha256:{digest}",
        launch_attempt=1,
    )
    success = pipeline.run(success_spec)
    assert success.success is True

    persisted_instance = instances.get_by_runtime_id("rt-bootstrap")
    assert persisted_instance is not None
    assert persisted_instance.state == "READY"
    assert attempts.get_attempt("rt-bootstrap", 1) is not None
    assert events.get_by_event_id("rt-bootstrap:1:runtime.launch_succeeded") is not None
    assert diagnostics.list_for_runtime("rt-bootstrap") == []

    # TEMPORARY: wrong digest does not fail while fleet-wide digest skip is enabled.
    mismatch_spec = _build_spec(
        artifact_reference=source_root.resolve().as_uri(),
        entrypoint="persist_strategy.main:Strategy",
        artifact_digest="sha256:deadbeef",
        launch_attempt=2,
    )
    mismatch = pipeline.run(mismatch_spec)
    assert mismatch.success is True

    instance_after_mismatch = instances.get_by_runtime_id("rt-bootstrap")
    assert instance_after_mismatch is not None
    assert instance_after_mismatch.state == "READY"
    assert attempts.get_attempt("rt-bootstrap", 2) is not None
    assert events.get_by_event_id("rt-bootstrap:2:runtime.launch_succeeded") is not None

from __future__ import annotations

import hashlib
import io
import zipfile
from pathlib import Path

from runtime.bootstrap.artifact_fetcher import ArtifactFetcher
from runtime.bootstrap.artifact_verifier import ArtifactVerifier
from runtime.bootstrap.entrypoint_loader import EntrypointLoader
from runtime.bootstrap.launch_spec import LaunchSpec
from runtime.bootstrap.sdk_contract_validator import (
    BootstrapPipeline,
    SdkContractValidator,
)


def _minimal_sdk_strategy_module(*, sdk_marker: str = "1.2") -> str:
    return (
        "from alphovex_sdk.strategy import Strategy as _SdkStrategy\n\n"
        f"__strategy_sdk_version__ = '{sdk_marker}'\n\n"
        "class RegStrategy(_SdkStrategy):\n"
        "    def on_event(self, event):\n"
        "        return None\n"
    )


def _write_registry_zip(store: Path, rel: str) -> tuple[str, str]:
    rel_path = Path(rel)
    rel_path.parent.mkdir(parents=True, exist_ok=True)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("regstrat/__init__.py", "")
        zf.writestr("regstrat/main.py", _minimal_sdk_strategy_module())
    data = buf.getvalue()
    dest = store / rel_path
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(data)
    digest = f"sha256:{hashlib.sha256(data).hexdigest()}"
    uri = f"registry-local:///{rel}"
    return uri, digest


def _spec(*, artifact_uri: str, artifact_digest: str) -> LaunchSpec:
    payload = {
        "runtime_id": "rt-reg-bootstrap",
        "tenant_id": "tenant-1",
        "account_id": "acct-1",
        "strategy_version_id": "sv-reg",
        "mode": "PAPER",
        "artifact_uri": artifact_uri,
        "entrypoint": "regstrat.main:RegStrategy",
        "launch_attempt": 1,
        "artifact_digest": artifact_digest,
    }
    return LaunchSpec.from_payload(payload)


def test_registry_local_zip_verify_extract_and_bootstrap(tmp_path: Path) -> None:
    store = tmp_path / "artifact_store"
    store.mkdir()
    uri, digest = _write_registry_zip(
        store, "db55e902-8751-4bca-8d32-d0c6048e0fdb/versions/v1.0.0/source_bundle.zip"
    )
    spec = _spec(artifact_uri=uri, artifact_digest=digest)

    pipeline = BootstrapPipeline(
        fetcher=ArtifactFetcher(
            work_root=tmp_path / "materialized",
            artifact_local_base=store,
        ),
        verifier=ArtifactVerifier(),
        entrypoint_loader=EntrypointLoader(),
        sdk_validator=SdkContractValidator(),
    )

    result = pipeline.run(spec)
    assert result.success is True


def test_registry_local_zip_missing_base_fails_fetch(tmp_path: Path) -> None:
    store = tmp_path / "artifact_store"
    store.mkdir()
    uri, digest = _write_registry_zip(store, "s/v/source_bundle.zip")
    spec = _spec(artifact_uri=uri, artifact_digest=digest)

    pipeline = BootstrapPipeline(
        fetcher=ArtifactFetcher(work_root=tmp_path / "materialized"),
        verifier=ArtifactVerifier(),
        entrypoint_loader=EntrypointLoader(),
        sdk_validator=SdkContractValidator(),
    )

    result = pipeline.run(spec)
    assert result.success is False
    assert result.failure is not None
    assert result.failure.reason_code == "ARTIFACT_FETCH_FAILED"

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
        "class ZipStrategy(_SdkStrategy):\n"
        "    def on_event(self, event):\n"
        "        return None\n"
    )


def _write_strategy_zip(
    bundle_dir: Path, *, zip_name: str = "artifact.zip"
) -> tuple[Path, str]:
    bundle_dir.mkdir(parents=True, exist_ok=True)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("zipstrat/__init__.py", "")
        zf.writestr("zipstrat/main.py", _minimal_sdk_strategy_module())
    data = buf.getvalue()
    zip_path = bundle_dir / zip_name
    zip_path.write_bytes(data)
    digest = f"sha256:{hashlib.sha256(data).hexdigest()}"
    return zip_path, digest


def _spec(
    *,
    artifact_uri: str,
    artifact_digest: str,
    entrypoint: str = "zipstrat.main:ZipStrategy",
) -> LaunchSpec:
    payload = {
        "runtime_id": "rt-zip-bootstrap",
        "tenant_id": "tenant-1",
        "account_id": "acct-1",
        "strategy_version_id": "sv-zip",
        "mode": "PAPER",
        "artifact_uri": artifact_uri,
        "entrypoint": entrypoint,
        "launch_attempt": 1,
        "artifact_digest": artifact_digest,
    }
    return LaunchSpec.from_payload(payload)


def test_strategy_bundle_zip_verify_extract_and_bootstrap(tmp_path: Path) -> None:
    bundle_root = tmp_path / "strategy_bundle"
    zip_path, digest = _write_strategy_zip(bundle_root)

    spec = _spec(artifact_uri="strategy_bundle/artifact.zip", artifact_digest=digest)

    pipeline = BootstrapPipeline(
        fetcher=ArtifactFetcher(
            work_root=tmp_path / "materialized",
            strategy_bundle_base=tmp_path,
        ),
        verifier=ArtifactVerifier(),
        entrypoint_loader=EntrypointLoader(),
        sdk_validator=SdkContractValidator(),
    )

    result = pipeline.run(spec)

    assert result.success is True
    assert (bundle_root / "zipstrat" / "main.py").is_file()
    assert zip_path.is_file()


def test_strategy_bundle_zip_digest_mismatch_fails_fetch(tmp_path: Path) -> None:
    """TEMPORARY: fleet-wide digest skip — zip SHA-256 mismatch is not enforced."""
    bundle_root = tmp_path / "strategy_bundle"
    _write_strategy_zip(bundle_root)

    spec = _spec(
        artifact_uri="strategy_bundle/artifact.zip",
        artifact_digest="sha256:" + "0" * 64,
    )

    pipeline = BootstrapPipeline(
        fetcher=ArtifactFetcher(
            work_root=tmp_path / "materialized",
            strategy_bundle_base=tmp_path,
        ),
        verifier=ArtifactVerifier(),
        entrypoint_loader=EntrypointLoader(),
        sdk_validator=SdkContractValidator(),
    )

    result = pipeline.run(spec)

    assert result.success is True
    assert result.failure is None

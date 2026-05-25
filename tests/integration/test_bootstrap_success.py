from __future__ import annotations

import hashlib
from pathlib import Path

from runtime.bootstrap.dependency_container import build_dependency_container
from runtime.infrastructure.config.settings import load_settings_from_bundle_dict
from runtime.domain.enums import WorkerPhase


def _write_strategy_package(base: Path) -> Path:
    package_dir = base / "strategy_pkg"
    package_dir.mkdir(parents=True, exist_ok=True)
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (package_dir / "main.py").write_text(
        "from alphovex_sdk.strategy import Strategy as _SdkStrategy\n"
        "\n"
        "class Strategy(_SdkStrategy):\n"
        "    __strategy_sdk_version__ = '1.0'\n",
        encoding="utf-8",
    )
    return base


def _digest(root: Path) -> str:
    hasher = hashlib.sha256()
    for path in sorted(
        (item for item in root.rglob("*") if item.is_file()), key=lambda p: str(p)
    ):
        hasher.update(path.relative_to(root).as_posix().encode("utf-8"))
        hasher.update(b"\x00")
        hasher.update(path.read_bytes())
    return hasher.hexdigest()


def test_minimal_settings_bootstrap_start_run_stop(tmp_path: Path) -> None:
    source_root = _write_strategy_package(tmp_path / "source")
    digest = _digest(source_root)

    settings = load_settings_from_bundle_dict(
        {
            "runtime_id": "rt-e2e-1",
            "tenant_id": "tenant-1",
            "account_id": "acct-1",
            "strategy_version_id": "sv-1",
            "mode": "PAPER",
            "artifact_uri": source_root.resolve().as_uri(),
            "artifact_digest": f"sha256:{digest}",
            "entrypoint": "strategy_pkg.main:Strategy",
            "launch_attempt": 1,
            "work_root": "work",
        },
        base_dir=tmp_path,
    )
    container = build_dependency_container(settings)

    container.worker_app.start()
    assert container.lifecycle_service.phase is WorkerPhase.READY

    stopped = container.worker_app.stop()
    assert stopped is True
    assert container.lifecycle_service.phase is WorkerPhase.STOPPED

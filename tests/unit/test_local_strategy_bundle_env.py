from __future__ import annotations

import json
from pathlib import Path

import pytest
from runtime.infrastructure.strategy_loader import strategy_bundle_loader as sbl
from runtime.infrastructure.config.settings import (
    load_settings,
    load_settings_from_bundle_dict,
)


def test_load_settings_reads_default_setting_json_when_sds_env_unset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``load_settings()`` uses cwd ``strategy_bundle/setting.json`` when SDS env is unset."""
    bundle = tmp_path / "strategy_bundle"
    bundle.mkdir()
    (bundle / "artifact.py").write_text("#\n", encoding="utf-8")
    data = {
        "runtime_id": "rt-default-setting",
        "tenant_id": "t-1",
        "strategy_version_id": "sv-1",
        "mode": "PAPER",
        "launch_attempt": 1,
        "entrypoint": "pkg:Cls",
        "artifact_uri": "strategy_bundle/artifact.py",
        "artifact_digest": "sha256:fixed",
        "deployment_id": "dep-test",
        "account_id": "a-1",
    }
    (bundle / "setting.json").write_text(json.dumps(data), encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("STRATEGY_DEPLOYMENT_SERVICE_BASE_URL", raising=False)
    monkeypatch.delenv("DEPLOYMENT_ID", raising=False)
    settings = load_settings(print_launch_banner=False)
    assert settings.launch_spec.runtime_id == "rt-default-setting"


def test_load_settings_from_file_reads_strategy_bundle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = tmp_path / "strategy_bundle"
    bundle.mkdir()
    (bundle / "artifact.py").write_text("#\n", encoding="utf-8")
    data = {
        "runtime_id": "rt-file",
        "tenant_id": "t-1",
        "strategy_version_id": "sv-1",
        "mode": "PAPER",
        "launch_attempt": 1,
        "entrypoint": "pkg:Cls",
        "artifact_uri": "strategy_bundle/artifact.py",
        "artifact_digest": "sha256:fixed",
        "deployment_id": "dep-test",
        "account_id": "a-1",
    }
    (bundle / "setting.json").write_text(json.dumps(data), encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("STRATEGY_DEPLOYMENT_SERVICE_BASE_URL", raising=False)
    monkeypatch.delenv("DEPLOYMENT_ID", raising=False)
    settings = load_settings(
        bundle_path=bundle / "setting.json",
        print_launch_banner=False,
    )
    assert settings.launch_spec.runtime_id == "rt-file"


def test_load_settings_from_bundle_dict_respects_account_only_scope(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    settings = load_settings_from_bundle_dict(
        {
            "runtime_id": "rt-test",
            "tenant_id": "t-1",
            "strategy_version_id": "sv-1",
            "mode": "PAPER",
            "launch_attempt": 1,
            "entrypoint": "pkg:Cls",
            "artifact_uri": "file:///tmp/x",
            "artifact_digest": "sha256:aaa",
            "deployment_id": "dep-test",
            "account_id": "a-1",
        },
        print_launch_banner=False,
    )
    assert settings.launch_spec.account_id == "a-1"
    assert settings.strategy_runtime_manager_base_url == ""


def test_default_bundle_setting_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    assert (
        sbl.default_bundle_setting_path()
        == (tmp_path / "strategy_bundle" / "setting.json").resolve()
    )


def test_default_work_root_uses_deployment_id(tmp_path: Path) -> None:
    _, work_root, _, _ = sbl.raw_dict_to_launch_payload(
        {
            "runtime_id": "rt-1",
            "deployment_id": "dep-abc-123",
            "mode": "PAPER",
            "entrypoint": "s:Strategy",
            "artifact_uri": "file:///tmp/x.zip",
            "artifact_digest": "sha256:00",
        },
        base_dir=tmp_path,
    )
    assert work_root == str((tmp_path / "dep-abc-123").resolve())


def test_default_work_root_prefers_deployment_id_over_job_id(
    tmp_path: Path,
) -> None:
    _, work_root, _, _ = sbl.raw_dict_to_launch_payload(
        {
            "runtime_id": "rt-1",
            "deployment_id": "dep-primary",
            "job_id": "job-backtest",
            "mode": "BACKTEST",
            "entrypoint": "s:Strategy",
            "artifact_uri": "file:///tmp/x.zip",
            "artifact_digest": "sha256:00",
        },
        base_dir=tmp_path,
    )
    assert work_root == str((tmp_path / "dep-primary").resolve())


def test_default_work_root_raises_without_deployment_id(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="deployment_id is required"):
        sbl.raw_dict_to_launch_payload(
            {
                "runtime_id": "rt-1",
                "job_id": "job-backtest",
                "mode": "BACKTEST",
                "entrypoint": "s:Strategy",
                "artifact_uri": "file:///tmp/x.zip",
                "artifact_digest": "sha256:00",
            },
            base_dir=tmp_path,
        )


def test_default_work_root_uses_deployment_id_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DEPLOYMENT_ID", "env-dep-99")
    _, work_root, _, _ = sbl.raw_dict_to_launch_payload(
        {
            "runtime_id": "rt-1",
            "mode": "PAPER",
            "entrypoint": "s:Strategy",
            "artifact_uri": "file:///tmp/x.zip",
            "artifact_digest": "sha256:00",
        },
        base_dir=tmp_path,
    )
    assert work_root == str((tmp_path / "env-dep-99").resolve())


def test_explicit_work_root_overrides_deployment_id(tmp_path: Path) -> None:
    _, work_root, _, _ = sbl.raw_dict_to_launch_payload(
        {
            "runtime_id": "rt-1",
            "deployment_id": "dep-abc",
            "work_root": "custom-work",
            "mode": "PAPER",
            "entrypoint": "s:Strategy",
            "artifact_uri": "file:///tmp/x.zip",
            "artifact_digest": "sha256:00",
        },
        base_dir=tmp_path,
    )
    assert work_root == str((tmp_path / "custom-work").resolve())


def test_default_work_root_sanitizes_deployment_id(tmp_path: Path) -> None:
    _, work_root, _, _ = sbl.raw_dict_to_launch_payload(
        {
            "runtime_id": "rt-1",
            "deployment_id": "dep/with:unsafe*chars",
            "mode": "PAPER",
            "entrypoint": "s:Strategy",
            "artifact_uri": "file:///tmp/x.zip",
            "artifact_digest": "sha256:00",
        },
        base_dir=tmp_path,
    )
    assert work_root == str((tmp_path / "dep_with_unsafe_chars").resolve())

from __future__ import annotations

import json
from pathlib import Path

import pytest
from runtime.bootstrap import strategy_bundle_loader as sbl
from runtime.config.settings import (
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

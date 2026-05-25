from __future__ import annotations

import json
from unittest.mock import MagicMock, patch
from urllib.error import URLError

import pytest

from runtime.infrastructure.strategy_loader.deployment_runtime_context_bootstrap import (
    _data_source_to_feed_list,
    fetch_bundle_from_deployment_runtime_context,
    worker_bundle_dict_from_runtime_context_response,
)
from runtime.bootstrap.launch_spec import LaunchSpecValidationError
from runtime.infrastructure.config.settings import load_settings


def _mock_urlopen_context(body: bytes, *, code: int = 200) -> MagicMock:
    inner = MagicMock()
    inner.getcode.return_value = code
    inner.read.return_value = body
    ctx = MagicMock()
    ctx.__enter__.return_value = inner
    ctx.__exit__.return_value = None
    return ctx


def test_data_source_to_feed_list() -> None:
    assert _data_source_to_feed_list("BAR") == ["bars"]
    assert _data_source_to_feed_list("TRADES") == ["trades"]
    assert _data_source_to_feed_list(["bars", "quotes"]) == ["bars", "quotes"]


def test_worker_bundle_dict_from_runtime_context_response() -> None:
    rc = {
        "account_id": "acct-1",
        "artifact_uri": "registry-local:///x/zip.zip",
        "artifact_digest": "sha256:deadbeef",
        "bar_timeframe": "1m",
        "correlation_id": "corr-1",
        "data_source": "BAR",
        "entrypoint": "mean_reversion:MeanReversionStrategy",
        "instrument_id": "inst_iwm",
        "job_id": "dep-uuid",
        "mode": "PAPER",
        "strategy_version_id": "sv-from-sds",
        "strategy_params": {"lookback": 20},
        "symbol": "IWM",
    }
    identity = {
        "runtime_id": "rt-1",
        "strategy_version_id": "sv-from-env",
        "tenant_id": "",
        "launch_attempt": 2,
    }
    out = worker_bundle_dict_from_runtime_context_response(rc, identity=identity)
    assert out["runtime_id"] == "rt-1"
    assert out["strategy_version_id"] == "sv-from-sds"
    assert out["launch_attempt"] == 2
    assert out["account_id"] == "acct-1"
    assert out["artifact_uri"] == "registry-local:///x/zip.zip"
    assert out["artifact_digest"] == "sha256:deadbeef"
    assert out["parameters"]["lookback"] == 20
    assert out["parameters"]["bar_timeframe"] == "1m"
    assert out["parameters"]["data_source"] == ["bars"]
    assert out["parameters"]["symbol"] == "IWM"
    assert out["parameters"]["instrument_id"] == "inst_iwm"
    assert out["strategy_params"] == {"lookback": 20}


def test_fetch_bundle_requires_runtime_identity(
    tmp_path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("STRATEGY_DEPLOYMENT_SERVICE_BASE_URL", "http://127.0.0.1:5090")
    monkeypatch.setenv("DEPLOYMENT_ID", "dep-1")
    monkeypatch.delenv("RUNTIME_ID", raising=False)
    monkeypatch.delenv("SWR_RUNTIME_ID", raising=False)
    monkeypatch.delenv("SWR_STRATEGY_VERSION_ID", raising=False)

    body = json.dumps(
        {
            "account_id": "a",
            "mode": "PAPER",
            "artifact_uri": "x.zip",
            "entrypoint": "m:C",
            "correlation_id": "c",
            "job_id": "dep-1",
            "bar_timeframe": "1m",
            "data_source": "BAR",
            "instrument_id": "i",
            "strategy_params": {},
            "strategy_version_id": "sv-from-response",
        }
    ).encode()

    with patch(
        "runtime.infrastructure.strategy_loader.deployment_runtime_context_bootstrap.urlopen",
        return_value=_mock_urlopen_context(body),
    ):
        with pytest.raises(LaunchSpecValidationError) as ei:
            fetch_bundle_from_deployment_runtime_context(
                base_url="http://127.0.0.1:5090",
                deployment_id="dep-1",
            )
    assert ei.value.reason == "deployment_runtime_context_missing_identity"
    assert "runtime_id" in ei.value.field_errors
    out = capsys.readouterr().out
    assert "HTTP 200" in out


def test_fetch_bundle_strategy_version_id_from_response_without_env_var(
    tmp_path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("RUNTIME_ID", "rt-only")
    monkeypatch.delenv("SWR_STRATEGY_VERSION_ID", raising=False)
    body = json.dumps(
        {
            "account_id": "a",
            "mode": "PAPER",
            "artifact_uri": "x.zip",
            "entrypoint": "m:C",
            "correlation_id": "c",
            "job_id": "dep-1",
            "bar_timeframe": "1m",
            "data_source": "BAR",
            "instrument_id": "i",
            "strategy_params": {},
            "strategy_version_id": "sv-from-sds",
        }
    ).encode()
    with patch(
        "runtime.infrastructure.strategy_loader.deployment_runtime_context_bootstrap.urlopen",
        return_value=_mock_urlopen_context(body),
    ):
        bundle = fetch_bundle_from_deployment_runtime_context(
            base_url="http://h:1",
            deployment_id="dep-1",
        )
    assert bundle["strategy_version_id"] == "sv-from-sds"
    assert "HTTP 200" in capsys.readouterr().out


def test_fetch_bundle_success(
    tmp_path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("RUNTIME_ID", "rt-x")
    monkeypatch.setenv("SWR_STRATEGY_VERSION_ID", "sv-x")
    monkeypatch.setenv("SWR_LAUNCH_ATTEMPT", "3")

    body = json.dumps(
        {
            "account_id": "a",
            "mode": "PAPER",
            "artifact_uri": "registry-local:///z.zip",
            "artifact_digest": "sha256:abc",
            "entrypoint": "m:C",
            "correlation_id": "c",
            "job_id": "dep-1",
            "bar_timeframe": "1m",
            "data_source": "BAR",
            "instrument_id": "i",
            "strategy_params": {"k": 1},
            "strategy_version_id": "sv-from-sds",
        }
    ).encode()

    with patch(
        "runtime.infrastructure.strategy_loader.deployment_runtime_context_bootstrap.urlopen",
        return_value=_mock_urlopen_context(body),
    ):
        bundle = fetch_bundle_from_deployment_runtime_context(
            base_url="http://h:1",
            deployment_id="dep-1",
        )
    assert bundle["runtime_id"] == "rt-x"
    assert bundle["strategy_version_id"] == "sv-from-sds"
    assert bundle["launch_attempt"] == 3
    assert bundle["artifact_uri"] == "registry-local:///z.zip"
    assert capsys.readouterr().out.count("HTTP 200") == 1


def test_fetch_bundle_url_error(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("RUNTIME_ID", "r")
    monkeypatch.setenv("SWR_STRATEGY_VERSION_ID", "s")
    with patch(
        "runtime.infrastructure.strategy_loader.deployment_runtime_context_bootstrap.urlopen",
        side_effect=URLError("nope"),
    ):
        with pytest.raises(LaunchSpecValidationError) as ei:
            fetch_bundle_from_deployment_runtime_context(
                base_url="http://h:1",
                deployment_id="d",
            )
    assert ei.value.reason == "deployment_runtime_context_unreachable"


def test_fetch_bundle_http_error(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("RUNTIME_ID", "r")
    monkeypatch.setenv("SWR_STRATEGY_VERSION_ID", "s")
    from io import BytesIO
    from urllib.error import HTTPError

    err = HTTPError("http://h:1/x", 404, "nf", hdrs={}, fp=BytesIO(b'{"x":1}'))
    with patch(
        "runtime.infrastructure.strategy_loader.deployment_runtime_context_bootstrap.urlopen",
        side_effect=err,
    ):
        with pytest.raises(LaunchSpecValidationError) as ei:
            fetch_bundle_from_deployment_runtime_context(
                base_url="http://h:1",
                deployment_id="d",
            )
    assert ei.value.reason == "deployment_runtime_context_http_error"


def test_load_settings_prefers_sds_over_strategy_bundle_setting_json(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When deployment-service env is set, runtime-context wins over ``strategy_bundle/setting.json``."""
    monkeypatch.chdir(tmp_path)
    bundle = tmp_path / "strategy_bundle"
    bundle.mkdir()
    (bundle / "artifact.py").write_text("#\n", encoding="utf-8")
    file_data = {
        "runtime_id": "rt-from-file",
        "tenant_id": "",
        "strategy_version_id": "sv-from-file",
        "mode": "PAPER",
        "launch_attempt": 1,
        "entrypoint": "pkg:Cls",
        "artifact_uri": "strategy_bundle/artifact.py",
        "artifact_digest": "sha256:fromfile",
        "account_id": "acct-from-file",
    }
    (bundle / "setting.json").write_text(json.dumps(file_data), encoding="utf-8")
    monkeypatch.setenv("STRATEGY_DEPLOYMENT_SERVICE_BASE_URL", "http://127.0.0.1:5090")
    monkeypatch.setenv("DEPLOYMENT_ID", "dep-1")
    monkeypatch.setenv("RUNTIME_ID", "rt-sds")
    monkeypatch.delenv("SWR_STRATEGY_VERSION_ID", raising=False)
    monkeypatch.setenv("SWR_RUNTIME_MANAGER_GRPC_TARGET", "127.0.0.1:50052")

    api = {
        "account_id": "acct-from-sds",
        "mode": "PAPER",
        "artifact_uri": "registry-local:///z.zip",
        "artifact_digest": "sha256:fromsds",
        "entrypoint": "m:C",
        "correlation_id": "c",
        "job_id": "dep-1",
        "bar_timeframe": "1m",
        "data_source": "BAR",
        "instrument_id": "inst",
        "strategy_params": {},
        "strategy_version_id": "sv-from-sds-api",
    }
    body = json.dumps(api).encode()
    with patch(
        "runtime.infrastructure.strategy_loader.deployment_runtime_context_bootstrap.urlopen",
        return_value=_mock_urlopen_context(body),
    ):
        settings = load_settings(print_launch_banner=False)
    assert settings.launch_spec.runtime_id == "rt-sds"
    assert settings.launch_spec.strategy_version_id == "sv-from-sds-api"
    assert settings.launch_spec.account_id == "acct-from-sds"
    assert settings.launch_spec.artifact_uri == "registry-local:///z.zip"

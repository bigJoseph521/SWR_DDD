from __future__ import annotations

from pathlib import Path

from runtime.config.settings import load_settings_from_bundle_dict
from runtime.main import _worker_control_bind_candidates


def _base_bundle() -> dict[str, object]:
    return {
        "runtime_id": "rt-1",
        "tenant_id": "tenant-1",
        "account_id": "acct-1",
        "strategy_version_id": "sv-1",
        "mode": "PAPER",
        "launch_attempt": 1,
        "entrypoint": "strategy.main:Strategy",
        "artifact_digest": "sha256:abcd",
        "artifact_uri": "file:///tmp/strategy",
    }


def test_worker_control_bind_candidates_auto_increment_from_primary() -> None:
    data = dict(_base_bundle())
    data["worker_control_grpc_bind"] = "127.0.0.1:50051"
    settings = load_settings_from_bundle_dict(
        data,
        base_dir=Path.cwd(),
        print_launch_banner=False,
    )
    _primary, candidates = _worker_control_bind_candidates(settings)
    assert candidates[0] == "127.0.0.1:50051"
    assert "127.0.0.1:50052" in candidates
    assert "127.0.0.1:50053" in candidates


def test_worker_control_bind_candidates_custom_list_no_auto_increment() -> None:
    data = dict(_base_bundle())
    data["worker_control_grpc_bind"] = "127.0.0.1:50051"
    data["worker_control_grpc_fallback_ports"] = "127.0.0.1:50053,127.0.0.1:50054"
    settings = load_settings_from_bundle_dict(
        data,
        base_dir=Path.cwd(),
        print_launch_banner=False,
    )
    _primary, candidates = _worker_control_bind_candidates(settings)
    assert candidates == ["127.0.0.1:50051", "127.0.0.1:50053", "127.0.0.1:50054"]


def test_worker_control_bind_candidates_no_fallback_only_primary() -> None:
    data = dict(_base_bundle())
    data["worker_control_grpc_bind"] = "127.0.0.1:50051"
    data["worker_control_grpc_no_fallback"] = True
    settings = load_settings_from_bundle_dict(
        data,
        base_dir=Path.cwd(),
        print_launch_banner=False,
    )
    _primary, candidates = _worker_control_bind_candidates(settings)
    assert candidates == ["127.0.0.1:50051"]

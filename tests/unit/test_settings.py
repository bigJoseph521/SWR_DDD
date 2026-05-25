from __future__ import annotations

from pathlib import Path

import pytest
from runtime.bootstrap.launch_spec import LaunchSpecValidationError
from runtime.config.settings import load_settings_from_bundle_dict


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


def _load(data: dict[str, object], *, base: Path | None = None) -> None:
    load_settings_from_bundle_dict(
        data, base_dir=base or Path.cwd(), print_launch_banner=False
    )


def test_load_settings_missing_mode_reports_launch_spec_error() -> None:
    data = dict(_base_bundle())
    del data["mode"]
    with pytest.raises(LaunchSpecValidationError) as exc_info:
        _load(data)
    assert exc_info.value.field_errors["mode"] == "required_field_missing"


@pytest.mark.parametrize(
    ("field_name",),
    [
        ("runtime_id",),
        ("strategy_version_id",),
        ("mode",),
        ("launch_attempt",),
        ("entrypoint",),
        ("artifact_uri",),
        ("account_id",),
    ],
)
def test_load_settings_missing_required_field_reports_error(field_name: str) -> None:
    data = dict(_base_bundle())
    del data[field_name]
    with pytest.raises(LaunchSpecValidationError) as exc_info:
        _load(data)
    assert exc_info.value.field_errors[field_name] == "required_field_missing"


def test_load_settings_missing_artifact_digest_when_uri_not_on_disk_is_allowed() -> (
    None
):
    """While digest validation is temporarily disabled, missing digest is accepted."""
    data = dict(_base_bundle())
    del data["artifact_digest"]
    data["artifact_uri"] = "file:///no/such/artifact"
    s = load_settings_from_bundle_dict(
        data, base_dir=Path.cwd(), print_launch_banner=False
    )
    assert s.launch_spec.artifact_digest is None


def test_load_settings_empty_mode_reports_launch_spec_error() -> None:
    data = dict(_base_bundle())
    data["mode"] = "   "
    with pytest.raises(LaunchSpecValidationError) as exc_info:
        _load(data)
    assert exc_info.value.field_errors["mode"] == "invalid_mode:"


def test_load_settings_missing_multiple_required_fields() -> None:
    data = dict(_base_bundle())
    del data["runtime_id"]
    del data["mode"]
    with pytest.raises(LaunchSpecValidationError) as exc_info:
        _load(data)
    assert exc_info.value.field_errors["runtime_id"] == "required_field_missing"
    assert exc_info.value.field_errors["mode"] == "required_field_missing"


def test_load_settings_blank_optional_field_is_validated_by_launch_spec() -> None:
    data = dict(_base_bundle())
    data["trader_id"] = "  "
    with pytest.raises(LaunchSpecValidationError) as exc_info:
        _load(data)
    assert exc_info.value.field_errors["trader_id"] == "must_not_be_empty"


def test_load_settings_replay_ingress_trace_from_bundle() -> None:
    data = dict(_base_bundle())
    data["replay_ingress_trace_payload"] = True
    s = load_settings_from_bundle_dict(
        data, base_dir=Path.cwd(), print_launch_banner=False
    )
    assert s.replay_ingress_trace_payload is True


def test_load_settings_replay_ingress_trace_from_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SWR_REPLAY_DATA_TRACE", "1")
    data = dict(_base_bundle())
    s = load_settings_from_bundle_dict(
        data, base_dir=Path.cwd(), print_launch_banner=False
    )
    assert s.replay_ingress_trace_payload is True


def test_load_settings_backtest_bar_replay_interval_default(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("SWR_BACKTEST_BAR_REPLAY_INTERVAL_MS", raising=False)
    data = dict(_base_bundle())
    s = load_settings_from_bundle_dict(
        data, base_dir=tmp_path, print_launch_banner=False
    )
    assert s.backtest_bar_replay_interval_ms == 100


def test_load_settings_backtest_bar_replay_interval_from_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("SWR_BACKTEST_BAR_REPLAY_INTERVAL_MS", "250")
    data = dict(_base_bundle())
    s = load_settings_from_bundle_dict(
        data, base_dir=tmp_path, print_launch_banner=False
    )
    assert s.backtest_bar_replay_interval_ms == 250


def test_load_settings_backtest_bar_replay_interval_from_bundle(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("SWR_BACKTEST_BAR_REPLAY_INTERVAL_MS", raising=False)
    data = dict(_base_bundle())
    data["backtest_bar_replay_interval_ms"] = 50
    s = load_settings_from_bundle_dict(
        data, base_dir=tmp_path, print_launch_banner=False
    )
    assert s.backtest_bar_replay_interval_ms == 50


def test_load_settings_market_data_redis_from_bundle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SWR_MARKET_DATA_REDIS_URL", raising=False)
    monkeypatch.delenv("SWR_MARKET_DATA_STREAMS", raising=False)
    monkeypatch.delenv("SWR_MARKET_DATA_STREAM_START_ID", raising=False)
    monkeypatch.delenv("SWR_MARKET_DATA_XREAD_BLOCK_MS", raising=False)
    monkeypatch.delenv("SWR_MARKET_DATA_XREAD_COUNT", raising=False)
    monkeypatch.delenv("SWR_MARKET_DATA_REALTIME_PARTITION_COUNT", raising=False)
    monkeypatch.delenv("MD_REALTIME_PARTITION_COUNT", raising=False)
    data = dict(_base_bundle())
    data["market_data_redis_url"] = "redis://localhost:6379/0"
    data["market_data_streams"] = ["trades", "quotes"]
    data["market_data_stream_start_id"] = "0-0"
    data["market_data_xread_block_ms"] = 2500
    data["market_data_xread_count"] = 50
    s = load_settings_from_bundle_dict(
        data, base_dir=tmp_path, print_launch_banner=False
    )
    assert s.market_data_redis_url == "redis://localhost:6379/0"
    assert s.market_data_feeds == ("trades", "quotes")
    assert s.market_data_stream_start_id == "0-0"
    assert s.market_data_xread_block_ms == 2500
    assert s.market_data_xread_count == 50
    assert s.market_data_realtime_partition_count == 128


def test_load_settings_market_data_realtime_partition_count_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SWR_MARKET_DATA_REALTIME_PARTITION_COUNT", raising=False)
    monkeypatch.delenv("MD_REALTIME_PARTITION_COUNT", raising=False)
    monkeypatch.setenv("SWR_MARKET_DATA_REALTIME_PARTITION_COUNT", "64")
    data = dict(_base_bundle())
    s = load_settings_from_bundle_dict(
        data, base_dir=tmp_path, print_launch_banner=False
    )
    assert s.market_data_realtime_partition_count == 64


def test_load_settings_market_data_realtime_partition_count_md_env_alias(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SWR_MARKET_DATA_REALTIME_PARTITION_COUNT", raising=False)
    monkeypatch.setenv("MD_REALTIME_PARTITION_COUNT", "32")
    data = dict(_base_bundle())
    s = load_settings_from_bundle_dict(
        data, base_dir=tmp_path, print_launch_banner=False
    )
    assert s.market_data_realtime_partition_count == 32


def test_load_settings_market_data_streams_env_overrides_bundle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SWR_MARKET_DATA_STREAMS", "bars,trades")
    data = dict(_base_bundle())
    data["market_data_streams"] = ["quotes"]
    s = load_settings_from_bundle_dict(
        data, base_dir=Path.cwd(), print_launch_banner=False
    )
    assert s.market_data_feeds == ("bars", "trades")


def test_load_settings_parameters_bar_timeframe_and_data_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SWR_MARKET_DATA_STREAMS", raising=False)
    data = dict(_base_bundle())
    data["parameters"] = {
        "symbol": "AAPL",
        "bar_timeframe": "1min",
        "data_source": ["trades", "bars"],
    }
    s = load_settings_from_bundle_dict(
        data, base_dir=tmp_path, print_launch_banner=False
    )
    assert s.replay_bar_timeframe == "1min"
    assert s.market_data_feeds == ("trades", "bars")


def test_load_settings_heartbeat_interval_from_env_and_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SWR_HEARTBEAT_INTERVAL_SECONDS", "12")
    data = dict(_base_bundle())
    data["heartbeat_interval_seconds"] = 99
    s = load_settings_from_bundle_dict(
        data, base_dir=tmp_path, print_launch_banner=False
    )
    assert s.heartbeat_interval_seconds == 12.0

    monkeypatch.delenv("SWR_HEARTBEAT_INTERVAL_SECONDS", raising=False)
    s2 = load_settings_from_bundle_dict(
        data, base_dir=tmp_path, print_launch_banner=False
    )
    assert s2.heartbeat_interval_seconds == 99.0


def test_load_settings_market_data_consumer_group_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SWR_MARKET_DATA_REDIS_USE_CONSUMER_GROUP", "false")
    monkeypatch.setenv("SWR_MARKET_DATA_CONSUMER_GROUP_PREFIX", "my-group")
    monkeypatch.setenv("SWR_MARKET_DATA_CONSUMER_NAME_PREFIX", "my-worker")
    data = dict(_base_bundle())
    s = load_settings_from_bundle_dict(
        data, base_dir=tmp_path, print_launch_banner=False
    )
    assert s.market_data_redis_use_consumer_group is False
    assert s.market_data_consumer_group_prefix == "my-group"
    assert s.market_data_consumer_name_prefix == "my-worker"

from __future__ import annotations

import json
import os
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from runtime.domain.launch_spec import LaunchSpec, LaunchSpecValidationError
from runtime.infrastructure.strategy_loader.strategy_bundle_loader import (
    _scalar,
    default_bundle_setting_path,
    extract_runtime_tuning,
    raw_dict_to_launch_payload,
    read_bundle_json,
)

# ---------------------------------------------------------------------------
# Connectivity (edit here). Module constants are not loaded from ``.env`` unless noted.
# ``SWR_HEARTBEAT_INTERVAL_SECONDS`` and ``SWR_HEARTBEAT_LOG_ENABLED`` are read from ``.env`` when ``SWR_LOAD_DOTENV=1``.
# ---------------------------------------------------------------------------
SWR_STRATEGY_RUNTIME_MANAGER_BASE_URL = ""
SWR_RUNTIME_MANAGER_HEARTBEAT_TIMEOUT_SECONDS = 30
SWR_RISK_GRPC_TIMEOUT_SECONDS = 3
SWR_MARKET_DATA_STREAMS = "bars"
SWR_MARKET_DATA_STREAM_TRADES = "md:stream:trades"
SWR_MARKET_DATA_STREAM_QUOTES = "md:stream:quotes"
SWR_MARKET_DATA_STREAM_BARS_1M = "md:stream:am"
SWR_MARKET_DATA_STREAM_START_ID = "$"
SWR_MARKET_DATA_XREAD_BLOCK_MS = 0
SWR_MARKET_DATA_XREAD_COUNT = 100
SWR_MARKET_DATA_REALTIME_PARTITION_COUNT = 128

# Env keys below are blocklisted in :func:`_load_worker_env_file` (see ``_DOTENV_BLOCKLIST``).
_DOTENV_BLOCKLIST: frozenset[str] = frozenset(
    {
        "SWR_STRATEGY_RUNTIME_MANAGER_BASE_URL",
        "SWR_RUNTIME_MANAGER_HEARTBEAT_TIMEOUT_SECONDS",
        "SWR_RISK_GRPC_TIMEOUT_SECONDS",
        "SWR_MARKET_DATA_STREAMS",
        "SWR_MARKET_DATA_STREAM_TRADES",
        "SWR_MARKET_DATA_STREAM_QUOTES",
        "SWR_MARKET_DATA_STREAM_BARS_1M",
        "SWR_MARKET_DATA_STREAM_START_ID",
        "SWR_MARKET_DATA_XREAD_BLOCK_MS",
        "SWR_MARKET_DATA_XREAD_COUNT",
        "SWR_MARKET_DATA_REALTIME_PARTITION_COUNT",
    }
)


def _use_module_connectivity_settings() -> bool:
    v = os.environ.get("SWR_USE_MODULE_CONNECTIVITY_SETTINGS", "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def _market_data_feeds_from_streams_constant() -> tuple[str, ...]:
    parts = [p.strip().lower() for p in SWR_MARKET_DATA_STREAMS.split(",") if p.strip()]
    allowed = frozenset({"trades", "quotes", "bars"})
    ordered: list[str] = []
    for p in parts:
        if p in allowed and p not in ordered:
            ordered.append(p)
    return tuple(ordered or ("bars",))


def _apply_module_connectivity_settings(tuning: dict[str, Any]) -> dict[str, Any]:
    if not _use_module_connectivity_settings():
        return tuning
    merged = dict(tuning)
    merged.update(
        {
            "strategy_runtime_manager_base_url": SWR_STRATEGY_RUNTIME_MANAGER_BASE_URL,
            "runtime_manager_heartbeat_timeout_seconds": float(
                SWR_RUNTIME_MANAGER_HEARTBEAT_TIMEOUT_SECONDS
            ),
            "risk_grpc_timeout_seconds": float(SWR_RISK_GRPC_TIMEOUT_SECONDS),
            "market_data_feeds": _market_data_feeds_from_streams_constant(),
            "market_data_redis_stream_trades": SWR_MARKET_DATA_STREAM_TRADES,
            "market_data_redis_stream_quotes": SWR_MARKET_DATA_STREAM_QUOTES,
            "market_data_redis_stream_bars_1m": SWR_MARKET_DATA_STREAM_BARS_1M,
            "market_data_stream_start_id": SWR_MARKET_DATA_STREAM_START_ID,
            "market_data_xread_block_ms": int(SWR_MARKET_DATA_XREAD_BLOCK_MS),
            "market_data_xread_count": int(SWR_MARKET_DATA_XREAD_COUNT),
            "market_data_realtime_partition_count": int(
                SWR_MARKET_DATA_REALTIME_PARTITION_COUNT
            ),
        }
    )
    return merged


def _grpc_target_status(target: str) -> str:
    t = target.strip()
    if not t:
        return "UNKNOWN (target_not_configured)"
    if ":" not in t:
        return "INVALID (expected_host:port)"
    host, _, port_s = t.rpartition(":")
    try:
        port = int(port_s)
    except ValueError:
        return "INVALID (port_not_integer)"
    if not host.strip():
        return "INVALID (host_missing)"
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return "UP"
    except OSError as e:
        return f"DOWN ({e})"


def _env_flag_enabled(name: str) -> bool:
    v = os.environ.get(name, "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _resolve_deployment_id(
    payload: Mapping[str, object], bundle: Mapping[str, Any]
) -> str:
    for source in (
        payload.get("deployment_id"),
        bundle.get("deployment_id"),
        os.environ.get("DEPLOYMENT_ID"),
    ):
        if isinstance(source, str):
            normalized = source.strip()
            if normalized:
                return normalized
    return ""


@dataclass(frozen=True, slots=True)
class Settings:
    launch_spec: LaunchSpec
    launch_payload: Mapping[str, object]
    work_root: Path
    #: Directory used to resolve relative ``artifact_uri`` paths (e.g. ``strategy_bundle/…``).
    bundle_resolve_base_dir: Path
    #: Absolute root for ``registry-local:///…`` artifact URIs (strategy-registry local backend).
    artifact_local_base_path: Path | None
    state_journal_enabled: bool
    state_journal_sqlite_path: Path
    state_journal_txt_path: Path
    strategy_runtime_manager_base_url: str
    runtime_manager_heartbeat_timeout_seconds: float
    #: When True, print control-plane JSON lines for SRM heartbeat/status posts.
    heartbeat_log_enabled: bool
    #: Wall-clock interval (seconds) between periodic SRM status heartbeats. Env: ``SWR_HEARTBEAT_INTERVAL_SECONDS``.
    heartbeat_interval_seconds: float
    deployment_id: str
    worker_control_http_bind: str
    #: ``host:port`` for Risk Service order-intent gRPC (``risk_worker.proto`` / ``OrderIntentService``).
    #: SDK order intents are sent **only** here (all runtime modes). Env: ``SWR_RISK_GRPC_TARGET``.
    risk_grpc_target: str
    risk_grpc_timeout_seconds: float
    #: Bar cadence: ``parameters.bar_timeframe`` / root ``bar_timeframe`` / ``replay_bar_timeframe``.
    replay_bar_timeframe: str
    order_intent_correlation_id: str
    disable_order_intent_grpc: bool
    backtest_symbol: str | None = None
    #: Redis URL for ``md:stream:*`` consumption (PAPER/LIVE). Empty disables the feed.
    market_data_redis_url: str = ""
    #: Redis URL for portfolio-ledger ``portfolio:update:*`` Pub/Sub (PAPER/LIVE).
    #: Empty falls back to ``market_data_redis_url`` at feed start.
    portfolio_update_redis_url: str = ""
    #: When False, do not subscribe to portfolio balance updates.
    portfolio_update_enabled: bool = True
    #: Channel prefix for PLS publishes (physical key ``{prefix}:{partition}``).
    portfolio_update_channel_prefix: str = "portfolio:update"
    #: Subscribed feeds: ``trades``, ``quotes``, ``bars`` (1m aggregate ``md:stream:am``).
    #: Source: env / root ``market_data_streams`` / ``parameters.data_source`` (PAPER/LIVE).
    market_data_feeds: tuple[str, ...] = ("bars",)
    #: Initial ``XREAD`` id per stream (``$`` = only new entries after worker start).
    market_data_stream_start_id: str = "$"
    #: ``XREAD`` / ``XREADGROUP`` ``BLOCK`` milliseconds; ``0`` = Redis ``BLOCK 0`` (same as
    #: ``redis-cli XREAD BLOCK 0 STREAMS md:stream:am:{partition} $``).
    market_data_xread_block_ms: int = 0
    market_data_xread_count: int = 100
    #: Redis stream key **prefixes** (market-data-service); override via env / bundle.
    #: Physical keys are ``{prefix}:{p}`` for partition ``p`` (see ``market_data_realtime_partition_count``).
    market_data_redis_stream_trades: str = "md:stream:trades"
    market_data_redis_stream_quotes: str = "md:stream:quotes"
    market_data_redis_stream_bars_1m: str = "md:stream:am"
    #: Partition count for ``md:stream:*:{p}`` (must match market-data-service).
    #: Env: ``SWR_MARKET_DATA_REALTIME_PARTITION_COUNT`` or ``MD_REALTIME_PARTITION_COUNT``; bundle: ``market_data_realtime_partition_count``.
    market_data_realtime_partition_count: int = 128
    #: When True, use ``XREADGROUP``/``XACK`` (opt-in; bundle or ``SWR_MARKET_DATA_REDIS_USE_CONSUMER_GROUP``).
    market_data_redis_use_consumer_group: bool = False
    #: Prefix for the Redis consumer group name (combined with deployment/runtime id).
    market_data_consumer_group_prefix: str = "strategy-worker-runtime"
    #: Prefix for the Redis consumer name (combined with pid and random suffix).
    market_data_consumer_name_prefix: str = "strategy-worker-runtime-worker"


def _settings_from_prepared(
    *,
    bundle_path: Path,
    data: dict[str, Any],
    base_dir: Path,
    print_launch_banner: bool = True,
    connectivity_env_only: bool = False,
    allow_default_localhost_manager: bool = True,
    spec_source_label: str | None = None,
) -> Settings:
    try:
        payload, work_root_str, digest_json, computed_digest = raw_dict_to_launch_payload(
            data, base_dir=base_dir
        )
    except ValueError as exc:
        message = str(exc)
        if "deployment_id" in message:
            raise LaunchSpecValidationError(
                reason="launch_spec_invalid",
                field_errors={"deployment_id": "required_field_missing"},
            ) from exc
        raise
    tuning = extract_runtime_tuning(
        data,
        connectivity_env_only=connectivity_env_only,
        allow_default_localhost_manager=allow_default_localhost_manager,
    )
    tuning = _apply_module_connectivity_settings(tuning)

    if print_launch_banner:
        launch_attempt = payload.get("launch_attempt", 1)
        la_int = (
            int(launch_attempt)
            if isinstance(launch_attempt, int)
            else int(str(launch_attempt) or "1")
        )
        worker_identity = (
            f"{payload.get('runtime_id', '')}:"
            f"{payload.get('strategy_version_id', '')}:"
            f"{la_int}"
        )
        cfg = {
            "account_id": str(payload.get("account_id") or ""),
            "event_name": "worker_configuration_validated",
            "launch_attempt": la_int,
            "level": "INFO",
            "local_phase": "INITIALIZING",
            "mode": str(payload.get("mode") or ""),
            "runtime_id": str(payload.get("runtime_id") or ""),
            "strategy_version_id": str(payload.get("strategy_version_id") or ""),
            "tenant_id": str(payload.get("tenant_id") or ""),
            "worker_identity": worker_identity,
        }
        print("Launching strategy_worker_runtime from strategy bundle...")
        print(f"Launch spec source: {spec_source_label or bundle_path}")
        if digest_json.strip() and computed_digest and digest_json != computed_digest:
            print(
                "Warning: Provided artifact_digest does not match local computed digest. "
                "Continuing launch; runtime validation is authoritative.",
                flush=True,
            )
            print(
                f"Warning: provided={digest_json} computed={computed_digest}",
                flush=True,
            )
        print("Launch metadata loaded.")
        print(f"Using artifact uri: {payload.get('artifact_uri', '')}")
        print(f"Using artifact digest: {payload.get('artifact_digest', '')}")
        srm_http = str(tuning.get("strategy_runtime_manager_base_url", "")).strip()
        if srm_http:
            print(f"strategy-runtime-manager HTTP base URL: {srm_http}")
        srm_http_only = str(tuning.get("strategy_runtime_manager_base_url", "")).strip()
        if srm_http_only:
            print(
                f"strategy-runtime-manager signals: HTTP only ({srm_http_only})",
            )
        risk_t = str(tuning.get("risk_grpc_target", "")).strip()
        if risk_t:
            print(f"Risk Service order-intent gRPC target: {risk_t}")
            print(f"Risk Service gRPC TCP status: {_grpc_target_status(risk_t)}")
        else:
            print(
                "Risk Service order-intent gRPC: SWR_RISK_GRPC_TARGET not configured.",
                flush=True,
            )
        print("----------------------------Internal Message-------------")
        print(json.dumps(cfg, separators=(",", ":"), ensure_ascii=True))
        print("worker runtime status: STARTING", flush=True)

    launch_spec = LaunchSpec.from_payload(payload)
    work_root = Path(work_root_str).resolve()

    sqlite_s = str(tuning["state_journal_sqlite"]).strip()
    txt_s = str(tuning["state_journal_txt"]).strip()
    state_journal_sqlite_path = (
        Path(sqlite_s).resolve() if sqlite_s else work_root / "state_journal.sqlite"
    )
    state_journal_txt_path = (
        Path(txt_s).resolve() if txt_s else work_root / "state_journal.txt"
    )

    backtest_symbol: str | None = None
    params_raw = data.get("parameters")
    if isinstance(params_raw, dict):
        raw_symbol = params_raw.get("symbol")
        if isinstance(raw_symbol, str):
            sym = raw_symbol.strip()
            backtest_symbol = sym or None

    return Settings(
        launch_spec=launch_spec,
        launch_payload=dict(payload),
        work_root=work_root,
        bundle_resolve_base_dir=base_dir.resolve(),
        artifact_local_base_path=(
            Path(str(tuning["artifact_local_base_path"]).strip()).resolve()
            if str(tuning.get("artifact_local_base_path", "")).strip()
            else None
        ),
        state_journal_enabled=not tuning["state_journal_disable"],
        state_journal_sqlite_path=state_journal_sqlite_path,
        state_journal_txt_path=state_journal_txt_path,
        strategy_runtime_manager_base_url=str(
            tuning["strategy_runtime_manager_base_url"]
        ),
        runtime_manager_heartbeat_timeout_seconds=float(
            SWR_RUNTIME_MANAGER_HEARTBEAT_TIMEOUT_SECONDS
        ),
        heartbeat_log_enabled=bool(tuning["heartbeat_log_enabled"]),
        heartbeat_interval_seconds=float(tuning["heartbeat_interval_seconds"]),
        deployment_id=_resolve_deployment_id(dict(payload), data),
        worker_control_http_bind=str(tuning["worker_control_http_bind"]),
        risk_grpc_target=str(tuning["risk_grpc_target"]),
        risk_grpc_timeout_seconds=float(tuning["risk_grpc_timeout_seconds"]),
        replay_bar_timeframe=str(tuning["replay_bar_timeframe"]),
        order_intent_correlation_id=str(tuning.get("order_intent_correlation_id", "")),
        disable_order_intent_grpc=bool(tuning["disable_order_intent_grpc"]),
        backtest_symbol=backtest_symbol,
        market_data_redis_url=str(tuning["market_data_redis_url"]),
        portfolio_update_redis_url=str(tuning["portfolio_update_redis_url"]),
        portfolio_update_enabled=bool(tuning["portfolio_update_enabled"]),
        portfolio_update_channel_prefix=str(tuning["portfolio_update_channel_prefix"]),
        market_data_feeds=tuple(tuning["market_data_feeds"]),
        market_data_stream_start_id=str(tuning["market_data_stream_start_id"]),
        market_data_xread_block_ms=int(tuning["market_data_xread_block_ms"]),
        market_data_xread_count=int(tuning["market_data_xread_count"]),
        market_data_redis_stream_trades=str(tuning["market_data_redis_stream_trades"]),
        market_data_redis_stream_quotes=str(tuning["market_data_redis_stream_quotes"]),
        market_data_redis_stream_bars_1m=str(
            tuning["market_data_redis_stream_bars_1m"]
        ),
        market_data_realtime_partition_count=max(
            1,
            min(65535, int(tuning["market_data_realtime_partition_count"])),
        ),
        market_data_redis_use_consumer_group=bool(
            tuning["market_data_redis_use_consumer_group"]
        ),
        market_data_consumer_group_prefix=str(
            tuning["market_data_consumer_group_prefix"]
        ),
        market_data_consumer_name_prefix=str(
            tuning["market_data_consumer_name_prefix"]
        ),
    )


SWR_LOAD_DOTENV_ENV = "SWR_LOAD_DOTENV"


def _dotenv_loading_enabled() -> bool:
    """SRM/Kubernetes inject env vars; ``.env`` is opt-in for local dev only."""
    return _env_flag_enabled(SWR_LOAD_DOTENV_ENV)


def _load_worker_env_file(cwd: Path) -> None:
    """Load ``<cwd>/.env`` when :envvar:`SWR_LOAD_DOTENV` is enabled (local dev only).

      Keys in :data:`_DOTENV_BLOCKLIST` are never read from the file. Existing process
    env (e.g. SRM-injected) always wins over the file.
    """
    if not _dotenv_loading_enabled():
        return
    path = cwd / ".env"
    if not path.is_file():
        return
    blocklist = (
        _DOTENV_BLOCKLIST if _use_module_connectivity_settings() else frozenset()
    )
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in blocklist:
            continue
        value = value.strip().strip('"').strip("'")
        if key in os.environ:
            continue
        os.environ[key] = value


def load_settings(
    *,
    bundle_path: Path | None = None,
    base_dir: Path | None = None,
    print_launch_banner: bool = True,
) -> Settings:
    """
    Load worker configuration.

    Launch identity (first match wins):

    1. ``bundle_path=`` when passed (tests): read that JSON file directly.
    2. When ``STRATEGY_DEPLOYMENT_SERVICE_BASE_URL`` and ``DEPLOYMENT_ID`` are both set:
       ``GET .../deployments/{id}/runtime-context`` from strategy-deployment-service,
       merged with ``RUNTIME_ID`` / ``SWR_RUNTIME_ID`` and ``SWR_STRATEGY_VERSION_ID`` from the environment.
    3. Otherwise ``<cwd>/strategy_bundle/setting.json`` when that file exists: same bundle shape
       as explicit ``bundle_path`` (local dev).

    Connectivity listed at the top of this module is applied from those constants (not ``.env``).
    Set ``SWR_USE_MODULE_CONNECTIVITY_SETTINGS=0`` to take connectivity from env/bundle only (tests).

    Configuration env is read from the process environment (SRM/Kubernetes injection in
    production). Optional ``<cwd>/.env`` is loaded only when ``SWR_LOAD_DOTENV=1`` (local dev).
    """
    from runtime.infrastructure.strategy_loader.deployment_runtime_context_bootstrap import (
        DEPLOYMENT_ID_ENV,
        STRATEGY_DEPLOYMENT_SERVICE_BASE_URL_ENV,
        fetch_bundle_from_deployment_runtime_context,
    )

    cwd = base_dir if base_dir is not None else Path.cwd()
    _load_worker_env_file(cwd)

    if bundle_path is not None:
        if not bundle_path.is_file():
            raise LaunchSpecValidationError(
                reason="launch_spec_invalid",
                field_errors={
                    "setting_path": f"strategy_bundle_setting_not_found:{bundle_path}"
                },
            )
        data = read_bundle_json(bundle_path)
        return _settings_from_prepared(
            bundle_path=bundle_path,
            data=data,
            base_dir=cwd,
            print_launch_banner=print_launch_banner,
            connectivity_env_only=False,
            allow_default_localhost_manager=True,
        )

    base_url = os.environ.get(STRATEGY_DEPLOYMENT_SERVICE_BASE_URL_ENV, "").strip()
    dep_id = os.environ.get(DEPLOYMENT_ID_ENV, "").strip()
    if base_url and dep_id:
        data = fetch_bundle_from_deployment_runtime_context(
            base_url=base_url,
            deployment_id=dep_id,
        )
        bp = default_bundle_setting_path(cwd=cwd)
        return _settings_from_prepared(
            bundle_path=bp,
            data=data,
            base_dir=cwd,
            print_launch_banner=print_launch_banner,
            connectivity_env_only=False,
            allow_default_localhost_manager=True,
            spec_source_label=(
                f"<{STRATEGY_DEPLOYMENT_SERVICE_BASE_URL_ENV}>/internal/v1/deployments/"
                f"{dep_id}/runtime-context"
            ),
        )

    default_setting = default_bundle_setting_path(cwd=cwd)
    if default_setting.is_file():
        data = read_bundle_json(default_setting)
        return _settings_from_prepared(
            bundle_path=default_setting,
            data=data,
            base_dir=cwd,
            print_launch_banner=print_launch_banner,
            connectivity_env_only=False,
            allow_default_localhost_manager=True,
            spec_source_label=str(default_setting),
        )

    raise LaunchSpecValidationError(
        reason="launch_identity_source_missing",
        field_errors={
            "bootstrap": (
                "Set STRATEGY_DEPLOYMENT_SERVICE_BASE_URL and DEPLOYMENT_ID for SDS "
                "runtime-context, or provide strategy_bundle/setting.json under cwd "
                "(or pass bundle_path in tests)."
            ),
        },
    )


def load_settings_from_bundle_dict(
    data: Mapping[str, Any],
    *,
    base_dir: Path | None = None,
    print_launch_banner: bool = False,
    bundle_path_for_messages: Path | None = None,
) -> Settings:
    """Construct :class:`Settings` from an in-memory bundle (tests and tooling)."""
    if not isinstance(data, dict):
        raise TypeError("bundle data must be a dict")
    cwd = base_dir if base_dir is not None else Path.cwd()
    _load_worker_env_file(cwd)
    bp = (
        bundle_path_for_messages
        if bundle_path_for_messages is not None
        else default_bundle_setting_path(cwd=cwd)
    )
    return _settings_from_prepared(
        bundle_path=bp,
        data=dict(data),
        base_dir=cwd,
        print_launch_banner=print_launch_banner,
        connectivity_env_only=False,
        allow_default_localhost_manager=True,
    )

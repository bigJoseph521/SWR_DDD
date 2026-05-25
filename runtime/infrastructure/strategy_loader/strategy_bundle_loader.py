"""Parse ``strategy_bundle/setting.json`` into launch payload + runtime tuning fields."""

from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse
from urllib.request import url2pathname

from runtime.infrastructure.sdk.strategy_params_yaml import (
    discover_params_yaml_adjacent_to_module_file,
)


def default_bundle_setting_path(*, cwd: Path | None = None) -> Path:
    root = cwd or Path.cwd()
    return (root / "strategy_bundle" / "setting.json").resolve()


def read_bundle_json(path: Path) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8")
    if not raw.strip():
        raise ValueError(f"Setting file is empty: {path}")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError(f"Setting file must be a JSON object: {path}")
    return data


def _scalar(data: dict[str, Any], key: str, default: Any = "") -> Any:
    v = data.get(key)
    if v is None:
        return default
    if isinstance(v, str):
        return v.strip()
    return v


def _bundle_order_intent_correlation_id(data: Mapping[str, Any]) -> str:
    """Read order-intent correlation id from bundle (supports pre-Risk-only legacy field name)."""
    primary = str(_scalar(data, "order_intent_correlation_id", ""))
    if primary:
        return primary
    legacy_key = "o" + "ms_correlation_id"
    return str(_scalar(data, legacy_key, ""))


def _truthy(v: object) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "yes")
    return False


def _artifact_reference(reference: str, base_dir: Path) -> tuple[str, Path | None]:
    raw = reference.strip()
    if not raw:
        return "", None
    if raw.startswith("file://"):
        parsed = urlparse(raw)
        local = Path(url2pathname(parsed.path))
        if not local.is_absolute():
            local = (base_dir / local).resolve()
        return raw, local
    p = Path(raw)
    if p.is_absolute():
        ap = p.resolve()
        return ap.as_uri(), ap
    candidate = (base_dir / raw).resolve()
    if candidate.exists():
        return candidate.as_uri(), candidate
    return raw, candidate


def _tree_digest(root_path: Path, algorithm: str = "sha256") -> str:
    root = root_path.resolve()
    h = hashlib.new(algorithm)
    if root.is_file():
        # Match :class:`ArtifactVerifier` scoped digest: primary file + adjacent parameter YAML.
        parent = root.parent
        files = [root]
        params_yaml = discover_params_yaml_adjacent_to_module_file(root)
        if params_yaml is not None:
            files.append(params_yaml.resolve())
        root_prefix = str(parent) + os.sep
        for file in sorted(files, key=lambda p: p.name):
            rel = str(file)[len(root_prefix) :].replace("\\", "/")
            h.update(rel.encode("utf-8"))
            h.update(b"\x00")
            h.update(file.read_bytes())
        return h.hexdigest()
    if root.is_dir():
        files = sorted(
            (p for p in root.rglob("*") if p.is_file()), key=lambda x: str(x)
        )
        rs = str(root)
        root_prefix = rs if rs.endswith(os.sep) else rs + os.sep
        for file in files:
            rel = str(file)[len(root_prefix) :].replace("\\", "/")
            h.update(rel.encode("utf-8"))
            h.update(b"\x00")
            h.update(file.read_bytes())
        return h.hexdigest()
    return h.hexdigest()


def _sanitize_work_root_dir_name(name: str) -> str:
    """Filesystem-safe directory name for per-worker local state (SQLite, artifacts)."""
    return re.sub(r"[^\w.\-]", "_", name.strip())


def _resolve_deployment_id_for_work_root(
    data: Mapping[str, Any], payload: Mapping[str, object]
) -> str:
    """Resolve ``deployment_id`` from bundle, launch payload, or ``DEPLOYMENT_ID`` env."""
    for source in (
        payload.get("deployment_id"),
        data.get("deployment_id"),
        os.environ.get("DEPLOYMENT_ID"),
    ):
        if isinstance(source, str):
            normalized = source.strip()
            if normalized:
                return normalized
    return ""


def _default_work_root_dir_name(
    data: Mapping[str, Any], payload: Mapping[str, object]
) -> str:
    """
    Default work_root directory under ``base_dir``.

    Uses ``deployment_id`` only (bundle, payload, or ``DEPLOYMENT_ID`` env).
    """
    deployment_id = _resolve_deployment_id_for_work_root(data, payload)
    if not deployment_id:
        raise ValueError(
            "deployment_id is required when work_root is not set in the strategy bundle"
        )
    sanitized = _sanitize_work_root_dir_name(deployment_id)
    if not sanitized:
        raise ValueError(
            f"deployment_id {deployment_id!r} is not a valid work_root directory name"
        )
    return sanitized


def raw_dict_to_launch_payload(
    data: dict[str, Any],
    *,
    base_dir: Path,
) -> tuple[dict[str, object], str, str, str | None]:
    """
    Returns ``(launch_payload, work_root, digest_from_json, computed_digest_or_none)`` for
    :class:`LaunchSpec.from_payload` and :class:`Settings`.
    """
    payload: dict[str, object] = {}

    def put_if_key(key: str) -> None:
        if key not in data:
            return
        v = data[key]
        if isinstance(v, str):
            payload[key] = v.strip()
        elif v is None:
            payload[key] = ""
        else:
            payload[key] = v

    for k in (
        "runtime_id",
        "tenant_id",
        "strategy_version_id",
        "mode",
        "entrypoint",
        "deployment_id",
    ):
        put_if_key(k)

    if "launch_attempt" in data:
        lar = data["launch_attempt"]
        if isinstance(lar, int):
            payload["launch_attempt"] = lar
        else:
            s = str(lar).strip() or "1"
            try:
                payload["launch_attempt"] = int(s)
            except ValueError:
                payload["launch_attempt"] = s

    if "account_id" in data:
        payload["account_id"] = str(data.get("account_id") or "").strip()
    if "trader_id" in data:
        payload["trader_id"] = str(data.get("trader_id") or "").strip()

    digest_from_json = (
        str(data.get("artifact_digest") or "").strip()
        if "artifact_digest" in data
        else ""
    )
    computed_digest: str | None = None
    if "artifact_uri" in data:
        artifact_uri_raw = str(data.get("artifact_uri") or "").strip()
        resolved_uri, artifact_path = _artifact_reference(artifact_uri_raw, base_dir)
        if artifact_path is not None and artifact_path.exists():
            if artifact_path.is_file() and artifact_path.suffix.lower() == ".zip":
                computed_digest = (
                    f"sha256:{hashlib.sha256(artifact_path.read_bytes()).hexdigest()}"
                )
            else:
                computed_digest = f"sha256:{_tree_digest(artifact_path)}"
        resolved_digest = digest_from_json
        if computed_digest and not resolved_digest.strip():
            resolved_digest = computed_digest
        payload["artifact_uri"] = resolved_uri
        payload["artifact_digest"] = resolved_digest

    job_primary = str(_scalar(data, "job_id", ""))
    resolved_job = job_primary.strip()
    if resolved_job:
        payload["job_id"] = resolved_job

    corr_primary = str(_scalar(data, "correlation_id", ""))
    if corr_primary.strip():
        payload["correlation_id"] = corr_primary.strip()

    params_root = data.get("parameters")
    if isinstance(params_root, dict):
        payload["parameters"] = dict(params_root)

    sp_root = data.get("strategy_params")
    if isinstance(sp_root, dict) and sp_root:
        payload["strategy_params"] = dict(sp_root)

    ic_root = data.get("initial_cash")
    if isinstance(ic_root, dict):
        amount = ic_root.get("amount")
        currency = ic_root.get("currency")
        if amount is not None and currency is not None:
            payload["initial_cash"] = {
                "amount": str(amount).strip(),
                "currency": str(currency).strip(),
            }

    ts_start = str(_scalar(data, "ts_start", "")) if "ts_start" in data else ""
    ts_end = str(_scalar(data, "ts_end", "")) if "ts_end" in data else ""
    params = params_root if isinstance(params_root, dict) else data.get("parameters")
    if isinstance(params, dict):
        if (not ts_start.strip() or "ts_start" not in data) and params.get(
            "ts_start"
        ) is not None:
            ts_start = str(params["ts_start"])
        if (not ts_end.strip() or "ts_end" not in data) and params.get(
            "ts_end"
        ) is not None:
            ts_end = str(params["ts_end"])
    if ts_start.strip():
        payload["ts_start"] = ts_start
    if ts_end.strip():
        payload["ts_end"] = ts_end

    sym_primary = bundle_primary_symbol(data)
    if sym_primary:
        payload["symbol"] = sym_primary

    work_root_json = str(_scalar(data, "work_root", ""))
    if work_root_json.strip():
        work_root = str((base_dir / work_root_json).resolve())
    else:
        work_root = str(
            (base_dir / _default_work_root_dir_name(data, payload)).resolve()
        )

    return payload, work_root, digest_from_json, computed_digest


def _feed_parts_from_bundle(data: dict[str, Any]) -> list[str]:
    """Resolve feed names from root ``market_data_streams`` or ``parameters.data_source``."""
    raw = data.get("market_data_streams")
    if isinstance(raw, list) and raw:
        return [str(x).strip().lower() for x in raw if str(x).strip()]
    if isinstance(raw, str) and raw.strip():
        return [p.strip().lower() for p in raw.split(",") if p.strip()]
    params = data.get("parameters")
    if isinstance(params, dict):
        ds = params.get("data_source")
        if isinstance(ds, list) and ds:
            return [str(x).strip().lower() for x in ds if str(x).strip()]
        if isinstance(ds, str) and ds.strip():
            return [p.strip().lower() for p in ds.split(",") if p.strip()]
    return []


def parse_market_data_feeds(data: dict[str, Any]) -> tuple[str, ...]:
    """
    Which Redis stream groups to follow: ``trades``, ``quotes``, ``bars`` (1m AM).

    Precedence: env ``SWR_MARKET_DATA_STREAMS`` → root ``market_data_streams`` →
    ``parameters.data_source`` (PAPER/LIVE). Default: ``("bars",)``.
    """
    env = str(os.environ.get("SWR_MARKET_DATA_STREAMS", "")).strip()
    if env:
        parts = [p.strip().lower() for p in env.split(",") if p.strip()]
    else:
        parts = _feed_parts_from_bundle(data)
    allowed = frozenset({"trades", "quotes", "bars"})
    ordered: list[str] = []
    for p in parts:
        if p in allowed and p not in ordered:
            ordered.append(p)
    if not ordered:
        ordered = ["bars"]
    return tuple(ordered)


def bundle_primary_symbol(data: dict[str, Any]) -> str | None:
    """Ticker from ``parameters.symbol`` only (``strategy_bundle/setting.json`` shape)."""
    params = data.get("parameters")
    if not isinstance(params, dict):
        return None
    sym = params.get("symbol")
    if isinstance(sym, str) and sym.strip():
        return sym.strip()
    return None


def effective_bar_timeframe(data: dict[str, Any]) -> str:
    """
    Bar cadence for backtest replay and for PAPER/LIVE bar tick labeling / stream choice.

    Precedence: ``parameters.bar_timeframe`` → root ``bar_timeframe`` → ``replay_bar_timeframe`` → ``1m``.
    """
    params = data.get("parameters")
    if isinstance(params, dict):
        bt = params.get("bar_timeframe")
        if isinstance(bt, str) and bt.strip():
            return bt.strip()
    root_bt = str(_scalar(data, "bar_timeframe", "")).strip()
    if root_bt:
        return root_bt
    legacy = str(_scalar(data, "replay_bar_timeframe", "")).strip()
    return legacy or "1m"


def extract_runtime_tuning(
    data: dict[str, Any],
    *,
    connectivity_env_only: bool = False,
    allow_default_localhost_manager: bool = True,
) -> dict[str, Any]:
    """Optional keys from bundle and/or ``SWR_*`` environment (snake_case).

    When ``connectivity_env_only`` is True, stable connectivity keys (gRPC targets, Redis URLs,
    market-data stream names, backtest Redis knobs, etc.) are read **only** from environment
    variables with code defaults — not from ``data``. Process-local binds (worker control, replay
    ingress, journals) still read from ``data`` / defaults.
    """

    def _env_or_scalar(env_key: str, data_key: str, default: Any) -> Any:
        if connectivity_env_only:
            ev = os.environ.get(env_key)
            if ev is not None and str(ev).strip() != "":
                return str(ev).strip() if isinstance(default, str) else ev
            return default
        ev = os.environ.get(env_key)
        if ev is not None and str(ev).strip() != "":
            return str(ev).strip() if isinstance(default, str) else ev
        return _scalar(data, data_key, default)

    def _float_env_or_bundle(env_key: str, data_key: str, default: float) -> float:
        raw_ev = os.environ.get(env_key)
        if raw_ev is not None and str(raw_ev).strip():
            try:
                return float(str(raw_ev).strip())
            except ValueError:
                pass
        if connectivity_env_only:
            return default
        try:
            return float(_scalar(data, data_key, default))
        except (TypeError, ValueError):
            return default

    strategy_runtime_manager_base_url = str(
        _env_or_scalar(
            "STRATEGY_RUNTIME_MANAGER_BASE_URL",
            "strategy_runtime_manager_base_url",
            "",
        )
        or _env_or_scalar(
            "SWR_STRATEGY_RUNTIME_MANAGER_BASE_URL",
            "strategy_runtime_manager_base_url",
            "",
        )
    ).strip()
    heartbeat_interval_seconds = _float_env_or_bundle(
        "SWR_HEARTBEAT_INTERVAL_SECONDS",
        "heartbeat_interval_seconds",
        5.0,
    )
    heartbeat_interval_seconds = max(0.001, heartbeat_interval_seconds)
    heartbeat_log_enabled = _truthy(os.environ.get("SWR_HEARTBEAT_LOG_ENABLED", ""))
    if not heartbeat_log_enabled and not connectivity_env_only:
        heartbeat_log_enabled = _truthy(data.get("heartbeat_log_enabled", False))
    risk_grpc_target = str(
        _env_or_scalar("SWR_RISK_GRPC_TARGET", "risk_grpc_target", "")
    ).strip()
    risk_grpc_timeout_seconds = _float_env_or_bundle(
        "SWR_RISK_GRPC_TIMEOUT_SECONDS",
        "risk_grpc_timeout_seconds",
        3.0,
    )

    market_data_redis_url = str(
        _env_or_scalar("SWR_MARKET_DATA_REDIS_URL", "market_data_redis_url", "")
    ).strip()
    portfolio_update_redis_url = str(
        os.environ.get("SWR_PORTFOLIO_UPDATE_REDIS_URL", "").strip()
        or (
            ""
            if connectivity_env_only
            else _scalar(data, "portfolio_update_redis_url", "")
        )
    ).strip()
    if not portfolio_update_redis_url:
        portfolio_update_redis_url = market_data_redis_url
    pu_enabled_raw = (
        str(os.environ.get("SWR_PORTFOLIO_UPDATE_ENABLED", "")).strip().lower()
    )
    if pu_enabled_raw in ("0", "false", "no", "off"):
        portfolio_update_enabled = False
    elif pu_enabled_raw in ("1", "true", "yes", "on"):
        portfolio_update_enabled = True
    else:
        portfolio_update_enabled = (
            False
            if connectivity_env_only
            else _truthy(data.get("portfolio_update_enabled", True))
        )
    portfolio_update_channel_prefix = str(
        os.environ.get("SWR_PORTFOLIO_UPDATE_CHANNEL_PREFIX", "").strip()
        or (
            ""
            if connectivity_env_only
            else _scalar(
                data,
                "portfolio_update_channel_prefix",
                "portfolio:update",
            )
        )
        or "portfolio:update"
    ).strip()
    market_data_stream_start_id = (
        str(
            os.environ.get("SWR_MARKET_DATA_STREAM_START_ID")
            or (
                ""
                if connectivity_env_only
                else _scalar(data, "market_data_stream_start_id", "$")
            )
        ).strip()
        or "$"
    )
    try:
        market_data_xread_block_ms = int(
            os.environ.get("SWR_MARKET_DATA_XREAD_BLOCK_MS")
            or (
                ""
                if connectivity_env_only
                else _scalar(data, "market_data_xread_block_ms", 0)
            )
            or 0
        )
    except (TypeError, ValueError):
        market_data_xread_block_ms = 0
    try:
        market_data_xread_count = int(
            os.environ.get("SWR_MARKET_DATA_XREAD_COUNT")
            or (
                ""
                if connectivity_env_only
                else _scalar(data, "market_data_xread_count", 100)
            )
            or 100
        )
    except (TypeError, ValueError):
        market_data_xread_count = 100
    market_data_feeds = parse_market_data_feeds(data)

    def _md_redis_key(env_name: str, data_key: str, default: str) -> str:
        ev = str(os.environ.get(env_name, "")).strip()
        if ev:
            return ev
        if connectivity_env_only:
            return str(default).strip()
        return str(_scalar(data, data_key, default)).strip() or default

    try:
        market_data_realtime_partition_count = int(
            os.environ.get("SWR_MARKET_DATA_REALTIME_PARTITION_COUNT", "").strip()
            or os.environ.get("MD_REALTIME_PARTITION_COUNT", "").strip()
            or (
                ""
                if connectivity_env_only
                else _scalar(data, "market_data_realtime_partition_count", 128)
            )
            or 128
        )
    except (TypeError, ValueError):
        market_data_realtime_partition_count = 128
    market_data_realtime_partition_count = max(
        1, min(65535, market_data_realtime_partition_count)
    )
    cg_raw = (
        str(os.environ.get("SWR_MARKET_DATA_REDIS_USE_CONSUMER_GROUP", ""))
        .strip()
        .lower()
    )
    if cg_raw in ("0", "false", "no", "off"):
        market_data_redis_use_consumer_group = False
    elif cg_raw in ("1", "true", "yes", "on"):
        market_data_redis_use_consumer_group = True
    else:
        market_data_redis_use_consumer_group = (
            False
            if connectivity_env_only
            else _truthy(data.get("market_data_redis_use_consumer_group", False))
        )
    market_data_consumer_group_prefix = str(
        os.environ.get("SWR_MARKET_DATA_CONSUMER_GROUP_PREFIX", "").strip()
        or (
            ""
            if connectivity_env_only
            else _scalar(
                data, "market_data_consumer_group_prefix", "strategy-worker-runtime"
            )
        )
        or "strategy-worker-runtime"
    ).strip()
    market_data_consumer_name_prefix = str(
        os.environ.get("SWR_MARKET_DATA_CONSUMER_NAME_PREFIX", "").strip()
        or (
            ""
            if connectivity_env_only
            else _scalar(
                data,
                "market_data_consumer_name_prefix",
                "strategy-worker-runtime-worker",
            )
        )
        or "strategy-worker-runtime-worker"
    ).strip()
    _ = allow_default_localhost_manager

    artifact_local_base_path = str(
        _env_or_scalar("SWR_ARTIFACT_LOCAL_BASE_PATH", "artifact_local_base_path", "")
    ).strip()
    if not artifact_local_base_path:
        alt = os.environ.get("ARTIFACT_LOCAL_BASE_PATH")
        if alt is not None and str(alt).strip():
            artifact_local_base_path = str(alt).strip()

    return {
        "strategy_runtime_manager_base_url": strategy_runtime_manager_base_url,
        "heartbeat_interval_seconds": heartbeat_interval_seconds,
        "heartbeat_log_enabled": heartbeat_log_enabled,
        "worker_control_http_bind": str(
            _env_or_scalar(
                "SWR_WORKER_CONTROL_HTTP_BIND",
                "worker_control_http_bind",
                "127.0.0.1:8091",
            )
        ),
        "risk_grpc_target": risk_grpc_target,
        "risk_grpc_timeout_seconds": risk_grpc_timeout_seconds,
        "replay_bar_timeframe": effective_bar_timeframe(data),
        "order_intent_correlation_id": _bundle_order_intent_correlation_id(data),
        "disable_order_intent_grpc": _truthy(
            data.get("disable_order_intent_grpc", False)
        ),
        "state_journal_disable": _truthy(data.get("state_journal_disable", False)),
        "state_journal_sqlite": str(_scalar(data, "state_journal_sqlite", "")),
        "state_journal_txt": str(_scalar(data, "state_journal_txt", "")),
        "market_data_redis_url": market_data_redis_url,
        "portfolio_update_redis_url": portfolio_update_redis_url,
        "portfolio_update_enabled": portfolio_update_enabled,
        "portfolio_update_channel_prefix": portfolio_update_channel_prefix,
        "market_data_feeds": market_data_feeds,
        "market_data_stream_start_id": market_data_stream_start_id,
        "market_data_xread_block_ms": max(0, market_data_xread_block_ms),
        "market_data_xread_count": max(1, market_data_xread_count),
        "market_data_redis_stream_trades": _md_redis_key(
            "SWR_MARKET_DATA_STREAM_TRADES",
            "market_data_redis_stream_trades",
            "md:stream:trades",
        ),
        "market_data_redis_stream_quotes": _md_redis_key(
            "SWR_MARKET_DATA_STREAM_QUOTES",
            "market_data_redis_stream_quotes",
            "md:stream:quotes",
        ),
        "market_data_redis_stream_bars_1m": _md_redis_key(
            "SWR_MARKET_DATA_STREAM_BARS_1M",
            "market_data_redis_stream_bars_1m",
            "md:stream:am",
        ),
        "market_data_realtime_partition_count": market_data_realtime_partition_count,
        "market_data_redis_use_consumer_group": market_data_redis_use_consumer_group,
        "market_data_consumer_group_prefix": market_data_consumer_group_prefix,
        "market_data_consumer_name_prefix": market_data_consumer_name_prefix,
        "artifact_local_base_path": artifact_local_base_path,
    }

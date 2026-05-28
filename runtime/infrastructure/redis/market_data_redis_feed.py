"""Consume market-data-service Redis streams via XREAD / XREADGROUP (PAPER/LIVE)."""

from __future__ import annotations

import json
import os
import re
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from runtime.domain.bar_timeframe import redis_md_stream_am_supports_timeframe
from runtime.infrastructure.clock.epoch_time import utc_datetime_to_epoch_millis
from runtime.infrastructure.redis.market_data_partition import market_data_partition

# Default Redis keys (market-data-service); override via env / bundle / ``MarketDataRedisKeys``.
STREAM_TRADES = "md:stream:trades"
STREAM_QUOTES = "md:stream:quotes"
STREAM_AM_1M = "md:stream:am"

# market-data-service publishes lossy ticks on Pub/Sub ``md:realtime:*:{p}``; durable ingress is
# Redis Streams ``md:stream:*:{p}``. Misconfigured workers sometimes copy the Pub/Sub prefix into
# ``SWR_MARKET_DATA_STREAM_*`` — normalize those to stream keys for ``XREAD``.
_PUBSUB_PREFIX_TO_STREAM: tuple[tuple[str, str], ...] = (
    ("md:realtime:trades", STREAM_TRADES),
    ("md:realtime:quotes", STREAM_QUOTES),
    ("md:realtime:bars", STREAM_AM_1M),
)


def _normalize_stream_prefix_for_xread(raw: str, *, setting: str) -> str:
    s = str(raw).strip()
    if not s:
        return s
    low = s.casefold()
    for pub, stream in _PUBSUB_PREFIX_TO_STREAM:
        if low == pub.casefold():
            print(
                "[market-data-redis] "
                f"{setting}={raw!r} is a Pub/Sub channel prefix, not a Redis Stream. "
                f"Using {stream!r} for XREAD (same layout as market-data-service XADD). "
                "Set SWR_MARKET_DATA_STREAM_* / bundle keys to md:stream:* to silence this.",
                flush=True,
            )
            return stream
    return s


@dataclass(frozen=True, slots=True)
class MarketDataRedisKeys:
    """Redis stream key prefixes (market-data-service ``XADD`` targets)."""

    stream_trades: str = STREAM_TRADES
    stream_quotes: str = STREAM_QUOTES
    stream_bars_1m: str = STREAM_AM_1M


def default_market_data_redis_keys() -> MarketDataRedisKeys:
    return MarketDataRedisKeys()


def market_data_redis_keys_from_settings(obj: Any) -> MarketDataRedisKeys:
    """Build :class:`MarketDataRedisKeys` from :class:`~runtime.infrastructure.config.settings.Settings` (or duck-typed)."""
    d = default_market_data_redis_keys()

    def _one(attr: str, fallback: str) -> str:
        raw = getattr(obj, attr, None)
        s = str(raw).strip() if raw is not None else ""
        return s or fallback

    st_tr = _normalize_stream_prefix_for_xread(
        _one("market_data_redis_stream_trades", d.stream_trades),
        setting="market_data_redis_stream_trades",
    )
    st_q = _normalize_stream_prefix_for_xread(
        _one("market_data_redis_stream_quotes", d.stream_quotes),
        setting="market_data_redis_stream_quotes",
    )
    st_b = _normalize_stream_prefix_for_xread(
        _one("market_data_redis_stream_bars_1m", d.stream_bars_1m),
        setting="market_data_redis_stream_bars_1m",
    )
    return MarketDataRedisKeys(
        stream_trades=st_tr,
        stream_quotes=st_q,
        stream_bars_1m=st_b,
    )


def sanitize_redis_stream_consumer_token(raw: str, *, max_len: int = 80) -> str:
    """
    Sanitize a fragment used in Redis consumer group / consumer names.

    Allows ASCII letters, digits, hyphen, underscore; other characters become ``_``.
    """
    out: list[str] = []
    for ch in (raw or "").strip():
        if ch.isascii() and (ch.isalnum() or ch in "-_"):
            out.append(ch)
        elif ch.isspace() or ch in ".:/@":
            out.append("_")
        else:
            out.append("_")
    s = "".join(out).strip("_") or "swr"
    return s[:max_len]


def sanitize_redis_url_for_log(redis_url: str) -> str:
    """
    Return a copy of ``redis_url`` safe to print (password redacted).

    Handles ``redis://user:pass@host:port/db`` and ``redis://:pass@host:port/db``.
    """
    s = str(redis_url or "").strip()
    if not s:
        return ""
    out, n = re.subn(
        r"(://)([^/?#:@]+):([^/?#@]+)(@)",
        r"\1\2:***\4",
        s,
        count=1,
    )
    if n:
        return out
    out2, n2 = re.subn(r"(://:)([^/?#@]+)(@)", r"\1***\3", s, count=1)
    if n2:
        return out2
    return s


def redis_stream_key_prefixes_snapshot(keys: MarketDataRedisKeys) -> dict[str, str]:
    """Flat map of configured stream **prefixes** (before ``:{partition}``)."""
    return {
        "stream_trades_prefix": keys.stream_trades,
        "stream_quotes_prefix": keys.stream_quotes,
        "stream_bars_1m_prefix": keys.stream_bars_1m,
    }


def market_data_read_command_fields(
    *,
    use_consumer_group: bool,
    stream_names: Sequence[str],
    stream_start_id: str,
    block_ms: int,
    count: int,
    consumer_group_name: str = "",
    consumer_name: str = "",
) -> dict[str, Any]:
    """
    Human-oriented summary of which Redis commands the worker uses for market data ingress.

    **Not** Pub/Sub: there is no ``SUBSCRIBE`` on ``md:realtime:*``—only ``XREAD`` / ``XREADGROUP``.
    """
    n = len(stream_names)
    max_show = 16
    shown = list(stream_names[:max_show])
    sid = (stream_start_id or "$").strip() or "$"
    blk = max(0, int(block_ms))
    cnt = max(1, int(count))
    if use_consumer_group:
        cg = (consumer_group_name or "").strip()
        cn = (consumer_name or "").strip()
        streams_part = " ".join(f"{k} >" for k in shown)
        if n > max_show:
            streams_part += f" …(+{n - max_show} more streams)"
        cli = (
            f"XREADGROUP GROUP {cg} {cn} BLOCK {blk} COUNT {cnt} STREAMS {streams_part}"
        )
        return {
            "ingest_transport": "redis_streams",
            "ingest_pubsub_note": "worker does not SUBSCRIBE md:realtime:*; use stream keys below",
            "redis_primary_command": "XREADGROUP",
            "redis_secondary_commands": "XGROUP CREATE (per stream, MKSTREAM), XACK (after processing)",
            "redis_cli_equivalent": cli,
            "xread_block_ms": blk,
            "xread_count": cnt,
            "stream_count": n,
            "consumer_group": cg or None,
            "consumer_name": cn or None,
        }
    streams_list = " ".join(shown)
    ids_list = " ".join([sid] * min(n, max_show)) if shown else sid
    if n > max_show:
        streams_list += f" …(+{n - max_show} more)"
        ids_list += " …"
    cli = f"XREAD BLOCK {blk} COUNT {cnt} STREAMS {streams_list} {ids_list}"
    return {
        "ingest_transport": "redis_streams",
        "ingest_pubsub_note": "worker does not SUBSCRIBE md:realtime:*; use stream keys below",
        "redis_primary_command": "XREAD",
        "redis_secondary_commands": "none",
        "redis_cli_equivalent": cli,
        "per_stream_start_id": sid,
        "xread_block_ms": blk,
        "xread_count": cnt,
        "stream_count": n,
        "consumer_group": None,
        "consumer_name": None,
    }


def build_market_data_consumer_group_name(
    *,
    group_prefix: str,
    deployment_id: str | None,
    runtime_id: str | None,
) -> str:
    """
    One consumer group name per runtime/deployment so different workers do not steal messages.

    Prefer ``deployment_id`` when set; otherwise ``runtime_id``.
    """
    base = sanitize_redis_stream_consumer_token(group_prefix, max_len=48)
    dep = (deployment_id or "").strip()
    rid = (runtime_id or "").strip()
    suffix_raw = dep if dep else rid
    suffix = sanitize_redis_stream_consumer_token(suffix_raw, max_len=48)
    return f"{base}:{suffix}"


def _time_utc_to_epoch_ms(time_utc: str) -> int | None:
    """Parse RFC3339 ``time_utc`` from market-data JSON into epoch milliseconds."""
    raw = time_utc.strip()
    if not raw:
        return None
    text = raw.replace("Z", "+00:00")
    if text.endswith("+00") and not text.endswith("+00:00"):
        text = text[:-3] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return utc_datetime_to_epoch_millis(dt.astimezone(timezone.utc))


def resolve_stream_names(
    feeds: tuple[str, ...],
    *,
    bar_timeframe: str | None = None,
    redis_keys: MarketDataRedisKeys | None = None,
) -> list[str]:
    """
    Map feed names to configured Redis stream keys.

    The aggregate bar stream is 1m only in the default deployment; if ``bars`` is requested but
    ``bar_timeframe`` is not a 1m alias, the bars stream is omitted (trades/quotes unchanged).
    """
    keys = redis_keys or default_market_data_redis_keys()
    feed_to_stream = {
        "trades": keys.stream_trades,
        "quotes": keys.stream_quotes,
        "bars": keys.stream_bars_1m,
    }
    out: list[str] = []
    tf = (bar_timeframe or "1m").strip() or "1m"
    warned = False
    for f in feeds:
        fl = f.strip().lower()
        if fl == "bars" and not redis_md_stream_am_supports_timeframe(tf):
            if not warned:
                print(
                    f"[market-data-redis] bar_timeframe={tf!r} is not available on "
                    f"{keys.stream_bars_1m!r} (1m aggregate only); omitting bars stream",
                    flush=True,
                )
                warned = True
            continue
        key = feed_to_stream.get(fl)
        if key is not None and key not in out:
            out.append(key)
    return out


def _stream_feed_matches(stream_name: str, stream_base: str) -> bool:
    return stream_name == stream_base or stream_name.startswith(stream_base + ":")


def partitions_for_realtime_subscribe(
    *,
    partition_count: int,
    symbol_filter: set[str] | None,
) -> tuple[int, ...]:
    """
    Partitions to open for ``XREAD`` / ``XREADGROUP`` when tailing market-data-service streams.

    - No symbol filter: all ``0 .. partition_count - 1`` (full universe).
    - With filter: distinct partitions for each symbol (IEEE CRC32, same as MD).
    """
    if partition_count < 1:
        return ()
    if not symbol_filter:
        return tuple(range(partition_count))
    seen: set[int] = set()
    ordered: list[int] = []
    for sym in symbol_filter:
        p = market_data_partition(sym, partition_count)
        if p not in seen:
            seen.add(p)
            ordered.append(p)
    return tuple(sorted(ordered))


def expand_market_data_stream_keys(
    stream_bases: Sequence[str],
    *,
    partition_count: int,
    symbol_filter: set[str] | None,
) -> list[str]:
    """
    Append ``:{p}`` to each stream base so keys match market-data-service partitioned ``XADD`` targets.

    When ``partition_count < 1``, returns ``stream_bases`` unchanged (legacy / tests).
    """
    bases = list(stream_bases)
    if partition_count < 1:
        return bases
    parts = partitions_for_realtime_subscribe(
        partition_count=partition_count, symbol_filter=symbol_filter
    )
    if not parts:
        return bases
    out: list[str] = []
    for base in bases:
        for p in parts:
            out.append(f"{base}:{p}")
    return out


def bar_message_to_tick(
    obj: Mapping[str, Any],
    *,
    stream_id: str,
    field: str,
    bar_timeframe: str = "1m",
) -> dict[str, Any] | None:
    time_utc = str(obj.get("time_utc") or "").strip()
    ts_ms = _time_utc_to_epoch_ms(time_utc)
    if ts_ms is None:
        return None
    sym = str(obj.get("symbol") or field).strip()
    if not sym:
        return None
    return {
        "type": "market.bar",
        "event_type": "market.bar",
        "event_id": f"md:bar:{stream_id}:{field}",
        "symbol": sym,
        # Align with replay bridge: canonical id is the ticker, not venue numeric `instrument_id`.
        "instrument_id": sym,
        "timeframe": (bar_timeframe or "1m").strip() or "1m",
        "open": float(obj.get("open") or 0.0),
        "high": float(obj.get("high") or 0.0),
        "low": float(obj.get("low") or 0.0),
        "close": float(obj.get("close") or 0.0),
        "volume": float(obj.get("volume") or 0.0),
        "ts_ms": int(ts_ms),
        "event_time": time_utc,
    }


def trade_message_to_tick(
    obj: Mapping[str, Any], *, stream_id: str, field: str
) -> dict[str, Any] | None:
    time_utc = str(obj.get("time_utc") or "").strip()
    ts_ms = _time_utc_to_epoch_ms(time_utc)
    if ts_ms is None:
        return None
    sym = str(obj.get("symbol") or field).strip()
    if not sym:
        return None
    return {
        "type": "market.tick",
        "event_type": "market.tick",
        "event_id": f"md:trade:{stream_id}:{field}",
        "symbol": sym,
        "instrument_id": sym,
        "price": float(obj.get("price") or 0.0),
        "size": float(obj.get("size") or 0.0),
        "ts_ms": int(ts_ms),
        "event_time": time_utc,
    }


def quote_message_to_tick(
    obj: Mapping[str, Any], *, stream_id: str, field: str
) -> dict[str, Any] | None:
    time_utc = str(obj.get("time_utc") or "").strip()
    ts_ms = _time_utc_to_epoch_ms(time_utc)
    if ts_ms is None:
        return None
    sym = str(obj.get("symbol") or field).strip()
    if not sym:
        return None
    return {
        "type": "market.quote",
        "event_type": "market.quote",
        "event_id": f"md:quote:{stream_id}:{field}",
        "symbol": sym,
        "instrument_id": sym,
        "bid": float(obj.get("bid_price") or 0.0),
        "ask": float(obj.get("ask_price") or 0.0),
        "bid_size": float(obj.get("bid_size") or 0.0),
        "ask_size": float(obj.get("ask_size") or 0.0),
        "ts_ms": int(ts_ms),
        "event_time": time_utc,
    }


def stream_payload_to_tick(
    stream: str,
    redis_field: str,
    raw_json: str,
    *,
    message_id: str,
    bar_timeframe: str = "1m",
    redis_keys: MarketDataRedisKeys | None = None,
) -> dict[str, Any] | None:
    keys = redis_keys or default_market_data_redis_keys()
    try:
        obj = json.loads(raw_json)
    except json.JSONDecodeError:
        return None
    if not isinstance(obj, dict):
        return None
    if _stream_feed_matches(stream, keys.stream_bars_1m):
        return bar_message_to_tick(
            obj,
            stream_id=message_id,
            field=redis_field,
            bar_timeframe=bar_timeframe,
        )
    if _stream_feed_matches(stream, keys.stream_trades):
        return trade_message_to_tick(obj, stream_id=message_id, field=redis_field)
    if _stream_feed_matches(stream, keys.stream_quotes):
        return quote_message_to_tick(obj, stream_id=message_id, field=redis_field)
    return None


def classify_stream_payload_failure(
    stream: str,
    redis_field: str,
    raw_json: str,
    *,
    message_id: str,
    bar_timeframe: str = "1m",
    redis_keys: MarketDataRedisKeys | None = None,
) -> str | None:
    """
    When :func:`stream_payload_to_tick` returns ``None``, return a short machine-readable reason.
    """
    keys = redis_keys or default_market_data_redis_keys()
    raw = raw_json if isinstance(raw_json, str) else str(raw_json)
    if not raw.strip():
        return "empty_payload"
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        return "json_decode_error"
    if not isinstance(obj, dict):
        return "payload_not_object"
    if not (
        _stream_feed_matches(stream, keys.stream_bars_1m)
        or _stream_feed_matches(stream, keys.stream_trades)
        or _stream_feed_matches(stream, keys.stream_quotes)
    ):
        return "unknown_stream_family"
    tick = stream_payload_to_tick(
        stream,
        redis_field,
        raw,
        message_id=message_id,
        bar_timeframe=bar_timeframe,
        redis_keys=keys,
    )
    if tick is None:
        return "validation_failed_time_or_symbol"
    return None


MarketDataStreamLogFn = Callable[..., None]


def _emit_stream_log(
    log: MarketDataStreamLogFn | None,
    *,
    level: str,
    event_name: str,
    message: str,
    **fields: Any,
) -> None:
    del log, level, event_name, message, fields
    return


def _ensure_consumer_groups(
    client: Any,
    *,
    stream_names: list[str],
    group: str,
    log: MarketDataStreamLogFn | None,
) -> None:
    import redis

    sample_cmds = [f"XGROUP CREATE {sn} {group} $ MKSTREAM" for sn in stream_names[:24]]
    if len(stream_names) > 24:
        sample_cmds.append(f"(+{len(stream_names) - 24} more XGROUP CREATE …)")
    _emit_stream_log(
        log,
        level="INFO",
        event_name="market_data_stream.step_xgroup_create_plan",
        message="Ensuring Redis consumer groups exist (idempotent; BUSYGROUP ignored).",
        redis_command_family="XGROUP CREATE",
        stream_count=len(stream_names),
        consumer_group=group,
        xgroup_create_commands_sample=sample_cmds,
    )
    for sn in stream_names:
        try:
            client.xgroup_create(name=sn, groupname=group, id="$", mkstream=True)
        except redis.ResponseError as exc:
            if "BUSYGROUP" in str(exc):
                continue
            raise
    _emit_stream_log(
        log,
        level="DEBUG",
        event_name="market_data_stream.consumer_groups_ready",
        message="Redis consumer groups created or already present.",
        stream_count=len(stream_names),
        consumer_group=group,
    )


def _open_redis_client(redis_url: str) -> Any:
    import redis

    return redis.Redis.from_url(redis_url, decode_responses=True)


def _reconnect_backoff_seconds(attempt: int) -> float:
    """Simple capped exponential backoff (seconds)."""
    return min(30.0, float(2 ** min(attempt, 5)))


def _partition_stream_stdout_enabled() -> bool:
    v = os.environ.get("SWR_MARKET_DATA_SUPPRESS_TICK_STDOUT", "").strip().lower()
    return v not in ("1", "true", "yes", "on")


def _log_partition_stream_event(
    *,
    stream_name: str,
    msg_id: str,
    field_sym: str,
    strategy_symbol: str | None,
    tick: dict[str, Any] | None,
    parse_reason: str | None = None,
) -> None:
    """Log stream fields for the strategy target symbol only (when configured)."""
    if not _partition_stream_stdout_enabled():
        return
    sym = str(field_sym).strip().upper()
    traded = (strategy_symbol or "").strip().upper()
    matches = bool(traded) and sym == traded
    if traded and not matches:
        return
    line: dict[str, Any] = {
        "event": "partition_stream_entry",
        "redis_stream": stream_name,
        "redis_stream_id": msg_id,
        "symbol": sym,
        "strategy_symbol": traded or None,
        "matches_strategy_symbol": matches,
    }
    if tick is not None:
        line["event_type"] = tick.get("event_type") or tick.get("type")
        line["ts_ms"] = tick.get("ts_ms")
        line["event_time"] = tick.get("event_time")
        if tick.get("event_type") == "market.bar" or tick.get("type") == "market.bar":
            line["close"] = tick.get("close")
    if parse_reason:
        line["parse_reason"] = parse_reason
    try:
        payload = json.dumps(line, default=str, separators=(",", ":"))
    except TypeError:
        payload = repr(line)
    if len(payload) > 2000:
        payload = payload[:2000] + "…"
    print(f"[market-data-redis] {payload}", flush=True)


def run_market_data_redis_loop(
    *,
    redis_url: str,
    stream_names: list[str],
    stream_start_id: str,
    block_ms: int,
    count: int,
    strategy_symbol: str | None,
    on_tick: Callable[[dict[str, Any]], None],
    should_stop: threading.Event,
    bar_timeframe: str = "1m",
    redis_keys: MarketDataRedisKeys | None = None,
    use_consumer_group: bool = False,
    consumer_group_name: str = "",
    consumer_name: str = "",
    log: MarketDataStreamLogFn | None = None,
) -> None:
    """
    Blocking Redis Streams loop for market-data-service writes.

    - Default: ``XREAD`` (``stream_start_id`` applies per stream), equivalent to
      ``redis-cli XREAD BLOCK 0 STREAMS md:stream:am:{partition} $`` when ``block_ms=0``
      and ``stream_start_id=$``.
    - ``block_ms=0`` → Redis ``BLOCK 0`` (block until new stream entries arrive).
    - When ``use_consumer_group`` is True: ``XREADGROUP`` + ``XACK`` with idempotent ``XGROUP CREATE … $ MKSTREAM`` per stream key.

    ``stream_names`` must be the **physical** Redis keys (including ``:{p}`` when partitioned).
    """
    if not stream_names:
        return
    import redis

    keys = redis_keys or default_market_data_redis_keys()
    group = (consumer_group_name or "").strip()
    consumer = (consumer_name or "").strip()
    sid0 = (stream_start_id or "$").strip() or "$"
    _emit_stream_log(
        log,
        level="INFO",
        event_name="market_data_stream.step_ingress_plan",
        message="Market data ingress plan: Redis Streams (not Pub/Sub md:realtime:*).",
        redis_url=sanitize_redis_url_for_log(redis_url),
        physical_stream_keys=list(stream_names),
        python_redis_client="redis.Redis.from_url(<redis_url>, decode_responses=True)",
        **redis_stream_key_prefixes_snapshot(keys),
        **market_data_read_command_fields(
            use_consumer_group=use_consumer_group,
            stream_names=stream_names,
            stream_start_id=sid0,
            block_ms=block_ms,
            count=count,
            consumer_group_name=group if use_consumer_group else "",
            consumer_name=consumer if use_consumer_group else "",
        ),
    )
    if use_consumer_group:
        if not group or not consumer:
            raise ValueError(
                "consumer_group_name and consumer_name are required when use_consumer_group is True"
            )
        _run_xreadgroup_loop(
            redis_url=redis_url,
            stream_names=stream_names,
            block_ms=block_ms,
            count=count,
            strategy_symbol=strategy_symbol,
            on_tick=on_tick,
            should_stop=should_stop,
            bar_timeframe=bar_timeframe,
            redis_keys=keys,
            consumer_group=group,
            consumer_name=consumer,
            log=log,
        )
        return

    start_id = (stream_start_id or "$").strip() or "$"
    positions: dict[str, str] = {s: start_id for s in stream_names}
    client: Any = None
    reconnect_attempt = 0
    try:
        client = _open_redis_client(redis_url)
        _emit_stream_log(
            log,
            level="INFO",
            event_name="market_data_stream.step_redis_client_open",
            message="Connected to Redis for market-data stream ingress.",
            redis_url=sanitize_redis_url_for_log(redis_url),
            redis_python_api="Redis.from_url",
        )
        _emit_stream_log(
            log,
            level="INFO",
            event_name="market_data_stream.consumer_started",
            message="Redis market data stream consumer started (XREAD).",
            mode="xread",
            stream_count=len(stream_names),
            streams_sample=stream_names[:16],
            start_id=start_id,
            block_ms=block_ms,
        )
        _emit_stream_log(
            log,
            level="INFO",
            event_name="market_data_stream.contract_keys",
            message="Stream key prefixes aligned with market-data-service.",
            stream_trades=keys.stream_trades,
            stream_quotes=keys.stream_quotes,
            stream_bars_1m=keys.stream_bars_1m,
        )
        _emit_stream_log(
            log,
            level="INFO",
            event_name="market_data_stream.step_begin_xread_loop",
            message="Entering blocking loop: repeated client.xread(...) until stop event.",
            redis_python_api="Redis.xread",
            redis_primary_command="XREAD",
            block_ms=max(0, int(block_ms)),
            count=max(1, int(count)),
            stream_count=len(stream_names),
        )
        while not should_stop.is_set():
            block = max(0, int(block_ms))
            try:
                resp = client.xread(positions, count=max(1, int(count)), block=block)
                reconnect_attempt = 0
            except (redis.ConnectionError, redis.TimeoutError, OSError) as exc:
                _emit_stream_log(
                    log,
                    level="WARNING",
                    event_name="market_data_stream.step_reconnect",
                    message="XREAD failed; reconnecting with backoff (will retry client.xread).",
                    redis_python_api="Redis.xread",
                    error_type=type(exc).__name__,
                    error=str(exc),
                    backoff_seconds=_reconnect_backoff_seconds(reconnect_attempt),
                )
                if should_stop.wait(_reconnect_backoff_seconds(reconnect_attempt)):
                    break
                reconnect_attempt += 1
                try:
                    client.close()
                except Exception:
                    pass
                client = _open_redis_client(redis_url)
                continue

            if not resp:
                continue

            for stream_name, messages in resp:
                last_id_for_stream = positions.get(stream_name, start_id)
                for msg_id, fields in messages:
                    last_id_for_stream = msg_id
                    if not isinstance(fields, dict):
                        continue
                    for field_sym, payload in fields.items():
                        _process_one_stream_field(
                            stream_name=str(stream_name),
                            msg_id=str(msg_id),
                            field_sym=str(field_sym),
                            payload=str(payload),
                            strategy_symbol=strategy_symbol,
                            bar_timeframe=bar_timeframe,
                            redis_keys=keys,
                            on_tick=on_tick,
                            log=log,
                            on_parse_failure=None,
                        )
                positions[str(stream_name)] = last_id_for_stream
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass
        _emit_stream_log(
            log,
            level="INFO",
            event_name="market_data_stream.consumer_stopped",
            message="Redis market data stream consumer stopped.",
            mode="xread",
        )


def _process_one_stream_field(
    *,
    stream_name: str,
    msg_id: str,
    field_sym: str,
    payload: str,
    strategy_symbol: str | None,
    bar_timeframe: str,
    redis_keys: MarketDataRedisKeys,
    on_tick: Callable[[dict[str, Any]], None],
    log: MarketDataStreamLogFn | None,
    on_parse_failure: Callable[[], None] | None,
) -> None:
    sym_key = str(field_sym).strip().upper()
    traded = (strategy_symbol or "").strip().upper()
    matches_strategy = not traded or sym_key == traded
    tick = stream_payload_to_tick(
        stream_name,
        str(field_sym),
        payload,
        message_id=str(msg_id),
        bar_timeframe=bar_timeframe,
        redis_keys=redis_keys,
    )
    if tick is None:
        reason = classify_stream_payload_failure(
            stream_name,
            str(field_sym),
            payload,
            message_id=str(msg_id),
            bar_timeframe=bar_timeframe,
            redis_keys=redis_keys,
        )
        _log_partition_stream_event(
            stream_name=stream_name,
            msg_id=msg_id,
            field_sym=field_sym,
            strategy_symbol=strategy_symbol,
            tick=None,
            parse_reason=reason or "unknown",
        )
        _emit_stream_log(
            log,
            level="WARNING",
            event_name="market_data_stream.parse_failed",
            message="Dropped malformed market-data stream field.",
            redis_stream_key=stream_name,
            redis_stream_id=msg_id,
            redis_field=field_sym,
            reason=reason or "unknown",
            symbol=sym_key or None,
        )
        if on_parse_failure is not None:
            on_parse_failure()
        return
    _log_partition_stream_event(
        stream_name=stream_name,
        msg_id=msg_id,
        field_sym=field_sym,
        strategy_symbol=strategy_symbol,
        tick=tick,
    )
    if not matches_strategy:
        if on_parse_failure is not None:
            on_parse_failure()
        return
    _emit_stream_log(
        log,
        level="DEBUG",
        event_name="market_data_stream.event_received",
        message="Market data stream entry accepted.",
        redis_stream_key=stream_name,
        redis_stream_id=msg_id,
        event_type=str(tick.get("event_type") or tick.get("type") or ""),
        symbol=str(tick.get("symbol") or ""),
    )
    _emit_stream_log(
        log,
        level="INFO",
        event_name="market_data_stream.strategy_execution_triggered",
        message="Dispatching market data tick to strategy adapter.",
        symbol=str(tick.get("symbol") or ""),
        event_type=str(tick.get("event_type") or tick.get("type") or ""),
    )
    try:
        on_tick(tick)
    except Exception as exc:
        _emit_stream_log(
            log,
            level="WARNING",
            event_name="market_data_stream.strategy_dispatch_exception",
            message="Strategy tick dispatch raised; consumer continues.",
            redis_stream_key=stream_name,
            redis_stream_id=msg_id,
            symbol=str(tick.get("symbol") or ""),
            error_type=type(exc).__name__,
            error=str(exc),
        )
    finally:
        if on_parse_failure is not None:
            on_parse_failure()


def _run_xreadgroup_loop(
    *,
    redis_url: str,
    stream_names: list[str],
    block_ms: int,
    count: int,
    strategy_symbol: str | None,
    on_tick: Callable[[dict[str, Any]], None],
    should_stop: threading.Event,
    bar_timeframe: str,
    redis_keys: MarketDataRedisKeys,
    consumer_group: str,
    consumer_name: str,
    log: MarketDataStreamLogFn | None,
) -> None:
    import redis

    streams_arg = {s: ">" for s in stream_names}
    client: Any = None
    reconnect_attempt = 0
    try:
        client = _open_redis_client(redis_url)
        _emit_stream_log(
            log,
            level="INFO",
            event_name="market_data_stream.step_redis_client_open",
            message="Connected to Redis for market-data stream ingress.",
            redis_url=sanitize_redis_url_for_log(redis_url),
            redis_python_api="Redis.from_url",
        )
        _ensure_consumer_groups(
            client, stream_names=stream_names, group=consumer_group, log=log
        )
        _emit_stream_log(
            log,
            level="INFO",
            event_name="market_data_stream.consumer_started",
            message="Redis market data stream consumer started (XREADGROUP).",
            mode="xreadgroup",
            stream_count=len(stream_names),
            streams_sample=stream_names[:16],
            consumer_group=consumer_group,
            consumer_name=consumer_name,
            block_ms=block_ms,
        )
        _emit_stream_log(
            log,
            level="INFO",
            event_name="market_data_stream.contract_keys",
            message="Stream key prefixes aligned with market-data-service.",
            stream_trades=redis_keys.stream_trades,
            stream_quotes=redis_keys.stream_quotes,
            stream_bars_1m=redis_keys.stream_bars_1m,
        )
        _emit_stream_log(
            log,
            level="INFO",
            event_name="market_data_stream.step_begin_xreadgroup_loop",
            message="Entering blocking loop: repeated client.xreadgroup(...) until stop event.",
            redis_python_api="Redis.xreadgroup",
            redis_primary_command="XREADGROUP",
            block_ms=max(0, int(block_ms)),
            count=max(1, int(count)),
            stream_count=len(stream_names),
            consumer_group=consumer_group,
            consumer_name=consumer_name,
        )
        while not should_stop.is_set():
            block = max(0, int(block_ms))
            try:
                resp = client.xreadgroup(
                    groupname=consumer_group,
                    consumername=consumer_name,
                    streams=streams_arg,
                    count=max(1, int(count)),
                    block=block,
                )
                reconnect_attempt = 0
            except (redis.ConnectionError, redis.TimeoutError, OSError) as exc:
                _emit_stream_log(
                    log,
                    level="WARNING",
                    event_name="market_data_stream.step_reconnect",
                    message="XREADGROUP failed; reconnecting with backoff (will retry client.xreadgroup).",
                    redis_python_api="Redis.xreadgroup",
                    error_type=type(exc).__name__,
                    error=str(exc),
                    backoff_seconds=_reconnect_backoff_seconds(reconnect_attempt),
                )
                if should_stop.wait(_reconnect_backoff_seconds(reconnect_attempt)):
                    break
                reconnect_attempt += 1
                try:
                    client.close()
                except Exception:
                    pass
                client = _open_redis_client(redis_url)
                _ensure_consumer_groups(
                    client,
                    stream_names=stream_names,
                    group=consumer_group,
                    log=log,
                )
                continue

            if not resp:
                continue

            for stream_name, messages in resp:
                for msg_id, fields in messages:
                    if not isinstance(fields, dict):
                        client.xack(stream_name, consumer_group, msg_id)
                        continue
                    if not fields:
                        client.xack(stream_name, consumer_group, msg_id)
                        continue

                    pending = [str(k) for k in fields]

                    def _make_ack_one(_f: str) -> Callable[[], None]:
                        def _inner() -> None:
                            nonlocal pending
                            try:
                                pending.remove(_f)
                            except ValueError:
                                return
                            if not pending:
                                client.xack(stream_name, consumer_group, msg_id)

                        return _inner

                    for field_sym in fields:
                        payload = fields[field_sym]
                        _process_one_stream_field(
                            stream_name=str(stream_name),
                            msg_id=str(msg_id),
                            field_sym=str(field_sym),
                            payload=str(payload),
                            strategy_symbol=strategy_symbol,
                            bar_timeframe=bar_timeframe,
                            redis_keys=redis_keys,
                            on_tick=on_tick,
                            log=log,
                            on_parse_failure=_make_ack_one(str(field_sym)),
                        )
                    # If all fields were ignored (filtered), pending may still be full — ack anyway.
                    if pending:
                        client.xack(stream_name, consumer_group, msg_id)
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass
        _emit_stream_log(
            log,
            level="INFO",
            event_name="market_data_stream.consumer_stopped",
            message="Redis market data stream consumer stopped.",
            mode="xreadgroup",
            consumer_group=consumer_group,
        )

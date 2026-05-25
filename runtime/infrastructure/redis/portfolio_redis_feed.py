"""Subscribe to portfolio-ledger-service ``portfolio:update:{partition}`` Pub/Sub."""

from __future__ import annotations

import threading
from collections.abc import Callable, Mapping
from typing import Any

from runtime.application.event_handling.portfolio_update_contract import (
    DEFAULT_CHANNEL_PREFIX as DEFAULT_PORTFOLIO_UPDATE_CHANNEL_PREFIX,
    portfolio_update_channel_name as portfolio_update_channel,
    portfolio_update_partition,
)
from runtime.infrastructure.redis.market_data_redis_feed import (
    _open_redis_client,
    _reconnect_backoff_seconds,
    sanitize_redis_url_for_log,
)
from runtime.infrastructure.redis.redis_portfolio_update_adapter import (
    parse_portfolio_balance_message,
)


def run_portfolio_update_pubsub_loop(
    *,
    redis_url: str,
    channel: str,
    expected_job_id: str,
    on_update: Callable[[Mapping[str, Any]], None],
    should_stop: threading.Event,
    log: Callable[..., None] | None = None,
) -> None:
    """
    Blocking Redis Pub/Sub loop for one ``portfolio:update:{partition}`` channel.

    Invokes ``on_update`` only when ``msg["job_id"]`` equals ``expected_job_id`` (trimmed).
    """
    import redis

    job_id = str(expected_job_id or "").strip()
    if not job_id:
        _log(
            log,
            level="WARNING",
            event_name="portfolio_update_feed.skipped",
            message="portfolio update feed requires launch job_id",
        )
        return

    reconnect_attempt = 0
    client: Any = None
    pubsub: Any = None
    try:
        while not should_stop.is_set():
            try:
                if client is None:
                    client = _open_redis_client(redis_url)
                    pubsub = client.pubsub(ignore_subscribe_messages=True)
                    pubsub.subscribe(channel)
                    _log(
                        log,
                        level="INFO",
                        event_name="portfolio_update_feed.subscribed",
                        message="Subscribed to portfolio balance Pub/Sub channel.",
                        fields={
                            "redis_url": sanitize_redis_url_for_log(redis_url),
                            "channel": channel,
                            "job_id": job_id,
                        },
                    )
                    reconnect_attempt = 0

                message = pubsub.get_message(timeout=1.0)
                if message is None:
                    continue
                if message.get("type") != "message":
                    continue
                parsed = parse_portfolio_balance_message(message.get("data"))
                if parsed is None:
                    continue
                if str(parsed.get("job_id") or "").strip() != job_id:
                    continue
                on_update(parsed)
            except (redis.ConnectionError, redis.TimeoutError, OSError) as exc:
                _log(
                    log,
                    level="WARNING",
                    event_name="portfolio_update_feed.reconnect",
                    message="Portfolio Pub/Sub read failed; reconnecting.",
                    fields={
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                        "backoff_seconds": _reconnect_backoff_seconds(
                            reconnect_attempt
                        ),
                    },
                )
                if should_stop.wait(_reconnect_backoff_seconds(reconnect_attempt)):
                    break
                reconnect_attempt += 1
                if pubsub is not None:
                    try:
                        pubsub.close()
                    except Exception:
                        pass
                    pubsub = None
                if client is not None:
                    try:
                        client.close()
                    except Exception:
                        pass
                    client = None
    finally:
        if pubsub is not None:
            try:
                pubsub.close()
            except Exception:
                pass
        if client is not None:
            try:
                client.close()
            except Exception:
                pass


def _log(
    log: Callable[..., None] | None,
    *,
    level: str,
    event_name: str,
    message: str,
    fields: Mapping[str, Any] | None = None,
) -> None:
    if log is None:
        return
    log(
        level=level,
        event_name=event_name,
        message=message,
        fields=dict(fields or {}),
    )

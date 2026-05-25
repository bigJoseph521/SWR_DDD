from __future__ import annotations

import json
import threading
from collections.abc import Callable, Mapping
from typing import Any

from runtime.application.event_handling.portfolio_update_contract import (
    DEFAULT_CHANNEL_PREFIX,
    parse_portfolio_update_message,
    portfolio_update_channel_name,
    portfolio_update_partition,
)
from runtime.domain.model.normalized_events import PortfolioUpdatedEvent
from runtime.infrastructure.redis.market_data_redis_feed import (
    _open_redis_client,
    _reconnect_backoff_seconds,
    sanitize_redis_url_for_log,
)


def parse_portfolio_balance_message(
    raw: bytes | str | None,
) -> dict[str, Any] | None:
    """Parse PLS wire JSON; returns ``None`` on empty or invalid payloads."""
    if raw is None:
        return None
    if isinstance(raw, bytes):
        text = raw.decode("utf-8", errors="replace").strip()
    else:
        text = str(raw).strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, dict):
        return None
    return parsed


class RedisPortfolioUpdateAdapter:
    """
    PAPER/LIVE portfolio update input adapter.

    Subscribes to ``portfolio:update:{partition}`` where
    ``partition = zlib.crc32(job_id.encode("utf-8")) % partition_count``.
    """

    def __init__(
        self,
        *,
        redis_url: str,
        job_id: str,
        partition_count: int,
        channel_prefix: str = DEFAULT_CHANNEL_PREFIX,
        on_event: Callable[[PortfolioUpdatedEvent], None],
        on_rejected: Callable[[str, Mapping[str, Any]], None] | None = None,
        log: Callable[..., None] | None = None,
    ) -> None:
        self._redis_url = redis_url
        self._job_id = str(job_id or "").strip()
        self._partition_count = int(partition_count)
        self._channel_prefix = channel_prefix
        self._on_event = on_event
        self._on_rejected = on_rejected
        self._log = log
        self._partition = portfolio_update_partition(
            self._job_id, self._partition_count
        )
        self._channel = portfolio_update_channel_name(
            self._channel_prefix, partition=self._partition
        )

    @property
    def channel(self) -> str:
        return self._channel

    @property
    def partition(self) -> int:
        return self._partition

    def handle_parsed_message(self, payload: Mapping[str, Any]) -> None:
        """Validate, filter by job_id, and emit normalized event (no strategy hooks)."""
        result = parse_portfolio_update_message(
            payload, expected_job_id=self._job_id
        )
        if result.event is not None:
            self._on_event(result.event)
            return
        if result.reject_reason and self._on_rejected is not None:
            self._on_rejected(result.reject_reason, dict(payload))

    def handle_wire_message(self, raw: bytes | str | None) -> None:
        parsed = parse_portfolio_balance_message(raw)
        if parsed is None:
            if self._on_rejected is not None:
                self._on_rejected("invalid_json", {})
            return
        self.handle_parsed_message(parsed)

    def run_pubsub_loop(self, *, should_stop: threading.Event) -> None:
        """Blocking Redis Pub/Sub loop for :attr:`channel`."""
        import redis

        if not self._job_id:
            self._emit_log(
                level="WARNING",
                event_name="portfolio_update_adapter.skipped",
                message="portfolio update adapter requires launch job_id",
            )
            return

        reconnect_attempt = 0
        client: Any = None
        pubsub: Any = None
        try:
            while not should_stop.is_set():
                try:
                    if client is None:
                        client = _open_redis_client(self._redis_url)
                        pubsub = client.pubsub(ignore_subscribe_messages=True)
                        pubsub.subscribe(self._channel)
                        self._emit_log(
                            level="INFO",
                            event_name="portfolio_update_adapter.subscribed",
                            message="Subscribed to portfolio balance Pub/Sub channel.",
                            fields={
                                "redis_url": sanitize_redis_url_for_log(
                                    self._redis_url
                                ),
                                "channel": self._channel,
                                "job_id": self._job_id,
                                "partition": self._partition,
                            },
                        )
                        reconnect_attempt = 0

                    message = pubsub.get_message(timeout=1.0)
                    if message is None:
                        continue
                    if message.get("type") != "message":
                        continue
                    try:
                        self.handle_wire_message(message.get("data"))
                    except Exception as exc:
                        self._emit_log(
                            level="WARNING",
                            event_name="portfolio_update_adapter.message_error",
                            message="Portfolio message handling failed; continuing.",
                            fields={
                                "error_type": type(exc).__name__,
                                "error": str(exc),
                            },
                        )
                except (redis.ConnectionError, redis.TimeoutError, OSError) as exc:
                    self._emit_log(
                        level="WARNING",
                        event_name="portfolio_update_adapter.reconnect",
                        message="Portfolio Pub/Sub read failed; reconnecting.",
                        fields={
                            "error_type": type(exc).__name__,
                            "error": str(exc),
                            "backoff_seconds": _reconnect_backoff_seconds(
                                reconnect_attempt
                            ),
                        },
                    )
                    if should_stop.wait(
                        _reconnect_backoff_seconds(reconnect_attempt)
                    ):
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

    def _emit_log(
        self,
        *,
        level: str,
        event_name: str,
        message: str,
        fields: Mapping[str, Any] | None = None,
    ) -> None:
        if self._log is None:
            return
        self._log(
            level=level,
            event_name=event_name,
            message=message,
            fields=dict(fields or {}),
        )

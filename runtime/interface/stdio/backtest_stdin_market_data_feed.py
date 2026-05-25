"""
BACKTEST stdin market-data feeder skeleton.

Reads JSON lines from an injected input stream (default ``sys.stdin``). Parsing and full
payload contract are not finalized; only minimal ``type`` routing is implemented.
"""

from __future__ import annotations

import json
import sys
import threading
from collections.abc import Callable, Mapping
from typing import Any, TextIO

from runtime.interface.stdio.stdin_protocol import (
    ALLOWED_STDIN_MESSAGE_TYPES,
    StdinMessageType,
)


class BacktestStdinMarketDataFeed:
    """Pull/read loop over stdin for BACKTEST market-data ingress (skeleton)."""

    def __init__(self, input_stream: TextIO | None = None) -> None:
        self._input = input_stream if input_stream is not None else sys.stdin
        self._on_tick: Callable[[Mapping[str, Any]], None] | None = None
        self._on_end_of_stream: Callable[[], None] | None = None
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._started = False

    def start(
        self,
        on_tick: Callable[[Mapping[str, Any]], None],
        *,
        on_end_of_stream: Callable[[], None] | None = None,
    ) -> None:
        if self._started:
            return
        self._on_tick = on_tick
        self._on_end_of_stream = on_end_of_stream
        self._stop_event.clear()
        self._started = True
        self._thread = threading.Thread(
            target=self._read_loop,
            name="swr-backtest-stdin-market-data-feed",
            daemon=False,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=60.0)
        self._started = False
        self._thread = None

    def _read_loop(self) -> None:
        on_tick = self._on_tick
        on_end_of_stream = self._on_end_of_stream
        if on_tick is None:
            return
        while not self._stop_event.is_set():
            line = self._input.readline()
            if not line:
                if on_end_of_stream is not None:
                    on_end_of_stream()
                break
            stripped = line.strip()
            if not stripped:
                continue
            try:
                message = json.loads(stripped)
            except json.JSONDecodeError:
                print(
                    "[backtest-stdin] skipped non-JSON line",
                    file=sys.stderr,
                    flush=True,
                )
                continue
            if not isinstance(message, dict):
                continue
            msg_type = str(message.get("type") or "").strip()
            if msg_type == StdinMessageType.MARKET_DATA.value:
                payload = message.get("payload")
                if isinstance(payload, Mapping):
                    on_tick(dict(payload))
                elif isinstance(message.get("data"), Mapping):
                    on_tick(dict(message["data"]))
                else:
                    tick = {k: v for k, v in message.items() if k != "type"}
                    if tick:
                        on_tick(tick)
            elif msg_type == StdinMessageType.CONTROL_STOP.value:
                self._stop_event.set()
                break
            elif msg_type == StdinMessageType.END_OF_STREAM.value:
                if on_end_of_stream is not None:
                    on_end_of_stream()
                break
            elif msg_type and msg_type not in ALLOWED_STDIN_MESSAGE_TYPES:
                print(
                    f"[backtest-stdin] unsupported message type: {msg_type!r}",
                    file=sys.stderr,
                    flush=True,
                )

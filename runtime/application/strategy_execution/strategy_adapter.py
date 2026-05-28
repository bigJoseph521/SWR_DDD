from __future__ import annotations

import inspect
from typing import Any, Callable, Mapping

from alphovex_sdk.strategy.base import Strategy as SdkStrategy
from runtime.application.strategy_execution.event_mapper import (
    EventMapper,
    MarketBarEvent,
    MarketQuoteEvent,
    MarketTickEvent,
)
from runtime.application.strategy_execution.event_mapper import (
    TimerEvent as WireTimerEvent,
)
from runtime.application.ports.backtest_sdk_bridge_port import BacktestSdkBridgePort
from runtime.application.strategy_execution.sdk_hook_utils import (
    replay_sdk_hook_overridden,
)
from runtime.application.strategy_execution.strategy_error_boundary import (
    StrategyCallResult,
    StrategyErrorBoundary,
)


def _legacy_positional_params(
    handler: Callable[..., object],
) -> list[inspect.Parameter]:
    try:
        sig = inspect.signature(handler)
    except (TypeError, ValueError):
        return []
    return [
        p
        for p in sig.parameters.values()
        if p.kind
        in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
    ]


class StrategyAdapter:
    def __init__(
        self,
        strategy: object,
        *,
        mapper: EventMapper | None = None,
        boundary: StrategyErrorBoundary | None = None,
        backtest_sdk_bridge: BacktestSdkBridgePort | None = None,
        runtime_sdk_bridge: BacktestSdkBridgePort | None = None,
        replay_sdk_bridge: BacktestSdkBridgePort | None = None,
    ) -> None:
        self._strategy = strategy
        self._mapper = mapper or EventMapper()
        self._boundary = boundary or StrategyErrorBoundary()
        self._runtime_sdk_bridge = (
            backtest_sdk_bridge or runtime_sdk_bridge or replay_sdk_bridge
        )
        # Legacy attribute name used by lifecycle wiring.
        self._backtest_sdk_bridge = self._runtime_sdk_bridge
        self._lifecycle_started = False

    def bind_and_start(self) -> StrategyCallResult[Any]:
        """
        Bind SDK strategy context once, run platform ``initialize()`` (``on_init``), then ``on_start``.

        Idempotent: subsequent calls return success with ``skipped`` diagnostics.
        """
        if self._lifecycle_started:
            return StrategyCallResult(
                ok=True,
                value=None,
                error_code=None,
                reason_code=None,
                exception=None,
                diagnostics={
                    "operation": "strategy.bind_and_start",
                    "skipped": "already_started",
                },
            )

        strategy = self._strategy
        bridge = self._runtime_sdk_bridge

        if isinstance(strategy, SdkStrategy):
            if bridge is not None:
                bind_ctx = getattr(strategy, "_bind_context", None)
                if callable(bind_ctx):
                    bind_ctx(bridge.strategy_context)
            init_result = self._boundary.call(
                "strategy.initialize", strategy.initialize
            )
            if not init_result.ok:
                return init_result
        else:
            legacy_init = self._legacy_initialize()
            if not legacy_init.ok:
                return legacy_init

        on_start = getattr(strategy, "on_start", None)
        if callable(on_start):
            start_result = self._boundary.call("strategy.on_start", on_start)
            if not start_result.ok:
                return start_result

        self._lifecycle_started = True
        return StrategyCallResult(
            ok=True,
            value=None,
            error_code=None,
            reason_code=None,
            exception=None,
            diagnostics={"operation": "strategy.bind_and_start"},
        )

    def _legacy_initialize(self) -> StrategyCallResult[object]:
        hook = getattr(self._strategy, "initialize", None)
        if not callable(hook):
            return StrategyCallResult(
                ok=True,
                value=None,
                error_code=None,
                reason_code=None,
                exception=None,
                diagnostics={"operation": "strategy.initialize", "skipped": "no_hook"},
            )
        params = _legacy_positional_params(hook)
        if len(params) >= 1:
            # TODO(STP-1012): remove legacy initialize(context) shim once artifacts use SDK lifecycle only.
            ctx = (
                self._runtime_sdk_bridge.strategy_context
                if self._runtime_sdk_bridge is not None
                else None
            )
            return self._boundary.call("strategy.initialize", hook, ctx)
        return self._boundary.call("strategy.initialize", hook)

    def initialize(self, *args: object, **kwargs: object) -> StrategyCallResult[object]:
        hook = getattr(self._strategy, "initialize", None)
        if not callable(hook):
            return StrategyCallResult(
                ok=True,
                value=None,
                error_code=None,
                reason_code=None,
                exception=None,
                diagnostics={"operation": "strategy.initialize"},
            )
        return self._boundary.call("strategy.initialize", hook, *args, **kwargs)

    def on_event(self, raw_event: Mapping[str, Any]) -> StrategyCallResult[object]:
        map_result = self._boundary.call("event.map", self._mapper.map_event, raw_event)
        if not map_result.ok:
            return StrategyCallResult(
                ok=False,
                value=None,
                error_code=map_result.error_code,
                reason_code=map_result.reason_code,
                exception=map_result.exception,
                diagnostics=dict(map_result.diagnostics),
            )

        mapped = map_result.value
        bridge = self._runtime_sdk_bridge

        if bridge is not None and isinstance(mapped, MarketBarEvent):
            on_bar = getattr(self._strategy, "on_bar", None)
            if callable(on_bar) and replay_sdk_hook_overridden(
                self._strategy, "on_bar"
            ):
                return self._boundary.call(
                    "strategy.on_bar",
                    bridge.dispatch_on_bar,
                    raw_event,
                    mapped,
                    on_bar,
                )

        if bridge is not None and isinstance(mapped, MarketQuoteEvent):
            on_quote = getattr(self._strategy, "on_quote", None)
            if callable(on_quote) and replay_sdk_hook_overridden(
                self._strategy, "on_quote"
            ):
                return self._boundary.call(
                    "strategy.on_quote",
                    bridge.dispatch_on_quote,
                    raw_event,
                    mapped,
                    on_quote,
                )

        if bridge is not None and isinstance(mapped, MarketTickEvent):
            on_tick = getattr(self._strategy, "on_tick", None)
            if callable(on_tick) and replay_sdk_hook_overridden(
                self._strategy, "on_tick"
            ):
                return self._boundary.call(
                    "strategy.on_tick",
                    bridge.dispatch_on_tick,
                    raw_event,
                    mapped,
                    on_tick,
                )

        if bridge is not None and isinstance(mapped, WireTimerEvent):
            on_timer = getattr(self._strategy, "on_timer", None)
            if callable(on_timer) and replay_sdk_hook_overridden(
                self._strategy, "on_timer"
            ):
                return self._boundary.call(
                    "strategy.on_timer",
                    bridge.dispatch_on_timer,
                    raw_event,
                    mapped,
                    on_timer,
                )

        hook = getattr(self._strategy, "on_event", None)
        if not callable(hook):
            return StrategyCallResult(
                ok=True,
                value=None,
                error_code=None,
                reason_code=None,
                exception=None,
                diagnostics={
                    "operation": "strategy.on_event",
                    "skipped": "no_on_event_hook",
                },
            )

        return self._boundary.call("strategy.on_event", hook, mapped)

    def stop(self) -> StrategyCallResult[object]:
        if isinstance(self._strategy, SdkStrategy):
            return self._boundary.call_cleanup(self._strategy.on_stop)
        hook = getattr(self._strategy, "stop", None)
        if not callable(hook):
            return StrategyCallResult(
                ok=True,
                value=None,
                error_code=None,
                reason_code=None,
                exception=None,
                diagnostics={"operation": "strategy.stop"},
            )
        return self._boundary.call_cleanup(hook)

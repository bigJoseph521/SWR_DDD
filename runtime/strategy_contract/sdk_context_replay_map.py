"""Map ``alphovex_sdk.context`` to replay services built in ``ReplaySdkBridge``.

``alphovex_sdk.context.StrategyContext`` is an ABC: seven abstract properties
(``account``, ``data``, ``orders``, ``logging``, ``params``, ``time``,
``indicator``). The context package defines no concrete implementation or
constructor; the worker must provide a subtype that delegates to replay state.

Sub-context ABCs (also no constructors in ``alphovex_sdk/context``):

- ``AccountContext``: ``portfolio()``, ``position(symbol)``, ``positions()``
- ``DataContext``: latest bar/quote/tick, ``get_bars`` / ``get_ticks`` /
  ``get_quotes`` (+ dict/DataFrame helpers as defined on the ABC)
- ``OrdersContext``: ``buy`` / ``sell``, cancel helpers, ``list`` / ``get`` /
  ``active`` / ``done`` / ``filled`` / per-instrument queries
- ``LoggingContext``: ``debug`` / ``info`` / ``warning`` / ``error``
- ``ParamsContext``: ``get``, ``get_params``
- ``TimeContext``: ``now`` → ``Timestamp`` (``datetime`` alias in
  ``alphovex_sdk.typedefs``), ``today`` → ``date``
- ``IndicatorContext``: ``register_indicator``, ``get_indicator_value``

Replay bridge wiring (see ``bootstrap.replay_sdk_bridge.ReplaySdkBridge``):

- ``account`` ← ``SnapshotPortfolioService`` constructed from the in-bridge
  ``PortfolioSnapshot`` (cash seeded; positions empty until portfolio updates).
- ``data`` ← ``InMemoryReplayDataService`` (same instance mutated by
  ``ReplayStateCoordinator.apply_bar`` / ``apply_quote`` / ``apply_tick``
  before strategy callbacks).
- ``orders`` ← ``GrpcSubmittingOrderService`` (if gRPC submitter given) else
  ``DefaultOrderService`` (strategy/user ids from launch + worker identity).
- ``logging`` ← ``_ReplayLogger`` (``LoggerBackend``; currently no-op emit).
  Facade should adapt ``LoggingContext`` calls to the logger backend or stdlib
  logging if behavior is added later.
- ``params`` ← ``ParameterSchema`` from the strategy’s ``build_parameter_schema``
  (or empty schema). Facade implements ``ParamsContext`` against that object.
- ``time`` ← ``_SimulatedClockService`` wrapping the worker ``SimulatedClock``
  (``set_event_time`` advances clock before each applied event).
- ``indicator`` ← ``IndicatorService`` constructed with the same ``data`` service
  and primary timeframe as the bridge.

Not represented on ``alphovex_sdk.context.StrategyContext`` but present on the
legacy monolithic context used by the bridge today: deployment metadata,
``run_id``, ``strategy_id``, ``metrics``, ``risk``. Those stay outside this ABC
or on separate runtime types until the SDK exposes them on the facade.

This module is documentation only; it is not imported by the bridge.
"""

__all__: list[str] = []

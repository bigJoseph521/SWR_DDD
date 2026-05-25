# Strategy Worker Runtime — DDD / Hexagonal Refactor

## Runtime package top-level structure

The `runtime/` package contains only:

```
runtime/
├── domain/           # Pure models, specs, events, policies
├── application/      # Use cases, orchestration, ports
├── interface/        # Inbound adapters (CLI, gRPC, HTTP)
├── infrastructure/   # Outbound adapters (Redis, gRPC, HTTP, SQLite, SDK, config)
├── bootstrap/        # Composition root + bootstrap orchestration
├── __init__.py
└── main.py           # Thin entrypoint wrapper → interface/cli/main.py
```

Deprecated compatibility wrapper folders (`config/`, `events/`, `integration/`, `transport/`, `persistence/`, `observability/`, `strategy_contract/`, nested `runtime/`) have been **removed**. Import canonical paths only.

### `bootstrap/` (composition + transitional orchestration)

| Module | Role |
|--------|------|
| `dependency_container.py` | Builds `DependencyContainer`, journal, bootstrap pipeline, `LifecycleService` |
| `lifecycle_wiring.py` | `build_lifecycle_ddd_wiring` (dispatcher, heartbeat, strategy execution) |
| `heartbeat_wiring.py` | `ManagerGatewayStatusAdapter` for SRM status port |
| `runtime_composition.py` | gRPC client construction + DI entry |
| `runtime_dependencies_wiring.py` | Wires concrete gateways/clocks into `RuntimeDependencies` |
| `sdk_order_intent_wiring.py` | SDK submitter factory → `SubmitOrderIntent` + Risk adapter |
| `runtime_spec_builder.py` | Builds domain spec models from settings (uses `domain/platform_trace_factory.py`) |
| `validator.py`, `sdk_contract_validator.py` | Launch validation pipeline (uses `domain/launch_spec.py`) |
| `persistence.py`, `strategy_instance_manager.py` | Bootstrap lifecycle |
| `minimal_env_validation.py`, `srm_env_status_report.py` | SRM pre-bootstrap |
| `infrastructure/backtest/backtest_bar_timeframe_filter.py` | BACKTEST bar-timeframe filter for stdin dispatch |
| `lifecycle_host_wiring.py`, `lifecycle_host_adapters.py`, `lifecycle_feed_wiring.py` | Concrete lifecycle side-effects (SRM, Redis feeds, SDK bridge, domain events) wired into `LifecycleService` |

**Follow-up:** move remaining `bootstrap/` orchestration into `application/` or `infrastructure/` as layers stabilize.

---

## Market-data ingress (mode-specific sources, shared strategy path)

BACKTEST market-data ingress target is **stdin JSON Lines / stdio input** from an upstream
**runner** process (subprocess model: worker reads stdin, writes machine-readable output to
stdout if needed, logs to stderr only). The stdin payload shape is **intentionally not
finalized** yet; placeholder types include ``MARKET_DATA``, ``CONTROL_STOP``, and
``END_OF_STREAM``. **``OPEN_ORDERS_SNAPSHOT`` is not supported.**

strategy-worker-runtime does **not** know or depend on backtest-service. The upstream
runner/controller is responsible for writing input to stdin.

PAPER/LIVE receive market data from **Redis** streams (unchanged). After wire events are
normalized, **all modes** use the same pipeline:

``market data feeder → normalized raw tick/event → EventDispatcher.dispatch_raw → StrategyAdapter → strategy hooks → order intents``

- BACKTEST ingress: ``BacktestStdinMarketDataFeed`` (``runtime/interface/stdio/``) wired from ``lifecycle_feed_wiring``; ``END_OF_STREAM`` / EOF calls ``mark_backtest_market_data_stream_complete``.
- PAPER/LIVE ingress: ``lifecycle_feed_wiring`` / ``market_data_redis_feed`` → ``LifecycleService._dispatch_paper_live_tick``.
- ``ReplaySdkBridge`` / ``BacktestSdkBridge`` is SDK in-memory data only, not a separate strategy execution path.

**Application port:** ``MarketDataFeedPort`` (``runtime/application/ports/market_data_feed.py``) — generic ``start(on_tick)`` / ``stop()`` contract for mode-specific feeders.

---

## Layer roles

| Layer | Role |
|-------|------|
| `runtime/domain/` | Pure models, specs, normalized events, policies |
| `runtime/application/` | Use cases, dispatch, strategy execution, ports |
| `runtime/interface/` | CLI entrypoint, inbound HTTP control, **BACKTEST stdin feed** |
| `runtime/infrastructure/` | Redis, Risk gRPC, SRM HTTP, SQLite, SDK bridge, settings |
| `runtime/bootstrap/` | Composition root, spec construction, bootstrap pipeline wiring |

### Import boundaries (enforced in CI)

`tests/unit/test_ddd_import_boundaries.py` scans `runtime/{domain,application,interface,infrastructure,bootstrap}/**/*.py` with `ast` and fails on forbidden cross-layer imports. New violations must be fixed or added to `ALLOWED_IMPORT_VIOLATIONS` with a removal reason.

| Source layer | Must not import |
|--------------|-----------------|
| `runtime/domain/` | `runtime.application`, `runtime.infrastructure`, `runtime.interface`, `runtime.bootstrap` |
| `runtime/application/` | `runtime.infrastructure`, `runtime.interface`, `runtime.bootstrap` |
| `runtime/interface/` | `runtime.infrastructure` (prefer wiring in `bootstrap`) |
| `runtime/infrastructure/` | `runtime.bootstrap` (pass specs via constructors; **zero allowlisted violations**) |
| `runtime/bootstrap/` | *(no restrictions — composition root)* |

**Composition ownership:** `runtime/bootstrap/dependency_container.py` builds the worker
container (logging, SQLite journal, bootstrap pipeline, runtime dependencies, lifecycle).
`runtime/bootstrap/lifecycle_wiring.py` wires DDD application services (dispatcher, heartbeat,
strategy execution) and is injected into :class:`~runtime.application.lifecycle.lifecycle_service.LifecycleService`
via ``ddd_wiring_builder``. Application lifecycle code depends on **application ports** and
``LifecycleHostPorts`` (injected from bootstrap); it does not import ``runtime/bootstrap``,
``runtime/infrastructure``, or ``runtime/interface``.

**Application ports** (under ``runtime/application/ports/``):

| Port / module | Role |
|---------------|------|
| `launch_context.py` | ``LaunchContext`` — launch metadata for lifecycle without ``LaunchSpec`` |
| `lifecycle_ports.py` | ``LifecycleHostPorts``, SRM/SDK/feed/journal/logger/bootstrap pipeline ports |
| `backtest_sdk_bridge_port.py` | ``BacktestSdkBridgePort`` — in-memory SDK data for BACKTEST (same hook path as other modes) |
| `application/market_data/` | BACKTEST historical batch ingest → shared ``EventDispatcher`` |
| `worker_domain_events.py` | ``StrategyWorkerDomainEvent`` enum for application observability |
| `runtime_journal_port.py` | ``RuntimeJournalPort`` — stop checkpoint / journal hooks |

**Bootstrap lifecycle host wiring:** ``build_lifecycle_host_ports(lifecycle_service)`` returns
``LifecycleHostPorts`` with Redis feed starters (PAPER/LIVE), SRM reporters, BACKTEST bar-timeframe filter, SDK bridge
factory, simulated-clock factory, and domain-event emitter. ``dependency_container`` calls
``lifecycle_service.attach_host_ports(...)`` after constructing the service.

**Infrastructure → bootstrap:** resolved. Shared types live under ``runtime/domain/``:

| Domain module | Role |
|---------------|------|
| `domain/launch_spec.py` | ``LaunchSpec``, ``LaunchSpecValidationError`` (parsed by ``Settings`` and loaders) |
| `domain/bootstrap_failures.py` | ``BootstrapFailure`` hierarchy raised by strategy loaders |
| `domain/bar_timeframe.py` | Bar timeframe normalization / Redis ``md:stream:am`` policy |
| `domain/platform_trace_factory.py` | ``build_platform_trace_spec_from_launch`` for logging and Risk trace |
| `domain/artifact_digest_policy.py` | Digest validation opt-out policy |

Bootstrap adapts launch metadata into domain types before constructing infrastructure adapters.

**Interface → infrastructure:** resolved. ``runtime/interface`` is protocol-only:

| Module | Role |
|--------|------|
| `interface/cli/main.py` | Thin handoff to ``bootstrap/runtime_entrypoint.run_runtime_from_cli()`` |
| `interface/stdio/backtest_stdin_market_data_feed.py` | ``BacktestStdinMarketDataFeed`` — BACKTEST stdin pull loop (skeleton) |
| `interface/stdio/stdin_protocol.py` | Placeholder stdin message type labels (payload TBD) |
| `interface/http/stop_control_server.py` | ``POST /internal/v1/stop`` using ``application/http/internal_auth`` |
| `application/http/internal_auth.py` | Internal caller/trust header validation |

**Bootstrap interface wiring:**

| Function / module | Role |
|-------------------|------|
| `bootstrap/runtime_entrypoint.py` | CLI process entry: settings, gRPC clients, container, stop server |
| `bootstrap/runtime_composition.py` | ``build_runtime_container``, ``build_runtime_outbound_clients`` (SRM HTTP + Risk gRPC) |

**Temporary allowlist:** empty — **domain**, **application**, **infrastructure**, and **interface** have **zero** allowlisted import violations.
Removing an allowlist entry without fixing the import causes `test_allowlist_entries_reference_real_violations` to fail when the import is already gone.

---

## Current wiring (this branch)

### Unified market-event path

```
Mode input → EventDispatcher.dispatch_raw()
  → RuntimeEventHandler.handle_raw_tick() (public API)
  → MarketEventHandler → StrategyExecutionService → StrategyAdapter
```

Portfolio updates use ``EventDispatcher.dispatch_portfolio()`` →
``RuntimeEventHandler.handle_portfolio_update()``.
``EventDispatcher`` must not introspect private handler attributes.

### Order intents → Risk Service (no OMS egress)

Strategy Worker Runtime does **not** contain OMS client code, OMS gateways, OMS config/env
(``SWR_OMS_*``), or composition paths that submit order intents to OMS. All SDK order intents
egress through Risk Service only. OMS interaction, when required by the platform, happens
downstream of Risk (not from SWR).

Enforced by ``tests/unit/test_no_oms_runtime_code.py`` (forbidden OMS tokens in ``runtime/**/*.py``).

```
SDK order intent
  → StrategyOrderIntent (domain; strategy fields only)
  → SubmitOrderIntent (application use case; depends on RiskOrderIntentSubmissionPort)
  → RiskGatewaySubmissionAdapter (infrastructure; implements the port)
  → build_risk_order_intent_wire_payload() (infrastructure; adds runtime_id, mode, correlation_id, …)
  → RiskOrderIntentGateway.submit_order_intent_wire()
  → Risk Service gRPC
```

SDK submitter composition lives in ``runtime/bootstrap/sdk_order_intent_wiring.py``.
``runtime/application/order_intents/`` must not import ``runtime/infrastructure/``.

Risk egress requires a real ``correlation_id`` from launch/bundle context,
``PlatformTraceSpec``, or validated runtime fallback. The wire mapper must not
invent random orphan correlation IDs; missing correlation metadata fails closed
with ``MISSING_CORRELATION_ID``.

``SubmitOrderIntent`` maps typed submission failures to stable ``reason_code``
values (for example ``RISK_SERVICE_UNAVAILABLE``, ``RISK_SERVICE_TIMEOUT``,
``ORDER_INTENT_SUBMISSION_INTERNAL_ERROR``). Raw exception strings are logged
internally only and must not appear in ``OrderIntentResult.reason_code``.
Normal Risk rejections (``accepted: false`` in the gRPC response) are returned
as structured ``OrderIntentResult`` values with the response payload preserved.

Application code uses :class:`~runtime.domain.model.strategy_order_intent.StrategyOrderIntent`
(instrument, side, type, quantity, prices, optional ``client_order_id``). Platform trace metadata
(``strategy_id``, ``correlation_id``, ``request_id``) lives in
:class:`~runtime.domain.model.platform_trace_spec.PlatformTraceSpec` (pure domain value object).
Bootstrap maps ``LaunchSpec`` into ``PlatformTraceSpec`` via
``runtime/domain/platform_trace_factory.py`` and
``runtime/bootstrap/runtime_spec_builder.py``. Trace fields are attached to structured
logs, domain events, manager signals, and Risk Service wire payloads via
``runtime/infrastructure/grpc/risk_order_intent_wire_mapper.py``.

Canonical env: `SWR_RISK_GRPC_TARGET`.

### Entrypoint

| Path | Role |
|------|------|
| `runtime/main.py` | Thin wrapper (Docker/package) |
| `runtime/interface/cli/main.py` | Full CLI implementation |

---

## Local worker state

When ``setting.json`` omits ``work_root``, the runtime creates a directory named after
``deployment_id`` (from bundle, launch payload, or ``DEPLOYMENT_ID`` env). Each deployment
therefore gets isolated SQLite/journal files under ``{cwd}/{deployment_id}/``:

- ``state_journal.sqlite``
- ``state_journal.txt``
- ``mypy_result.txt`` (SDK contract mypy validation during bootstrap)

Fallback order: ``deployment_id`` only (bundle, launch payload, or ``DEPLOYMENT_ID`` env).
When ``work_root`` is omitted and ``deployment_id`` is missing, settings load fails.

Explicit ``work_root`` in the bundle still overrides this default.

---

## Manager → worker control

SRM stops workers via **HTTP** ``POST /internal/v1/stop`` and Kubernetes **SIGTERM** (see ``cooperative_shutdown.py``). Worker→SRM lifecycle signals use HTTP JSON via ``ManagerGateway``.

---

- [ ] Move `bootstrap/sdk_contract_validator`, etc. into correct layers
- [ ] Extract Redis feeds behind `InputFeedPort`
- [ ] Split `LifecycleService` into focused application services
- [ ] Fix pre-existing `test_proto_contracts` golden (`orderIntentId` as string)

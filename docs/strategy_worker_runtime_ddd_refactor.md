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
| `composition_root.py` | Re-exports `build_runtime_container` |
| `runtime_composition.py` | gRPC client construction + DI |
| `runtime_dependencies_wiring.py` | Wires concrete gateways/clocks into `RuntimeDependencies` |
| `sdk_order_intent_wiring.py` | SDK submitter factory → `SubmitOrderIntent` + Risk adapter |
| `runtime_spec_builder.py` | Builds domain spec models from settings |
| `launch_spec.py`, `validator.py`, `sdk_contract_validator.py` | Launch validation pipeline |
| `failures.py`, `persistence.py`, `strategy_instance_manager.py` | Bootstrap lifecycle |
| `minimal_env_validation.py`, `srm_env_status_report.py` | SRM pre-bootstrap |
| `backtest_replay_tick_filter.py`, `bar_timeframe.py` | Backtest helpers |

**Follow-up:** move remaining `bootstrap/` orchestration into `application/` or `infrastructure/` as layers stabilize.

---

## Layer roles

| Layer | Role |
|-------|------|
| `runtime/domain/` | Pure models, specs, normalized events, policies |
| `runtime/application/` | Use cases, dispatch, strategy execution, ports |
| `runtime/interface/` | CLI entrypoint, inbound gRPC/HTTP control, replay ingress |
| `runtime/infrastructure/` | Redis, Risk gRPC, SRM HTTP, SQLite, SDK bridge, settings |
| `runtime/bootstrap/` | Composition root, spec construction, bootstrap pipeline wiring |

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

### Order intents → Risk Service

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
:class:`~runtime.domain.model.platform_trace_spec.PlatformTraceSpec` and is attached to structured
logs, domain events, manager signals, and Risk Service wire payloads via
``runtime/bootstrap/runtime_spec_builder.py`` and
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

- [ ] Move `bootstrap/launch_spec`, `sdk_contract_validator`, etc. into correct layers
- [ ] Extract Redis feeds behind `InputFeedPort`
- [ ] Split `LifecycleService` into focused application services
- [ ] Fix pre-existing `test_proto_contracts` golden (`orderIntentId` as string)

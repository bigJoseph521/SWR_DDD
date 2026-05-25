# Strategy Worker Runtime — Pull Request Review

**Branch:** `feature/make-inspect-results`  
**Review date:** 2026-05-25  
**Reviewer role:** Senior software engineer (pre-merge risk review)

---

## Context

| Field | Value |
|-------|-------|
| **Goal** | DDD/hexagonal refactor: restructure `runtime/` into domain/application/interface/infrastructure/bootstrap; unify market-event dispatch; route order intents through Risk Service with `StrategyOrderIntent` / `PlatformTraceSpec` separation; delete deprecated `transport/`, `config/`, `persistence/`, etc. |
| **Application area** | All layers + protos + migrations + tests |
| **Execution modes** | BACKTEST, PAPER, LIVE (mode policy wired via `get_mode_policy`) |
| **External dependencies** | Risk gRPC, SRM HTTP, Redis market data, Redis portfolio updates, replay ingress gRPC, SQLite journal, SDK bundle loader |
| **Constraints** | Proto backward compatibility, SQLite migration ordering, SDK contract stability, launch_attempt dedupe, cooperative shutdown idempotency, hot-path event latency |
| **Scope** | ~185 files changed (+4,708 / −23,867 lines); major new dirs under `runtime/application/`, `runtime/domain/`, `runtime/infrastructure/`; deleted legacy modules; untracked `migrations/019_drop_causation_id_columns.sql` |

### Architecture reference (canonical)

```
runtime/
├── domain/           # Pure models, specs, normalized events, policies — NO I/O
├── application/      # Use cases, dispatch, strategy execution, ports (interfaces)
├── interface/        # Inbound adapters: CLI, HTTP stop, replay ingress gRPC
├── infrastructure/   # Outbound adapters: Redis, Risk gRPC, SRM HTTP, SQLite, SDK, config
├── bootstrap/        # Composition root, launch validation, spec construction (transitional)
└── main.py           # Thin wrapper → runtime/interface/cli/main.py
```

### Key flows to protect

1. **Market events:** `EventDispatcher.dispatch_raw()` → `MarketEventHandler` / `StrategyExecutionService` → `StrategyAdapter`
2. **Order intents:** SDK → `StrategyOrderIntent` (domain) → `SubmitOrderIntent` → `RiskOrderIntentSubmissionPort` → wire mapper → Risk Service gRPC
3. **Trace metadata:** `PlatformTraceSpec` attached via `runtime_spec_builder` and wire mappers
4. **Shutdown:** HTTP `POST /internal/v1/stop` and SIGTERM → `CooperativeShutdownCoordinator`
5. **Lifecycle signals:** SRM HTTP JSON via `ManagerGateway`; heartbeat dedupe via `LifecycleSignalDedupe`

See also: `docs/strategy_worker_runtime_ddd_refactor.md`

---

## A. Summary verdict

**Needs changes**

The refactor direction is sound and key separations (`StrategyOrderIntent` vs `PlatformTraceSpec`, Risk egress, unified `EventDispatcher`) are implemented. But the change set is too large and partially uncommitted to merge safely: application code still imports infrastructure directly, `EventDispatcher` reaches into private handler state, migration `019` is untracked, and the full suite reported **20 failures** in the review environment (some may be sandbox SQLite write issues — that must be verified on a writable checkout before merge). Layer purity is inconsistent with the stated DDD goal, which raises long-term regression risk even if unit tests for the new paths pass.

---

## B. Top risks

Ordered by severity.

### 1. Application layer imports infrastructure (hex boundary broken)

| | |
|---|---|
| **Risk** | Application use cases are coupled to concrete gRPC/Redis/SQLite adapters. |
| **Why it matters** | Application code becomes hard to test in isolation; refactors re-couple layers and invite circular imports. |
| **Evidence** | `runtime/application/order_intents/risk_gateway_submission_port.py` imports `RiskOrderIntentGateway` and `build_risk_order_intent_wire_payload` from `runtime/infrastructure/grpc/`. Same pattern in `sdk_order_intent_submission.py`, `dependency_container.py`, `lifecycle_service.py`, `runtime_dependencies.py`, `strategy_adapter.py`. |
| **Suggested fix** | Move `RiskGatewaySubmissionPort` and wire mapping into `runtime/infrastructure/`. Application should only see `RiskOrderIntentSubmissionPort`. Wire construction stays in infrastructure; `SubmitOrderIntent` stays in application. |

### 2. `EventDispatcher` uses private attribute introspection

| | |
|---|---|
| **Risk** | Dispatch routing depends on `_market_handler` / `_portfolio_handler` implementation details. |
| **Why it matters** | Renaming or restructuring handlers silently breaks dispatch without compile-time errors. |
| **Evidence** | `EventDispatcher.dispatch_raw()` and `dispatch_portfolio()` in `runtime/application/event_handling/event_dispatcher.py` use `getattr(self._runtime_handler, "_market_handler", None)` and `getattr(..., "_portfolio_handler", None)`. |
| **Suggested fix** | Expose explicit methods on `RuntimeEventHandler` (`has_market_handler()`, `handle_raw_tick()`, `handle_portfolio()`) or inject handlers directly into `EventDispatcher` at composition time. |

### 3. Correlation ID silently replaced with random UUID

| | |
|---|---|
| **Risk** | Missing correlation metadata is masked by a generated UUID on the wire. |
| **Why it matters** | Breaks end-to-end traceability for order intents; Risk Service and SRM may not correlate intents to the originating deployment request. |
| **Evidence** | `build_risk_order_intent_wire_payload()` in `runtime/infrastructure/grpc/risk_order_intent_wire_mapper.py`: `wire_corr = bundle_corr or trace_corr or fallback_corr or uuid.uuid4().hex` |
| **Suggested fix** | Fail closed or emit a structured warning + metric when correlation is absent; do not invent a random ID on the egress hot path unless Risk Service explicitly accepts orphan correlation. |

### 4. Migration `019` is untracked / may not ship with the refactor

| | |
|---|---|
| **Risk** | Schema migration referenced in code but not committed. |
| **Why it matters** | Deployments with existing `{deployment_id}/state_journal.sqlite` may fail migration or retain dead columns. |
| **Evidence** | Untracked `migrations/019_drop_causation_id_columns.sql`; listed in `runtime/infrastructure/persistence/migrations.py`. |
| **Suggested fix** | Commit migration `019` in the same PR; add an integration test that applies migrations 001→019 on a fixture DB. |

### 5. Broad exception swallowing on order-intent submit

| | |
|---|---|
| **Risk** | All submission failures collapse to a generic `OrderIntentResult`. |
| **Why it matters** | Transient gRPC failures, validation errors, and programming bugs are indistinguishable; `reason_code=str(exc)` may leak internal exception text into journals/logs. |
| **Evidence** | `SubmitOrderIntent.execute()` in `runtime/application/order_intents/submit_order_intent.py` catches bare `Exception` and returns `reason_code=str(exc)`. |
| **Suggested fix** | Catch typed transport errors; map to stable reason codes; log full exception at ERROR with trace fields; never pass raw exception strings to external systems. |

### 6. Domain layer depends on bootstrap

| | |
|---|---|
| **Risk** | Domain models import bootstrap orchestration types. |
| **Why it matters** | Violates “pure domain” and makes domain models harder to reuse/test. |
| **Evidence** | `PlatformTraceSpec.from_launch()` in `runtime/domain/model/platform_trace_spec.py` imports `LaunchSpec` from `runtime.bootstrap.launch_spec`. |
| **Suggested fix** | Define a minimal domain DTO (e.g. `LaunchIdentity`) in `domain/` and map from `LaunchSpec` in bootstrap or `runtime_spec_builder`. |

### 7. Change scope and test coverage gap

| | |
|---|---|
| **Risk** | Large deletion surface with incomplete test migration. |
| **Why it matters** | Regressions in bootstrap, shutdown, and proto compatibility may slip through. |
| **Evidence** | ~24k lines deleted including tests (`test_serializers.py`, `test_worker_control_application.py`, `test_historical_data_client.py`, `test_interceptors.py`, manager gRPC client). Full pytest run: **20 failed, 510 passed** (some failures were SQLite “unable to open database file” in read-only sandbox; bootstrap integration failures need confirmation on a normal checkout). |
| **Suggested fix** | Split into reviewable PRs (layer move → port wiring → delete legacy). Require green `pytest tests/` on CI before merge. Replace deleted tests with equivalents under new module paths. |

---

## C. Missing tests

1. **`tests/integration/test_migrations.py`** — apply through `019_drop_causation_id_columns.sql` on a pre-018 schema fixture; assert columns gone and worker still boots.
2. **`tests/unit/test_event_dispatcher.py`** — portfolio and raw-tick routing via public `RuntimeEventHandler` API (not `_market_handler` introspection); assert `EventMappingError` is logged, not silently dropped.
3. **`tests/unit/test_risk_order_intent_wire_mapper.py`** — correlation fallback behavior: missing bundle/env correlation should not silently UUID unless explicitly intended; assert `PlatformTraceSpec.effective_correlation_id()` precedence.
4. **`tests/integration/test_order_intent_to_risk_service_flow.py`** — extend to cover `SubmitOrderIntent` failure paths (Risk timeout, rejected intent) and journal callback behavior.
5. **`tests/e2e/test_controlled_stop.py`** — re-run after refactor to confirm HTTP stop + SIGTERM still share `CooperativeShutdownCoordinator` under new `LifecycleService` wiring.
6. **`tests/unit/test_layer_imports.py`** (new) — static check that `runtime/domain/**` does not import `bootstrap`, `infrastructure`, or `application`.

---

## D. Questions for the author

1. Is the full DDD restructure intended to land in **one PR**, or should this be split (e.g. move files → wire ports → delete legacy)?
2. Why is `019_drop_causation_id_columns.sql` untracked — intentional WIP or accidental omission?
3. Is random `uuid.uuid4().hex` for missing `correlation_id` an explicit product decision for Risk Service, or a temporary fallback?
4. Are the 20 pytest failures reproducible on a clean writable checkout / CI? Which are real regressions vs environment?
5. Is direct OMS egress fully removed, or is `oms_client` in `runtime_composition.py` / `runtime_dependencies.py` still a supported path for PAPER/LIVE?
6. Does Risk Service require `strategy_id` on the wire payload? `PlatformTraceSpec.strategy_id` is populated but not obviously mapped in `build_risk_order_intent_wire_payload()`.
7. Was `test_proto_contracts` golden update for `orderIntentId` as string (noted in refactor doc TODO) completed?

---

## E. Smaller alternative

Merge in three incremental PRs:

1. **Structure only:** Move files to `domain/application/infrastructure/interface` with compatibility re-exports; no behavior change; all tests green.
2. **Order-intent + trace split:** Introduce `StrategyOrderIntent`, `PlatformTraceSpec`, `SubmitOrderIntent`, infrastructure wire mapper; keep old submit path behind a flag until Risk path is verified.
3. **Delete legacy:** Remove `runtime/transport/`, manager gRPC protos, OMS aliases; commit migration `019`; fix `EventDispatcher` handler wiring; enforce import-boundary lint.

Each PR should be ≤~2k net lines and independently revertible.

---

## Checklist summary

| Area | Status | Notes |
|------|--------|-------|
| Intent and scope | ⚠️ | Goal achieved but scope is very large; unrelated deletions bundled |
| Correctness | ⚠️ | Core paths wired; correlation UUID fallback and silent event drops are concerns |
| Security and data handling | ✅ | No new obvious exposure; validate exception strings in order-intent errors |
| Reliability and failure modes | ⚠️ | Broad exception catch; dependency failure mapping unclear |
| Performance and scalability | ✅ | No obvious hot-path regression identified in review |
| Tests | ❌ | 20 failures reported; deleted tests not fully replaced |
| Maintainability | ⚠️ | Layer violations and `EventDispatcher` coupling undermine DDD intent |
| Deployment and rollback | ⚠️ | Migration `019` untracked; large rollback surface |

---

## Post-merge verification

After release, confirm:

- [ ] Heartbeat continuity to SRM
- [ ] Stop via HTTP `POST /internal/v1/stop` and SIGTERM (idempotent)
- [ ] Order-intent egress when `SWR_RISK_GRPC_TARGET` is set
- [ ] Bootstrap mypy journal (`mypy_result.txt`) written under `{deployment_id}/`
- [ ] Worker phase transitions (INITIALIZING → RUNNING → STOPPED)
- [ ] SQLite migrations apply cleanly on existing deployment journals

---

## Uncertainty

Full test failure root cause needs a writable `pytest tests/` run on the author's machine or CI. The review sandbox blocked SQLite journal creation, which may explain some `test_dependency_container` failures but likely not all bootstrap integration failures.

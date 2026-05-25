"""
SWR-101 — Canonical Contract Registry and Identifier Guardrails

Unit tests covering:
  A. Canonical identifier presence / completeness
  B. Domain uniqueness / registry consistency
  C. Validator helper behaviour
  D. JSON helper behaviour
  E. Worker/runtime semantics guardrails
  F. CI / lint-facing negative coverage
"""

from __future__ import annotations

import pytest

# ---------------------------------------------------------------------------
# Domain enums & frozenset collections
# ---------------------------------------------------------------------------
from runtime.domain.enums import (
    CANONICAL_EVENT_IDS,
    CANONICAL_EVENT_NAMES,
    CANONICAL_INTERNAL_PRIVILEGES,
    CANONICAL_ROUTE_IDS,
    CANONICAL_WORKER_MODES,
    RUNTIME_LIFECYCLE_EVENT_ID_TO_NAME,
    RUNTIME_MANAGER_SIGNAL_REQUIRED_PRIVILEGE,
    WORKER_EVENT_ID_TO_NAME,
    WORKER_ROUTE_REQUIRED_PRIVILEGE,
    InternalPrivilege,
    RuntimeLifecycleEventId,
    RuntimeLifecycleEventName,
    RuntimeManagerSignalId,
    WorkerEventId,
    WorkerEventName,
    WorkerMode,
    WorkerRouteId,
)
from runtime.domain.errors import (
    ALL_KNOWN_ERROR_CODES,
    SHARED_BOUNDARY_ERROR_CODES,
    WORKER_CANONICAL_ERROR_CODES,
    SharedBoundaryErrorCode,
    WorkerErrorCode,
    is_known_error_code,
    is_shared_boundary_error_code,
    is_worker_error_code,
)
from runtime.domain.runtime_state import (
    CANONICAL_RUNTIME_REASONS,
    CANONICAL_RUNTIME_STATES,
    TERMINAL_CANONICAL_STATES,
    TERMINAL_LOCAL_PHASES,
    VALID_STATE_REASONS,
    CanonicalRuntimeReason,
    CanonicalRuntimeState,
    RuntimeStatus,
    WorkerLocalPhase,
    is_canonical_runtime_reason,
    is_canonical_runtime_state,
    is_terminal_canonical_state,
    is_terminal_local_phase,
    require_canonical_runtime_reason,
    require_canonical_runtime_state,
    to_default_canonical_state,
    validate_state_reason_pair,
)

# ---------------------------------------------------------------------------
# Identifier registry helpers
# ---------------------------------------------------------------------------
from runtime.utils.ids import (
    ALL_CANONICAL_IDENTIFIERS,
    CANONICAL_IDENTIFIERS_BY_DOMAIN,
    find_identifier_domain,
    get_canonical_identifiers,
    is_canonical_identifier,
    is_shared_identifier_domain,
    require_canonical_identifier,
    require_canonical_identifiers,
)

# ---------------------------------------------------------------------------
# JSON helpers
# ---------------------------------------------------------------------------
from runtime.utils.json import (
    canonical_error_body,
    canonical_event_envelope,
    canonical_runtime_state_payload,
    require_iso8601_timestamp,
)

# ===========================================================================
# A. Canonical identifier presence / completeness
# ===========================================================================


class TestRouteIdPresence:
    def test_worker_create_route_present(self) -> None:
        assert WorkerRouteId.CREATE == "ROUTE-WORKER-CREATE"
        assert "ROUTE-WORKER-CREATE" in CANONICAL_ROUTE_IDS

    def test_worker_stop_route_present(self) -> None:
        assert WorkerRouteId.STOP == "ROUTE-WORKER-STOP"
        assert "ROUTE-WORKER-STOP" in CANONICAL_ROUTE_IDS

    def test_manager_heartbeat_signal_present(self) -> None:
        assert RuntimeManagerSignalId.HEARTBEAT == "ROUTE-RUNTIME-HEARTBEAT"
        assert "ROUTE-RUNTIME-HEARTBEAT" in CANONICAL_ROUTE_IDS

    def test_manager_launch_succeeded_signal_present(self) -> None:
        assert (
            RuntimeManagerSignalId.LAUNCH_SUCCEEDED == "ROUTE-RUNTIME-LAUNCH-SUCCEEDED"
        )
        assert "ROUTE-RUNTIME-LAUNCH-SUCCEEDED" in CANONICAL_ROUTE_IDS

    def test_manager_launch_failed_signal_present(self) -> None:
        assert RuntimeManagerSignalId.LAUNCH_FAILED == "ROUTE-RUNTIME-LAUNCH-FAILED"
        assert "ROUTE-RUNTIME-LAUNCH-FAILED" in CANONICAL_ROUTE_IDS

    def test_manager_terminated_signal_present(self) -> None:
        assert RuntimeManagerSignalId.TERMINATED == "ROUTE-RUNTIME-TERMINATED"
        assert "ROUTE-RUNTIME-TERMINATED" in CANONICAL_ROUTE_IDS

    def test_every_worker_route_id_in_collection(self) -> None:
        for route in WorkerRouteId:
            assert route.value in CANONICAL_ROUTE_IDS

    def test_every_manager_signal_id_in_collection(self) -> None:
        for signal in RuntimeManagerSignalId:
            assert signal.value in CANONICAL_ROUTE_IDS

    def test_every_worker_route_has_privilege_entry(self) -> None:
        for route in WorkerRouteId:
            assert route in WORKER_ROUTE_REQUIRED_PRIVILEGE

    def test_every_manager_signal_has_privilege_entry(self) -> None:
        for signal in RuntimeManagerSignalId:
            assert signal in RUNTIME_MANAGER_SIGNAL_REQUIRED_PRIVILEGE

    def test_create_route_requires_substrate_privilege(self) -> None:
        assert (
            WORKER_ROUTE_REQUIRED_PRIVILEGE[WorkerRouteId.CREATE]
            == InternalPrivilege.RUNTIME_SUBSTRATE
        )

    def test_stop_route_requires_control_privilege(self) -> None:
        assert (
            WORKER_ROUTE_REQUIRED_PRIVILEGE[WorkerRouteId.STOP]
            == InternalPrivilege.RUNTIME_CONTROL
        )

    def test_health_routes_require_no_privilege(self) -> None:
        assert WORKER_ROUTE_REQUIRED_PRIVILEGE[WorkerRouteId.HEALTH_LIVE] is None
        assert WORKER_ROUTE_REQUIRED_PRIVILEGE[WorkerRouteId.HEALTH_READY] is None


class TestEventIdPresence:
    @pytest.mark.parametrize(
        "evt_id",
        [
            "EVT-WORKER-LAUNCH-SUCCEEDED",
            "EVT-WORKER-LAUNCH-FAILED",
            "EVT-WORKER-HEARTBEAT",
        ],
    )
    def test_worker_event_ids_present(self, evt_id: str) -> None:
        assert evt_id in CANONICAL_EVENT_IDS

    @pytest.mark.parametrize(
        "evt_id",
        [
            "EVT-RUNTIME-STARTED",
            "EVT-RUNTIME-DEGRADED",
            "EVT-RUNTIME-FAILED",
        ],
    )
    def test_lifecycle_event_ids_present(self, evt_id: str) -> None:
        assert evt_id in CANONICAL_EVENT_IDS

    def test_worker_event_id_enum_string_values(self) -> None:
        assert WorkerEventId.LAUNCH_SUCCEEDED == "EVT-WORKER-LAUNCH-SUCCEEDED"
        assert WorkerEventId.LAUNCH_FAILED == "EVT-WORKER-LAUNCH-FAILED"
        assert WorkerEventId.HEARTBEAT == "EVT-WORKER-HEARTBEAT"

    def test_lifecycle_event_id_enum_string_values(self) -> None:
        assert RuntimeLifecycleEventId.STARTED == "EVT-RUNTIME-STARTED"
        assert RuntimeLifecycleEventId.DEGRADED == "EVT-RUNTIME-DEGRADED"
        assert RuntimeLifecycleEventId.FAILED == "EVT-RUNTIME-FAILED"

    def test_worker_event_id_to_name_covers_all_members(self) -> None:
        for evt_id in WorkerEventId:
            assert evt_id in WORKER_EVENT_ID_TO_NAME

    def test_lifecycle_event_id_to_name_covers_all_members(self) -> None:
        for evt_id in RuntimeLifecycleEventId:
            assert evt_id in RUNTIME_LIFECYCLE_EVENT_ID_TO_NAME


class TestEventNamePresence:
    @pytest.mark.parametrize(
        "evt_name",
        [
            "runtime.launch_succeeded",
            "runtime.launch_failed",
            "runtime.heartbeat",
        ],
    )
    def test_worker_event_names_present(self, evt_name: str) -> None:
        assert evt_name in CANONICAL_EVENT_NAMES

    @pytest.mark.parametrize(
        "evt_name",
        [
            "runtime.started",
            "runtime.degraded",
            "runtime.failed",
        ],
    )
    def test_lifecycle_event_names_present(self, evt_name: str) -> None:
        assert evt_name in CANONICAL_EVENT_NAMES

    def test_worker_event_name_enum_string_values(self) -> None:
        assert WorkerEventName.LAUNCH_SUCCEEDED == "runtime.launch_succeeded"
        assert WorkerEventName.LAUNCH_FAILED == "runtime.launch_failed"
        assert WorkerEventName.HEARTBEAT == "runtime.heartbeat"

    def test_lifecycle_event_name_enum_string_values(self) -> None:
        assert RuntimeLifecycleEventName.STARTED == "runtime.started"
        assert RuntimeLifecycleEventName.DEGRADED == "runtime.degraded"
        assert RuntimeLifecycleEventName.FAILED == "runtime.failed"


class TestPrivilegePresence:
    def test_substrate_privilege_present(self) -> None:
        assert InternalPrivilege.RUNTIME_SUBSTRATE == "internal:runtimes:substrate"
        assert "internal:runtimes:substrate" in CANONICAL_INTERNAL_PRIVILEGES

    def test_control_privilege_present(self) -> None:
        assert InternalPrivilege.RUNTIME_CONTROL == "internal:runtimes:control"
        assert "internal:runtimes:control" in CANONICAL_INTERNAL_PRIVILEGES

    def test_all_privilege_enum_members_in_collection(self) -> None:
        for priv in InternalPrivilege:
            assert priv.value in CANONICAL_INTERNAL_PRIVILEGES


class TestCanonicalStatePresence:
    @pytest.mark.parametrize(
        "state",
        ["STARTING", "RUNNING", "DEGRADED", "STOPPING", "STOPPED", "FAILED"],
    )
    def test_required_states_present(self, state: str) -> None:
        assert state in CANONICAL_RUNTIME_STATES

    def test_all_canonical_state_enum_members_in_collection(self) -> None:
        for s in CanonicalRuntimeState:
            assert s.value in CANONICAL_RUNTIME_STATES


class TestCanonicalReasonPresence:
    @pytest.mark.parametrize(
        "reason",
        ["LAUNCH_FAILED", "HEARTBEAT_TIMEOUT", "STOP_REQUESTED"],
    )
    def test_required_reasons_present(self, reason: str) -> None:
        assert reason in CANONICAL_RUNTIME_REASONS

    def test_all_canonical_reason_enum_members_in_collection(self) -> None:
        for r in CanonicalRuntimeReason:
            assert r.value in CANONICAL_RUNTIME_REASONS


class TestCanonicalModePresence:
    @pytest.mark.parametrize("mode", ["PAPER", "LIVE", "BACKTEST"])
    def test_required_modes_present(self, mode: str) -> None:
        assert mode in CANONICAL_WORKER_MODES

    def test_all_worker_mode_enum_members_in_collection(self) -> None:
        for m in WorkerMode:
            assert m.value in CANONICAL_WORKER_MODES


class TestCanonicalErrorPresence:
    @pytest.mark.parametrize(
        "code",
        [
            "WORKER_INVALID_REQUEST",
            "WORKER_ARTIFACT_NOT_FOUND",
            "WORKER_BOOTSTRAP_FAILED",
            "WORKER_INTERNAL_ERROR",
            "WORKER_RUNTIME_COMPLETED",
            "WORKER_RUNTIME_FAILED",
            "WORKER_POLICY_VIOLATION",
        ],
    )
    def test_worker_error_codes_present(self, code: str) -> None:
        assert code in WORKER_CANONICAL_ERROR_CODES

    @pytest.mark.parametrize(
        "code",
        [
            "STRATEGY_VERSION_NOT_RUNNABLE",
            "RUNTIME_START_VALIDATION_FAILED",
            "RUNTIME_WORKER_START_FAILED",
            "RUNTIME_HEARTBEAT_MISSED",
            "RUNTIME_VERSION_NOT_APPROVED",
        ],
    )
    def test_shared_boundary_codes_present(self, code: str) -> None:
        assert code in SHARED_BOUNDARY_ERROR_CODES

    def test_all_worker_error_code_enum_members_in_collection(self) -> None:
        for code in WorkerErrorCode:
            assert code.value in WORKER_CANONICAL_ERROR_CODES

    def test_all_shared_boundary_code_enum_members_in_collection(self) -> None:
        for code in SharedBoundaryErrorCode:
            assert code.value in SHARED_BOUNDARY_ERROR_CODES

    def test_all_known_error_codes_is_union_of_both_sets(self) -> None:
        assert (
            ALL_KNOWN_ERROR_CODES
            == WORKER_CANONICAL_ERROR_CODES | SHARED_BOUNDARY_ERROR_CODES
        )


# ===========================================================================
# B. Domain uniqueness / registry consistency
# ===========================================================================


class TestDomainUniquenessAndConsistency:
    def test_no_duplicate_route_ids(self) -> None:
        flat = [item.value for item in WorkerRouteId] + [
            item.value for item in RuntimeManagerSignalId
        ]
        assert len(flat) == len(set(flat)), "Duplicate route IDs detected"

    def test_no_duplicate_event_ids(self) -> None:
        flat = [item.value for item in WorkerEventId] + [
            item.value for item in RuntimeLifecycleEventId
        ]
        assert len(flat) == len(set(flat)), "Duplicate event IDs detected"

    def test_no_duplicate_event_names(self) -> None:
        flat = [item.value for item in WorkerEventName] + [
            item.value for item in RuntimeLifecycleEventName
        ]
        assert len(flat) == len(set(flat)), "Duplicate event names detected"

    def test_no_duplicate_error_codes(self) -> None:
        worker = [item.value for item in WorkerErrorCode]
        shared = [item.value for item in SharedBoundaryErrorCode]
        combined = worker + shared
        assert len(combined) == len(set(combined)), "Duplicate error codes detected"

    def test_no_cross_domain_collisions_in_all_canonical_identifiers(self) -> None:
        """
        If any identifier value exists in two domains the frozenset union would
        shrink relative to the per-domain total — catch that here.
        """
        total_across_domains = sum(
            len(s) for s in CANONICAL_IDENTIFIERS_BY_DOMAIN.values()
        )
        assert len(ALL_CANONICAL_IDENTIFIERS) == total_across_domains, (
            "Cross-domain identifier collision: at least one value appears in "
            "more than one domain"
        )

    def test_all_canonical_identifiers_is_superset_of_every_domain(self) -> None:
        for domain, identifiers in CANONICAL_IDENTIFIERS_BY_DOMAIN.items():
            assert identifiers.issubset(ALL_CANONICAL_IDENTIFIERS), (
                f"Domain {domain!r} contains values absent from ALL_CANONICAL_IDENTIFIERS"
            )

    def test_find_identifier_domain_covers_every_identifier(self) -> None:
        for value in ALL_CANONICAL_IDENTIFIERS:
            domain = find_identifier_domain(value)
            assert domain is not None, f"Orphaned canonical identifier: {value!r}"

    @pytest.mark.parametrize(
        ("value", "expected_domain"),
        [
            ("ROUTE-WORKER-CREATE", "routes"),
            ("ROUTE-RUNTIME-HEARTBEAT", "routes"),
            ("internal:runtimes:substrate", "privileges"),
            ("internal:runtimes:control", "privileges"),
            ("EVT-WORKER-HEARTBEAT", "event_ids"),
            ("EVT-RUNTIME-FAILED", "event_ids"),
            ("runtime.heartbeat", "event_names"),
            ("runtime.failed", "event_names"),
            ("RUNNING", "states"),
            ("STOPPED", "states"),
            ("LAUNCH_FAILED", "reasons"),
            ("STOP_REQUESTED", "reasons"),
            ("WORKER_INTERNAL_ERROR", "errors"),
            ("RUNTIME_HEARTBEAT_MISSED", "errors"),
            ("PAPER", "modes"),
            ("BACKTEST", "modes"),
        ],
    )
    def test_find_identifier_domain_returns_correct_domain(
        self, value: str, expected_domain: str
    ) -> None:
        assert find_identifier_domain(value) == expected_domain

    def test_find_identifier_domain_returns_none_for_unknown(self) -> None:
        assert find_identifier_domain("TOTALLY_UNKNOWN_IDENTIFIER") is None

    def test_canonical_identifiers_by_domain_routes_matches_source(self) -> None:
        assert CANONICAL_IDENTIFIERS_BY_DOMAIN["routes"] == CANONICAL_ROUTE_IDS

    def test_canonical_identifiers_by_domain_event_ids_matches_source(self) -> None:
        assert CANONICAL_IDENTIFIERS_BY_DOMAIN["event_ids"] == CANONICAL_EVENT_IDS

    def test_canonical_identifiers_by_domain_event_names_matches_source(self) -> None:
        assert CANONICAL_IDENTIFIERS_BY_DOMAIN["event_names"] == CANONICAL_EVENT_NAMES

    def test_canonical_identifiers_by_domain_states_matches_source(self) -> None:
        assert CANONICAL_IDENTIFIERS_BY_DOMAIN["states"] == CANONICAL_RUNTIME_STATES

    def test_canonical_identifiers_by_domain_reasons_matches_source(self) -> None:
        assert CANONICAL_IDENTIFIERS_BY_DOMAIN["reasons"] == CANONICAL_RUNTIME_REASONS

    def test_canonical_identifiers_by_domain_errors_is_union_of_sources(self) -> None:
        expected = WORKER_CANONICAL_ERROR_CODES | SHARED_BOUNDARY_ERROR_CODES
        assert CANONICAL_IDENTIFIERS_BY_DOMAIN["errors"] == expected

    def test_canonical_identifiers_by_domain_modes_matches_source(self) -> None:
        assert CANONICAL_IDENTIFIERS_BY_DOMAIN["modes"] == CANONICAL_WORKER_MODES

    def test_get_canonical_identifiers_no_domain_returns_all(self) -> None:
        assert get_canonical_identifiers() == ALL_CANONICAL_IDENTIFIERS

    def test_get_canonical_identifiers_with_domain_returns_domain_subset(self) -> None:
        for domain in CANONICAL_IDENTIFIERS_BY_DOMAIN:
            result = get_canonical_identifiers(domain)  # type: ignore[arg-type]
            assert result == CANONICAL_IDENTIFIERS_BY_DOMAIN[domain]

    def test_is_shared_identifier_domain_true_for_all_shared_domains(self) -> None:
        shared_domains = {
            "routes",
            "privileges",
            "event_ids",
            "event_names",
            "states",
            "reasons",
            "errors",
        }
        for domain in shared_domains:
            assert is_shared_identifier_domain(domain) is True  # type: ignore[arg-type]

    def test_is_shared_identifier_domain_false_for_modes(self) -> None:
        assert is_shared_identifier_domain("modes") is False  # type: ignore[arg-type]


# ===========================================================================
# C. Validator helper behaviour
# ===========================================================================


class TestIsCanonicalIdentifier:
    @pytest.mark.parametrize(
        "value",
        [
            "ROUTE-WORKER-CREATE",
            "ROUTE-WORKER-STOP",
            "ROUTE-RUNTIME-HEARTBEAT",
            "internal:runtimes:substrate",
            "internal:runtimes:control",
            "EVT-WORKER-HEARTBEAT",
            "EVT-RUNTIME-FAILED",
            "runtime.heartbeat",
            "runtime.failed",
            "RUNNING",
            "FAILED",
            "LAUNCH_FAILED",
            "STOP_REQUESTED",
            "WORKER_INTERNAL_ERROR",
            "RUNTIME_HEARTBEAT_MISSED",
            "PAPER",
            "LIVE",
        ],
    )
    def test_known_identifiers_return_true(self, value: str) -> None:
        assert is_canonical_identifier(value) is True

    @pytest.mark.parametrize(
        "value",
        [
            "",
            "INVALID",
            "TERMINATED",
            "route-worker-create",  # wrong case
            "EVT_WORKER_HEARTBEAT",  # underscores instead of dashes
            "internal:runtimes",  # truncated privilege
            "UNKNOWN_ERROR_CODE",
            "runtime.launch_succeeded2",  # invented variant
        ],
    )
    def test_invalid_identifiers_return_false(self, value: str) -> None:
        assert is_canonical_identifier(value) is False

    def test_domain_filter_event_ids_accepts_event_id(self) -> None:
        assert (
            is_canonical_identifier("EVT-WORKER-HEARTBEAT", domain="event_ids") is True
        )

    def test_domain_filter_event_ids_rejects_globally_valid_event_name(self) -> None:
        # "runtime.heartbeat" is globally canonical but lives in event_names, not event_ids
        assert is_canonical_identifier("runtime.heartbeat", domain="event_ids") is False

    def test_domain_filter_event_names_accepts_event_name(self) -> None:
        assert is_canonical_identifier("runtime.failed", domain="event_names") is True

    def test_domain_filter_event_names_rejects_globally_valid_event_id(self) -> None:
        # "EVT-WORKER-HEARTBEAT" is globally canonical but lives in event_ids, not event_names
        assert (
            is_canonical_identifier("EVT-WORKER-HEARTBEAT", domain="event_names")
            is False
        )

    def test_domain_filter_states_accepts_state(self) -> None:
        assert is_canonical_identifier("RUNNING", domain="states") is True

    def test_domain_filter_states_rejects_mode(self) -> None:
        assert is_canonical_identifier("PAPER", domain="states") is False

    def test_domain_filter_routes_rejects_globally_valid_event_name(self) -> None:
        assert is_canonical_identifier("runtime.failed", domain="routes") is False

    def test_domain_filter_errors_accepts_worker_code(self) -> None:
        assert is_canonical_identifier("WORKER_INTERNAL_ERROR", domain="errors") is True

    def test_domain_filter_errors_accepts_shared_boundary_code(self) -> None:
        assert (
            is_canonical_identifier("RUNTIME_HEARTBEAT_MISSED", domain="errors") is True
        )

    def test_domain_filter_privileges_rejects_route(self) -> None:
        assert (
            is_canonical_identifier("ROUTE-WORKER-CREATE", domain="privileges") is False
        )


class TestRequireCanonicalIdentifier:
    @pytest.mark.parametrize(
        "value",
        [
            "ROUTE-WORKER-CREATE",
            "EVT-WORKER-LAUNCH-SUCCEEDED",
            "runtime.launch_failed",
            "STOPPED",
            "STOP_REQUESTED",
            "WORKER_ARTIFACT_NOT_FOUND",
            "LIVE",
        ],
    )
    def test_returns_original_string_for_valid_values(self, value: str) -> None:
        assert require_canonical_identifier(value) == value

    @pytest.mark.parametrize(
        "value",
        [
            "NOT_A_THING",
            "TERMINATED",
            "EVT.WORKER.HEARTBEAT",
            "",
        ],
    )
    def test_raises_value_error_for_invalid_values(self, value: str) -> None:
        with pytest.raises(ValueError):
            require_canonical_identifier(value)

    def test_raises_on_valid_global_but_wrong_domain(self) -> None:
        with pytest.raises(ValueError):
            require_canonical_identifier("runtime.failed", domain="event_ids")

    def test_returns_value_when_domain_matches(self) -> None:
        result = require_canonical_identifier(
            "EVT-WORKER-HEARTBEAT", domain="event_ids"
        )
        assert result == "EVT-WORKER-HEARTBEAT"

    def test_error_message_contains_non_canonical_label(self) -> None:
        with pytest.raises(ValueError, match="Non-canonical identifier"):
            require_canonical_identifier("TOTAL_GARBAGE")

    def test_error_message_includes_domain_when_specified(self) -> None:
        with pytest.raises(ValueError, match="domain="):
            require_canonical_identifier("runtime.failed", domain="event_ids")


class TestRequireCanonicalIdentifiers:
    def test_valid_list_returns_tuple_of_same_values(self) -> None:
        values = ["ROUTE-WORKER-CREATE", "ROUTE-WORKER-STOP"]
        result = require_canonical_identifiers(values)
        assert isinstance(result, tuple)
        assert set(result) == set(values)

    def test_empty_list_returns_empty_tuple(self) -> None:
        assert require_canonical_identifiers([]) == ()

    def test_mixed_valid_invalid_list_raises(self) -> None:
        with pytest.raises(ValueError):
            require_canonical_identifiers(["ROUTE-WORKER-CREATE", "TOTAL_GARBAGE"])

    def test_all_invalid_list_raises(self) -> None:
        with pytest.raises(ValueError):
            require_canonical_identifiers(["FAKE_A", "FAKE_B"])

    def test_domain_constrained_valid_list_passes(self) -> None:
        result = require_canonical_identifiers(
            ["EVT-WORKER-LAUNCH-SUCCEEDED", "EVT-WORKER-HEARTBEAT"],
            domain="event_ids",
        )
        assert len(result) == 2

    def test_domain_constrained_rejects_cross_domain_member(self) -> None:
        # "runtime.heartbeat" is globally canonical but not in event_ids
        with pytest.raises(ValueError):
            require_canonical_identifiers(
                ["runtime.heartbeat"],
                domain="event_ids",
            )


# ===========================================================================
# D. JSON helper behaviour
# ===========================================================================


class TestCanonicalEventEnvelope:
    _EVENT_ID = WorkerEventId.HEARTBEAT.value
    _EVENT_NAME = WorkerEventName.HEARTBEAT.value

    def _make(self, **overrides):  # type: ignore[no-untyped-def]
        kwargs: dict = dict(
            event_id=self._EVENT_ID,
            event_name=self._EVENT_NAME,
            producer="strategy-worker-runtime",
            payload={"runtime_id": "rt-abc123"},
        )
        kwargs.update(overrides)
        return canonical_event_envelope(**kwargs)

    def test_valid_input_returns_dict(self) -> None:
        assert isinstance(self._make(), dict)

    def test_envelope_contains_all_required_keys(self) -> None:
        result = self._make()
        for key in (
            "event_id",
            "event_name",
            "event_version",
            "producer",
            "occurred_at",
            "payload",
        ):
            assert key in result

    def test_event_id_and_name_preserved_verbatim(self) -> None:
        result = self._make()
        assert result["event_id"] == self._EVENT_ID
        assert result["event_name"] == self._EVENT_NAME

    def test_default_event_version_is_one(self) -> None:
        assert self._make()["event_version"] == 1

    def test_custom_event_version_accepted(self) -> None:
        assert self._make(event_version=3)["event_version"] == 3

    def test_event_version_zero_raises(self) -> None:
        with pytest.raises(ValueError):
            self._make(event_version=0)

    def test_non_canonical_event_name_raises(self) -> None:
        with pytest.raises(ValueError):
            self._make(event_name="not.a.canonical.name")

    def test_non_canonical_event_id_raises(self) -> None:
        with pytest.raises(ValueError):
            self._make(event_id="EVT-FAKE-EVENT")

    def test_event_id_from_event_names_domain_raises(self) -> None:
        # "runtime.heartbeat" is canonical but belongs to event_names, not event_ids
        with pytest.raises(ValueError):
            self._make(event_id=WorkerEventName.HEARTBEAT.value)

    def test_event_name_from_event_ids_domain_raises(self) -> None:
        # "EVT-WORKER-HEARTBEAT" is canonical but belongs to event_ids, not event_names
        with pytest.raises(ValueError):
            self._make(event_name=WorkerEventId.HEARTBEAT.value)

    def test_optional_fields_included_when_provided(self) -> None:
        result = self._make(
            correlation_id="corr-123",
            causation_id="caus-456",
            runtime_id="rt-abc",
            tenant_id="tenant-x",
            account_id="acc-y",
            worker_identity="wid-z",
            launch_attempt=2,
            strategy_version_id="sv-1",
        )
        assert result["correlation_id"] == "corr-123"
        assert result["causation_id"] == "caus-456"
        assert result["runtime_id"] == "rt-abc"
        assert result["tenant_id"] == "tenant-x"
        assert result["account_id"] == "acc-y"
        assert result["worker_identity"] == "wid-z"
        assert result["launch_attempt"] == 2
        assert result["strategy_version_id"] == "sv-1"

    def test_optional_fields_absent_when_not_provided(self) -> None:
        result = self._make()
        for optional_key in (
            "correlation_id",
            "causation_id",
            "runtime_id",
            "tenant_id",
        ):
            assert optional_key not in result

    def test_explicit_occurred_at_preserved(self) -> None:
        ts = "2024-01-15T10:00:00Z"
        assert self._make(occurred_at=ts)["occurred_at"] == ts

    def test_invalid_occurred_at_raises(self) -> None:
        with pytest.raises(ValueError):
            self._make(occurred_at="not-a-timestamp")

    def test_all_worker_event_id_name_pairs_produce_valid_envelopes(self) -> None:
        for evt_id, evt_name in WORKER_EVENT_ID_TO_NAME.items():
            result = canonical_event_envelope(
                event_id=evt_id.value,
                event_name=evt_name.value,
                producer="strategy-worker-runtime",
                payload={},
            )
            assert result["event_id"] == evt_id.value
            assert result["event_name"] == evt_name.value


class TestCanonicalRuntimeStatePayload:
    def test_valid_state_returns_payload_with_runtime_id_and_state(self) -> None:
        result = canonical_runtime_state_payload(
            runtime_id="rt-abc",
            state=CanonicalRuntimeState.RUNNING.value,
        )
        assert result["runtime_id"] == "rt-abc"
        assert result["state"] == "RUNNING"

    def test_valid_state_with_reason_code_included(self) -> None:
        result = canonical_runtime_state_payload(
            runtime_id="rt-abc",
            state=CanonicalRuntimeState.FAILED.value,
            reason_code=CanonicalRuntimeReason.LAUNCH_FAILED.value,
        )
        assert result["reason_code"] == "LAUNCH_FAILED"

    def test_invalid_state_raises(self) -> None:
        with pytest.raises(ValueError):
            canonical_runtime_state_payload(runtime_id="rt-abc", state="INVALID_STATE")

    def test_non_canonical_state_terminated_raises(self) -> None:
        with pytest.raises(ValueError):
            canonical_runtime_state_payload(runtime_id="rt-abc", state="TERMINATED")

    def test_invalid_reason_code_raises(self) -> None:
        with pytest.raises(ValueError):
            canonical_runtime_state_payload(
                runtime_id="rt-abc",
                state=CanonicalRuntimeState.FAILED.value,
                reason_code="NOT_A_REASON",
            )

    def test_optional_fields_included_when_provided(self) -> None:
        ts = "2024-03-01T12:00:00Z"
        result = canonical_runtime_state_payload(
            runtime_id="rt-abc",
            state=CanonicalRuntimeState.RUNNING.value,
            worker_identity="worker-1",
            launch_attempt=2,
            occurred_at=ts,
            observed_at=ts,
            correlation_id="corr-xyz",
            causation_id="caus-xyz",
        )
        assert result["worker_identity"] == "worker-1"
        assert result["launch_attempt"] == 2
        assert result["occurred_at"] == ts
        assert result["observed_at"] == ts
        assert result["correlation_id"] == "corr-xyz"
        assert result["causation_id"] == "caus-xyz"

    def test_optional_fields_absent_when_not_provided(self) -> None:
        result = canonical_runtime_state_payload(
            runtime_id="rt-abc",
            state=CanonicalRuntimeState.RUNNING.value,
        )
        for optional_key in (
            "reason_code",
            "worker_identity",
            "occurred_at",
            "observed_at",
        ):
            assert optional_key not in result

    def test_invalid_occurred_at_raises(self) -> None:
        with pytest.raises(ValueError):
            canonical_runtime_state_payload(
                runtime_id="rt-abc",
                state=CanonicalRuntimeState.RUNNING.value,
                occurred_at="not-a-date",
            )

    @pytest.mark.parametrize("state", list(CanonicalRuntimeState))
    def test_all_canonical_states_accepted(self, state: CanonicalRuntimeState) -> None:
        # The JSON helper validates only that the state value is canonical;
        # it does not enforce state/reason pairing — that is validate_state_reason_pair's job.
        result = canonical_runtime_state_payload(
            runtime_id="rt-test", state=state.value
        )
        assert result["state"] == state.value


class TestCanonicalErrorBody:
    def test_valid_worker_error_code_returns_body(self) -> None:
        result = canonical_error_body(
            code=WorkerErrorCode.INTERNAL_ERROR.value,
            message="Something went wrong.",
        )
        assert "error" in result
        assert result["error"]["code"] == "WORKER_INTERNAL_ERROR"
        assert result["error"]["message"] == "Something went wrong."

    def test_valid_shared_boundary_code_accepted(self) -> None:
        result = canonical_error_body(
            code=SharedBoundaryErrorCode.RUNTIME_HEARTBEAT_MISSED.value,
            message="Heartbeat was missed.",
        )
        assert result["error"]["code"] == "RUNTIME_HEARTBEAT_MISSED"

    def test_non_canonical_code_raises(self) -> None:
        with pytest.raises(ValueError):
            canonical_error_body(code="TOTALLY_FAKE_CODE", message="bad")

    def test_empty_code_raises(self) -> None:
        with pytest.raises(ValueError):
            canonical_error_body(code="", message="bad")

    def test_retryable_false_by_default(self) -> None:
        result = canonical_error_body(
            code=WorkerErrorCode.INTERNAL_ERROR.value, message="msg"
        )
        assert result["error"]["retryable"] is False

    def test_retryable_can_be_set_true(self) -> None:
        result = canonical_error_body(
            code=WorkerErrorCode.BOOTSTRAP_FAILED.value,
            message="bootstrap failed",
            retryable=True,
        )
        assert result["error"]["retryable"] is True

    def test_correlation_id_included_when_provided(self) -> None:
        result = canonical_error_body(
            code=WorkerErrorCode.NOT_FOUND.value,
            message="not found",
            correlation_id="corr-999",
        )
        assert result["error"]["correlation_id"] == "corr-999"

    def test_details_defaults_to_empty_dict(self) -> None:
        result = canonical_error_body(
            code=WorkerErrorCode.INTERNAL_ERROR.value, message="msg"
        )
        assert result["error"]["details"] == {}

    def test_details_dict_preserved(self) -> None:
        result = canonical_error_body(
            code=WorkerErrorCode.INVALID_REQUEST.value,
            message="bad request",
            details={"field": "mode", "reason": "unsupported"},
        )
        assert result["error"]["details"]["field"] == "mode"

    def test_details_list_preserved(self) -> None:
        result = canonical_error_body(
            code=WorkerErrorCode.INVALID_REQUEST.value,
            message="bad request",
            details=[{"field": "x"}],
        )
        assert result["error"]["details"] == [{"field": "x"}]

    @pytest.mark.parametrize("code", list(WorkerErrorCode))
    def test_all_worker_error_codes_accepted(self, code: WorkerErrorCode) -> None:
        result = canonical_error_body(code=code.value, message="test")
        assert result["error"]["code"] == code.value

    @pytest.mark.parametrize("code", list(SharedBoundaryErrorCode))
    def test_all_shared_boundary_codes_accepted(
        self, code: SharedBoundaryErrorCode
    ) -> None:
        result = canonical_error_body(code=code.value, message="test")
        assert result["error"]["code"] == code.value


class TestRequireIso8601Timestamp:
    @pytest.mark.parametrize(
        "ts",
        [
            "2024-01-15T10:00:00Z",
            "2024-01-15T10:00:00+00:00",
            "2024-01-15T10:00:00+05:30",
            "2024-01-15T10:00:00",
            "2024-01-15T10:00:00.000Z",
            "2024-01-15T10:00:00.123456Z",
        ],
    )
    def test_valid_iso8601_strings_pass(self, ts: str) -> None:
        assert require_iso8601_timestamp(ts, field_name="ts") == ts

    @pytest.mark.parametrize(
        "ts",
        [
            "not-a-timestamp",
            "2024/01/15",
            "15-01-2024",
            "2024-13-01T00:00:00Z",  # month 13 is invalid
            "",
            "  ",
        ],
    )
    def test_invalid_strings_raise_value_error(self, ts: str) -> None:
        with pytest.raises(ValueError):
            require_iso8601_timestamp(ts, field_name="ts")

    def test_non_string_value_raises_value_error(self) -> None:
        with pytest.raises(ValueError):
            require_iso8601_timestamp(12345, field_name="ts")  # type: ignore[arg-type]


# ===========================================================================
# E. Worker/runtime semantics guardrails
# ===========================================================================


class TestSemanticGuardrails:
    def test_terminated_not_in_canonical_runtime_states(self) -> None:
        """
        TERMINATED appears only as a RuntimeManagerSignalId route value.
        It must never leak into the shared canonical runtime-state contract.
        """
        assert "TERMINATED" not in CANONICAL_RUNTIME_STATES
        assert "TERMINATED" not in {s.value for s in CanonicalRuntimeState}

    def test_terminated_not_in_worker_local_phases(self) -> None:
        assert "TERMINATED" not in {p.value for p in WorkerLocalPhase}

    def test_worker_internal_phases_initializing_and_ready_absent_from_canonical_states(
        self,
    ) -> None:
        """
        INITIALIZING and READY are worker-local phases with no shared canonical
        equivalent — they map to STARTING for external reporting purposes.
        """
        assert "INITIALIZING" not in CANONICAL_RUNTIME_STATES
        assert "READY" not in CANONICAL_RUNTIME_STATES

    def test_worker_only_phases_disjoint_from_canonical_state_values(self) -> None:
        canonical_values = {s.value for s in CanonicalRuntimeState}
        worker_only = {
            WorkerLocalPhase.INITIALIZING.value,
            WorkerLocalPhase.READY.value,
        }
        assert worker_only.isdisjoint(canonical_values)

    def test_initializing_maps_to_starting_canonical_state(self) -> None:
        assert (
            to_default_canonical_state(WorkerLocalPhase.INITIALIZING)
            == CanonicalRuntimeState.STARTING
        )

    def test_ready_maps_to_starting_canonical_state(self) -> None:
        assert (
            to_default_canonical_state(WorkerLocalPhase.READY)
            == CanonicalRuntimeState.STARTING
        )

    def test_completed_maps_to_stopped_canonical_state(self) -> None:
        assert (
            to_default_canonical_state(WorkerLocalPhase.COMPLETED)
            == CanonicalRuntimeState.STOPPED
        )

    def test_terminal_canonical_states_contain_stopped_and_failed(self) -> None:
        assert CanonicalRuntimeState.STOPPED in TERMINAL_CANONICAL_STATES
        assert CanonicalRuntimeState.FAILED in TERMINAL_CANONICAL_STATES

    def test_terminal_local_phases_contain_stopped_and_failed(self) -> None:
        assert WorkerLocalPhase.STOPPED in TERMINAL_LOCAL_PHASES
        assert WorkerLocalPhase.COMPLETED in TERMINAL_LOCAL_PHASES
        assert WorkerLocalPhase.FAILED in TERMINAL_LOCAL_PHASES

    @pytest.mark.parametrize(
        "state",
        [
            CanonicalRuntimeState.RUNNING,
            CanonicalRuntimeState.STARTING,
            CanonicalRuntimeState.DEGRADED,
            CanonicalRuntimeState.STOPPING,
        ],
    )
    def test_active_states_not_in_terminal_set(
        self, state: CanonicalRuntimeState
    ) -> None:
        assert state not in TERMINAL_CANONICAL_STATES

    def test_is_terminal_canonical_state_stopped(self) -> None:
        assert is_terminal_canonical_state(CanonicalRuntimeState.STOPPED) is True

    def test_is_terminal_canonical_state_running(self) -> None:
        assert is_terminal_canonical_state(CanonicalRuntimeState.RUNNING) is False

    def test_is_terminal_local_phase_stopped(self) -> None:
        assert is_terminal_local_phase(WorkerLocalPhase.STOPPED) is True

    def test_is_terminal_local_phase_completed(self) -> None:
        assert is_terminal_local_phase(WorkerLocalPhase.COMPLETED) is True

    def test_is_terminal_local_phase_running(self) -> None:
        assert is_terminal_local_phase(WorkerLocalPhase.RUNNING) is False

    def test_valid_state_reason_pair_accepted(self) -> None:
        validate_state_reason_pair(
            CanonicalRuntimeState.FAILED,
            CanonicalRuntimeReason.LAUNCH_FAILED,
        )

    def test_valid_stopping_reason_accepted(self) -> None:
        validate_state_reason_pair(
            CanonicalRuntimeState.STOPPING,
            CanonicalRuntimeReason.STOP_REQUESTED,
        )

    def test_wrong_reason_for_failed_state_raises(self) -> None:
        with pytest.raises(ValueError):
            validate_state_reason_pair(
                CanonicalRuntimeState.FAILED,
                CanonicalRuntimeReason.STOP_REQUESTED,
            )

    def test_missing_required_reason_for_failed_raises(self) -> None:
        with pytest.raises(ValueError):
            validate_state_reason_pair(CanonicalRuntimeState.FAILED, None)

    def test_reason_on_state_that_forbids_it_raises(self) -> None:
        with pytest.raises(ValueError):
            validate_state_reason_pair(
                CanonicalRuntimeState.RUNNING,
                CanonicalRuntimeReason.LAUNCH_FAILED,
            )

    def test_no_reason_on_unrestricted_state_passes(self) -> None:
        validate_state_reason_pair(CanonicalRuntimeState.RUNNING, None)
        validate_state_reason_pair(CanonicalRuntimeState.STARTING, None)

    def test_valid_state_reasons_mapping_covers_expected_states(self) -> None:
        assert CanonicalRuntimeState.FAILED in VALID_STATE_REASONS
        assert CanonicalRuntimeState.STOPPING in VALID_STATE_REASONS

    def test_runtime_status_with_canonical_state_and_reason(self) -> None:
        status = RuntimeStatus(local_phase=WorkerLocalPhase.FAILED)
        updated = status.with_canonical_state(
            CanonicalRuntimeState.FAILED,
            reason_code=CanonicalRuntimeReason.HEARTBEAT_TIMEOUT,
        )
        assert updated.canonical_state == CanonicalRuntimeState.FAILED
        assert updated.reason_code == CanonicalRuntimeReason.HEARTBEAT_TIMEOUT

    def test_runtime_status_rejects_invalid_state_reason_pair(self) -> None:
        status = RuntimeStatus(local_phase=WorkerLocalPhase.RUNNING)
        with pytest.raises(ValueError):
            status.with_canonical_state(
                CanonicalRuntimeState.RUNNING,
                reason_code=CanonicalRuntimeReason.LAUNCH_FAILED,
            )

    def test_is_canonical_runtime_state_true_for_running(self) -> None:
        assert is_canonical_runtime_state("RUNNING") is True

    def test_is_canonical_runtime_state_false_for_terminated(self) -> None:
        assert is_canonical_runtime_state("TERMINATED") is False

    def test_require_canonical_runtime_state_returns_value_for_valid(self) -> None:
        assert require_canonical_runtime_state("FAILED") == "FAILED"

    def test_require_canonical_runtime_state_raises_for_terminated(self) -> None:
        with pytest.raises(ValueError):
            require_canonical_runtime_state("TERMINATED")

    def test_is_canonical_runtime_reason_true_for_launch_failed(self) -> None:
        assert is_canonical_runtime_reason("LAUNCH_FAILED") is True

    def test_is_canonical_runtime_reason_false_for_unknown(self) -> None:
        assert is_canonical_runtime_reason("UNKNOWN_REASON") is False

    def test_require_canonical_runtime_reason_raises_for_invalid(self) -> None:
        with pytest.raises(ValueError):
            require_canonical_runtime_reason("UNKNOWN_REASON")


# ===========================================================================
# F. CI / lint-facing negative coverage
# ===========================================================================


class TestLintAndCiGuardrails:
    """
    This class exercises the domain-classification helpers that a CI forbidden-
    literal scan can delegate to.

    TODO(SWR-101): Once a source-scanner utility is wired into the lint pipeline
    (e.g. ``strategy_worker_runtime.utils.lint.scan_forbidden_literals``), add
    parametrized tests here that pass sample source strings containing raw
    canonical literals and assert the scanner flags them. Example shape:

        def test_scanner_flags_raw_state_literal():
            from runtime.utils.lint import scan_forbidden_literals
            violations = scan_forbidden_literals(
                source='state = "RUNNING"',   # raw literal, should use CanonicalRuntimeState
                domain="states",
            )
            assert len(violations) > 0

    The tests below use the currently-available helpers as a narrow proxy.
    """

    def test_is_worker_error_code_accepts_entire_worker_namespace(self) -> None:
        for code in WorkerErrorCode:
            assert is_worker_error_code(code.value) is True

    def test_is_worker_error_code_rejects_shared_namespace(self) -> None:
        for code in SharedBoundaryErrorCode:
            assert is_worker_error_code(code.value) is False

    def test_is_shared_boundary_error_code_accepts_shared_namespace(self) -> None:
        for code in SharedBoundaryErrorCode:
            assert is_shared_boundary_error_code(code.value) is True

    def test_is_shared_boundary_error_code_rejects_worker_namespace(self) -> None:
        for code in WorkerErrorCode:
            assert is_shared_boundary_error_code(code.value) is False

    def test_is_known_error_code_accepts_both_namespaces(self) -> None:
        for code in WorkerErrorCode:
            assert is_known_error_code(code.value) is True
        for shared_code in SharedBoundaryErrorCode:
            assert is_known_error_code(shared_code.value) is True

    @pytest.mark.parametrize(
        "invented_literal",
        [
            "WORKER_FAKE_ERROR",
            "RUNTIME_FAKE_ERROR",
            "UNKNOWN",
            "SOME_INVENTED_CODE",
            "",
        ],
    )
    def test_is_known_error_code_rejects_invented_literals(
        self, invented_literal: str
    ) -> None:
        assert is_known_error_code(invented_literal) is False

    def test_is_canonical_identifier_rejects_invented_route_literal(self) -> None:
        # Simulates a forbidden literal that a developer might hard-code
        assert is_canonical_identifier("ROUTE-WORKER-EXECUTE") is False

    def test_is_canonical_identifier_rejects_invented_event_id_literal(self) -> None:
        assert is_canonical_identifier("EVT-WORKER-CUSTOM-ACTION") is False

    def test_is_canonical_identifier_rejects_invented_state_literal(self) -> None:
        assert is_canonical_identifier("WARMING_UP") is False

from __future__ import annotations

from typing import Final, Literal

from runtime.domain.enums import (
    CANONICAL_EVENT_IDS,
    CANONICAL_EVENT_NAMES,
    CANONICAL_INTERNAL_PRIVILEGES,
    CANONICAL_ROUTE_IDS,
    CANONICAL_WORKER_MODES,
)
from runtime.domain.errors import (
    SHARED_BOUNDARY_ERROR_CODES,
    WORKER_CANONICAL_ERROR_CODES,
)
from runtime.domain.runtime_state import (
    CANONICAL_RUNTIME_REASONS,
    CANONICAL_RUNTIME_STATES,
)

CanonicalIdentifierDomain = Literal[
    "routes",
    "privileges",
    "event_ids",
    "event_names",
    "states",
    "reasons",
    "errors",
    "modes",
]


CANONICAL_IDENTIFIERS_BY_DOMAIN: Final[
    dict[CanonicalIdentifierDomain, frozenset[str]]
] = {
    "routes": CANONICAL_ROUTE_IDS,
    "privileges": CANONICAL_INTERNAL_PRIVILEGES,
    "event_ids": CANONICAL_EVENT_IDS,
    "event_names": CANONICAL_EVENT_NAMES,
    "states": CANONICAL_RUNTIME_STATES,
    "reasons": CANONICAL_RUNTIME_REASONS,
    "errors": frozenset().union(
        WORKER_CANONICAL_ERROR_CODES,
        SHARED_BOUNDARY_ERROR_CODES,
    ),
    "modes": CANONICAL_WORKER_MODES,
}

ALL_CANONICAL_IDENTIFIERS: Final[frozenset[str]] = frozenset().union(
    *CANONICAL_IDENTIFIERS_BY_DOMAIN.values()
)


def get_canonical_identifiers(
    domain: CanonicalIdentifierDomain | None = None,
) -> frozenset[str]:
    if domain is None:
        return ALL_CANONICAL_IDENTIFIERS
    return CANONICAL_IDENTIFIERS_BY_DOMAIN[domain]


def is_canonical_identifier(
    value: str,
    *,
    domain: CanonicalIdentifierDomain | None = None,
) -> bool:
    if domain is None:
        return value in ALL_CANONICAL_IDENTIFIERS
    return value in CANONICAL_IDENTIFIERS_BY_DOMAIN[domain]


def require_canonical_identifier(
    value: str,
    *,
    domain: CanonicalIdentifierDomain | None = None,
) -> str:
    if not is_canonical_identifier(value, domain=domain):
        expected = (
            sorted(CANONICAL_IDENTIFIERS_BY_DOMAIN[domain])
            if domain is not None
            else sorted(ALL_CANONICAL_IDENTIFIERS)
        )
        raise ValueError(
            f"Non-canonical identifier: {value!r}; "
            f"domain={domain!r}; "
            f"allowed={expected}"
        )
    return value


def require_canonical_identifiers(
    values: list[str] | tuple[str, ...] | set[str],
    *,
    domain: CanonicalIdentifierDomain | None = None,
) -> tuple[str, ...]:
    return tuple(require_canonical_identifier(value, domain=domain) for value in values)


def find_identifier_domain(value: str) -> CanonicalIdentifierDomain | None:
    for domain, allowed in CANONICAL_IDENTIFIERS_BY_DOMAIN.items():
        if value in allowed:
            return domain
    return None


def is_shared_identifier_domain(domain: CanonicalIdentifierDomain) -> bool:
    return domain in {
        "routes",
        "privileges",
        "event_ids",
        "event_names",
        "states",
        "reasons",
        "errors",
    }

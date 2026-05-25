"""Internal HTTP caller/trust validation (application; domain errors only)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

from runtime.domain.enums import InternalPrivilege
from runtime.domain.errors import WorkerInternalCallerNotAllowedError

TRUST_CLASS_SUBSTRATE = InternalPrivilege.RUNTIME_SUBSTRATE.value
TRUST_CLASS_CONTROL = InternalPrivilege.RUNTIME_CONTROL.value

_CALLER_HEADER_CANDIDATES = (
    "x-internal-caller",
    "internal-caller",
    "internal_caller",
)
_TRUST_CLASS_HEADER_CANDIDATES = (
    "x-internal-trust-class",
    "internal-trust-class",
    "internal_trust_class",
)


@dataclass(frozen=True, slots=True)
class InternalAuthContext:
    caller: str
    trust_class: str
    metadata: Mapping[str, str]

    @property
    def is_trusted_internal(self) -> bool:
        return bool(self.caller) and self.trust_class in {
            TRUST_CLASS_SUBSTRATE,
            TRUST_CLASS_CONTROL,
        }


def _normalize_metadata(
    metadata: Mapping[str, str] | Iterable[tuple[str, str]],
) -> dict[str, str]:
    if isinstance(metadata, Mapping):
        pairs: Iterable[tuple[str, str]] = list(metadata.items())
    else:
        pairs = metadata

    out: dict[str, str] = {}
    for key, value in pairs:
        out[str(key).lower()] = str(value)
    return out


def _first_present(
    metadata: Mapping[str, str], candidates: tuple[str, ...]
) -> str | None:
    for key in candidates:
        value = metadata.get(key)
        if value:
            return value
    return None


def parse_internal_auth_metadata(
    metadata: Mapping[str, str] | Iterable[tuple[str, str]],
) -> InternalAuthContext:
    normalized = _normalize_metadata(metadata)
    caller = _first_present(normalized, _CALLER_HEADER_CANDIDATES)
    trust_class = _first_present(normalized, _TRUST_CLASS_HEADER_CANDIDATES)
    if caller is None or trust_class is None:
        raise WorkerInternalCallerNotAllowedError(
            caller=caller,
            operation="internal_transport_call",
            required_privilege="internal metadata headers",
        )
    return InternalAuthContext(
        caller=caller,
        trust_class=trust_class,
        metadata=normalized,
    )


def require_internal_substrate(auth: InternalAuthContext, *, operation: str) -> None:
    if auth.trust_class != TRUST_CLASS_SUBSTRATE:
        raise WorkerInternalCallerNotAllowedError(
            caller=auth.caller,
            operation=operation,
            required_privilege=TRUST_CLASS_SUBSTRATE,
        )


def require_internal_control(auth: InternalAuthContext, *, operation: str) -> None:
    if auth.trust_class != TRUST_CLASS_CONTROL:
        raise WorkerInternalCallerNotAllowedError(
            caller=auth.caller,
            operation=operation,
            required_privilege=TRUST_CLASS_CONTROL,
        )


def require_internal_health_trust(auth: InternalAuthContext, *, operation: str) -> None:
    if not auth.is_trusted_internal:
        raise WorkerInternalCallerNotAllowedError(
            caller=auth.caller,
            operation=operation,
            required_privilege="trusted internal caller",
        )

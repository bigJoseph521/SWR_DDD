"""Re-export internal HTTP auth from application (infrastructure compatibility shim)."""

from runtime.application.http.internal_auth import (
    InternalAuthContext,
    TRUST_CLASS_CONTROL,
    TRUST_CLASS_SUBSTRATE,
    parse_internal_auth_metadata,
    require_internal_control,
    require_internal_health_trust,
    require_internal_substrate,
)

__all__ = [
    "InternalAuthContext",
    "TRUST_CLASS_CONTROL",
    "TRUST_CLASS_SUBSTRATE",
    "parse_internal_auth_metadata",
    "require_internal_control",
    "require_internal_health_trust",
    "require_internal_substrate",
]

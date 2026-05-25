from __future__ import annotations

import json
from typing import Mapping

from runtime.domain.launch_spec import (
    LaunchSpec,
    LaunchSpecValidationError,
)
from runtime.domain.errors import RuntimeStartValidationFailedError


def _payload_fingerprint(payload: Mapping[str, object]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


class LaunchSpecValidator:
    """
    Strict launch payload validation with deterministic duplicate behavior.
    """

    def __init__(self) -> None:
        self._accepted_payloads: dict[tuple[str, int], str] = {}

    def validate(self, payload: Mapping[str, object]) -> LaunchSpec:
        try:
            launch_spec = LaunchSpec.from_payload(payload)
        except LaunchSpecValidationError as exc:
            raise RuntimeStartValidationFailedError(
                reason=exc.reason,
                field_errors=exc.field_errors,
            ) from exc

        dedupe_key = (launch_spec.runtime_id, launch_spec.launch_attempt)
        fingerprint = _payload_fingerprint(payload)
        existing_fingerprint = self._accepted_payloads.get(dedupe_key)

        if existing_fingerprint is None:
            self._accepted_payloads[dedupe_key] = fingerprint
            return launch_spec

        if existing_fingerprint == fingerprint:
            return launch_spec

        raise RuntimeStartValidationFailedError(
            reason="duplicate_key_payload_mismatch",
            field_errors={
                "payload": "payload_broadening_or_mutation_for_existing_runtime_launch_attempt"
            },
            runtime_id=launch_spec.runtime_id,
            launch_attempt=launch_spec.launch_attempt,
        )

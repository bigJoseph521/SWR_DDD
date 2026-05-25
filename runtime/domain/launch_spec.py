from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, cast

from runtime.domain.artifact_digest_policy import skip_artifact_digest_validation
from runtime.domain.enums import WorkerMode


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _require_string(
    *,
    payload: Mapping[str, object],
    field_name: str,
    required: bool,
    errors: dict[str, str],
) -> str | None:
    if field_name not in payload:
        if required:
            errors[field_name] = "required_field_missing"
        return None

    value = payload[field_name]
    if not isinstance(value, str):
        errors[field_name] = "wrong_type_expected_string"
        return None
    if not value:
        errors[field_name] = "must_not_be_empty"
        return None
    if value != value.strip():
        errors[field_name] = "must_not_have_surrounding_whitespace"
        return None
    return value


class LaunchSpecValidationError(ValueError):
    def __init__(
        self, *, reason: str, field_errors: Mapping[str, str] | None = None
    ) -> None:
        super().__init__("LaunchSpec validation failed.")
        self.reason = reason
        self.field_errors = dict(field_errors or {})


@dataclass(frozen=True, slots=True)
class LaunchSpec:
    runtime_id: str
    tenant_id: str
    strategy_version_id: str
    mode: WorkerMode
    launch_attempt: int
    artifact_uri: str
    entrypoint: str
    trader_id: str | None = None
    account_id: str | None = None
    artifact_digest: str | None = None
    job_id: str | None = None
    ts_start: str | None = None
    ts_end: str | None = None
    #: Primary ticker from bundle ``parameters.symbol`` (see ``strategy_bundle/setting.json``).
    symbol: str | None = None
    #: Optional; when set, matches root ``correlation_id`` in ``strategy_bundle/setting.json``.
    correlation_id: str | None = None

    @property
    def artifact_reference(self) -> str:
        # Backward compatibility with pre-rename metadata field.
        return self.artifact_uri

    _REQUIRED_FIELDS = frozenset(
        {
            "runtime_id",
            "strategy_version_id",
            "mode",
            "launch_attempt",
            "entrypoint",
            # TEMPORARY (2026-05): digest not required while ``skip_artifact_digest_validation`` is
            # forced on fleet-wide (see ``digest_validation_env``).
            # "artifact_digest",
            "account_id",
        }
    )
    _OPTIONAL_FIELDS = frozenset(
        {
            "tenant_id",
            "trader_id",
            "artifact_digest",
            "artifact_uri",
            "artifact_reference",
            "validated_parameter_identity",
            "parameter_hash",
            "job_id",
            "deployment_id",
            "ts_start",
            "ts_end",
            "symbol",
            "correlation_id",
        }
    )
    _ALLOWED_FIELDS = _REQUIRED_FIELDS | _OPTIONAL_FIELDS

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> "LaunchSpec":
        if not isinstance(payload, Mapping):
            raise LaunchSpecValidationError(
                reason="payload_not_mapping",
                field_errors={"payload": "must_be_object_mapping"},
            )

        field_errors: dict[str, str] = {}

        # ``parameters`` may appear on bundle-shaped payloads; only ``parameters.symbol`` is read
        # for :attr:`symbol` (see ``raw_dict_to_launch_payload``). Other keys inside it are ignored.
        mode_raw_early = payload.get("mode")
        allow_backtest_only_keys = (
            isinstance(mode_raw_early, str) and mode_raw_early == "BACKTEST"
        )
        allowed_top_level = (
            cls._ALLOWED_FIELDS
            | frozenset({"parameters", "strategy_params", "initial_cash"})
            | (
                frozenset({"backtest_job_id"})
                if allow_backtest_only_keys
                else frozenset()
            )
        )
        unknown_fields = sorted(set(payload.keys()) - allowed_top_level)
        if unknown_fields:
            field_errors["payload"] = f"unknown_fields:{','.join(unknown_fields)}"

        if "strategy_params" in payload:
            spv = payload["strategy_params"]
            if spv is not None and not isinstance(spv, dict):
                field_errors["strategy_params"] = "wrong_type_expected_object_or_null"

        if "initial_cash" in payload:
            icv = payload["initial_cash"]
            if icv is not None and not isinstance(icv, dict):
                field_errors["initial_cash"] = "wrong_type_expected_object_or_null"

        runtime_id = _require_string(
            payload=payload,
            field_name="runtime_id",
            required=True,
            errors=field_errors,
        )
        tenant_id: str | None
        if "tenant_id" not in payload or payload["tenant_id"] is None:
            tenant_id = ""
        else:
            tv = payload["tenant_id"]
            if isinstance(tv, str):
                tenant_id = tv.strip()
            else:
                field_errors["tenant_id"] = "wrong_type_expected_string_or_null"
                tenant_id = ""
        strategy_version_id = _require_string(
            payload=payload,
            field_name="strategy_version_id",
            required=True,
            errors=field_errors,
        )
        artifact_uri_val = _require_string(
            payload=payload,
            field_name="artifact_uri",
            required=False,
            errors=field_errors,
        )
        artifact_ref_val = _require_string(
            payload=payload,
            field_name="artifact_reference",
            required=False,
            errors=field_errors,
        )
        if artifact_uri_val is not None:
            artifact_uri = artifact_uri_val
        elif artifact_ref_val is not None:
            artifact_uri = artifact_ref_val
        else:
            artifact_uri = None
            if (
                "artifact_uri" not in field_errors
                and "artifact_reference" not in field_errors
            ):
                field_errors["artifact_uri"] = "required_field_missing"
        entrypoint = _require_string(
            payload=payload,
            field_name="entrypoint",
            required=True,
            errors=field_errors,
        )

        trader_id = _require_string(
            payload=payload,
            field_name="trader_id",
            required=False,
            errors=field_errors,
        )
        account_id = _require_string(
            payload=payload,
            field_name="account_id",
            required=True,
            errors=field_errors,
        )
        if skip_artifact_digest_validation():
            ad_raw = payload.get("artifact_digest")
            if ad_raw is None:
                artifact_digest = None
            elif isinstance(ad_raw, str):
                stripped = ad_raw.strip()
                if not stripped:
                    artifact_digest = None
                elif ad_raw != ad_raw.strip():
                    field_errors["artifact_digest"] = (
                        "must_not_have_surrounding_whitespace"
                    )
                    artifact_digest = None
                else:
                    artifact_digest = stripped
            else:
                field_errors["artifact_digest"] = "wrong_type_expected_string"
                artifact_digest = None
        else:
            artifact_digest = _require_string(
                payload=payload,
                field_name="artifact_digest",
                required=True,
                errors=field_errors,
            )
        job_id = _require_string(
            payload=payload,
            field_name="job_id",
            required=False,
            errors=field_errors,
        )
        ts_start = _require_string(
            payload=payload,
            field_name="ts_start",
            required=False,
            errors=field_errors,
        )
        ts_end = _require_string(
            payload=payload,
            field_name="ts_end",
            required=False,
            errors=field_errors,
        )
        symbol = _require_string(
            payload=payload,
            field_name="symbol",
            required=False,
            errors=field_errors,
        )
        if symbol is None:
            params_sym = payload.get("parameters")
            if isinstance(params_sym, dict):
                nested = params_sym.get("symbol")
                if isinstance(nested, str) and nested.strip():
                    symbol = nested.strip()

        correlation_id: str | None = None
        if "correlation_id" in payload:
            cr = payload["correlation_id"]
            if isinstance(cr, str):
                cs = cr.strip()
                if cs:
                    correlation_id = cs
            elif cr is not None:
                field_errors["correlation_id"] = "wrong_type_expected_string"

        mode_raw = payload.get("mode")
        if mode_raw is None:
            field_errors["mode"] = "required_field_missing"
            mode = WorkerMode.PAPER
        elif not isinstance(mode_raw, str):
            field_errors["mode"] = "wrong_type_expected_string"
            mode = WorkerMode.PAPER
        else:
            try:
                mode = WorkerMode(mode_raw)
            except ValueError:
                field_errors["mode"] = f"invalid_mode:{mode_raw}"
                mode = WorkerMode.PAPER

        launch_attempt_raw = payload.get("launch_attempt")
        if launch_attempt_raw is None:
            field_errors["launch_attempt"] = "required_field_missing"
            launch_attempt = 1
        elif not _is_int(launch_attempt_raw):
            field_errors["launch_attempt"] = "wrong_type_expected_integer"
            launch_attempt = 1
        else:
            launch_attempt = cast(int, launch_attempt_raw)
            if launch_attempt < 1:
                field_errors["launch_attempt"] = "must_be_greater_or_equal_to_1"

        if mode is not WorkerMode.BACKTEST and (
            ts_start is not None or ts_end is not None
        ):
            field_errors["parameters"] = (
                "ts_start_ts_end_only_allowed_for_backtest_mode"
            )
        if mode is WorkerMode.BACKTEST and (ts_start is None or ts_end is None):
            field_errors["parameters"] = (
                "ts_start_and_ts_end_required_for_backtest_mode"
            )

        if field_errors:
            raise LaunchSpecValidationError(
                reason="launch_spec_invalid",
                field_errors=field_errors,
            )

        return cls(
            runtime_id=runtime_id or "",
            tenant_id=tenant_id or "",
            strategy_version_id=strategy_version_id or "",
            mode=mode,
            launch_attempt=launch_attempt,
            artifact_uri=artifact_uri or "",
            entrypoint=entrypoint or "",
            trader_id=trader_id,
            account_id=account_id,
            artifact_digest=artifact_digest,
            job_id=job_id,
            ts_start=ts_start,
            ts_end=ts_end,
            symbol=symbol,
            correlation_id=correlation_id,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "runtime_id": self.runtime_id,
            "tenant_id": self.tenant_id,
            "trader_id": self.trader_id,
            "account_id": self.account_id,
            "strategy_version_id": self.strategy_version_id,
            "mode": self.mode.value,
            "artifact_uri": self.artifact_uri,
            "artifact_digest": self.artifact_digest,
            "entrypoint": self.entrypoint,
            "launch_attempt": self.launch_attempt,
            "job_id": self.job_id,
            "ts_start": self.ts_start,
            "ts_end": self.ts_end,
            "symbol": self.symbol,
            "correlation_id": self.correlation_id,
        }

from __future__ import annotations

import importlib
import inspect
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Protocol

from alphovex_sdk.strategy import Strategy as AlphovexStrategy
import runtime.infrastructure.strategy_loader.run_mypy_validation as mypy_validation_runner
from runtime.infrastructure.strategy_loader.artifact_fetcher import (
    ArtifactFetcher,
    ArtifactFetchResult,
)
from runtime.infrastructure.strategy_loader.artifact_verifier import (
    ArtifactVerificationResult,
    ArtifactVerifier,
)
from runtime.infrastructure.strategy_loader.entrypoint_loader import (
    EntrypointLoader,
    EntrypointLoadResult,
)
from runtime.domain.bootstrap_failures import (
    BootstrapFailure,
    SDKContractFailure,
)
from runtime.domain.launch_spec import LaunchSpec


@dataclass(frozen=True, slots=True)
class SdkContractValidationResult:
    is_valid: bool
    sdk_version_marker: str | None
    validated_type: str
    details: Mapping[str, Any] = field(default_factory=dict)


class SdkContractValidator:
    """
    Deterministic, side-effect free strategy SDK contract checks.

    Default contract:
    - Entrypoint must resolve to a **class** (not a bare function).
    - That class must subclass ``alphovex_sdk.strategy.Strategy`` and be instantiable with
      no required constructor arguments beyond the base ``Strategy.__init__``.
    - Optional ``__strategy_sdk_version__`` is enforced when present.

    For tests or legacy artifacts, pass ``required_base_type=None`` and optionally
    ``required_methods=("run",)`` to require a ``run(context)``-style callable (including
    a module-level ``run`` function when ``run`` is listed).
    """

    def __init__(
        self,
        *,
        required_methods: tuple[str, ...] = (),
        required_sdk_major: int = 1,
        required_base_type: type[Any] | None = AlphovexStrategy,
        work_root: Path | None = None,
    ) -> None:
        self._required_methods = required_methods
        self._required_sdk_major = required_sdk_major
        self._required_base_type = required_base_type
        self._work_root = (work_root or Path.cwd()).resolve()

    def validate(self, entrypoint: EntrypointLoadResult) -> SdkContractValidationResult:
        candidate = entrypoint.symbol
        instance: Any
        validated_type: str

        if inspect.isclass(candidate):
            if self._required_base_type is not None and not issubclass(
                candidate, self._required_base_type
            ):
                self._sdk_failure(
                    reason_code="sdk_protocol_mismatch",
                    message="Resolved strategy class does not implement required protocol/base type.",
                    details={
                        "entrypoint": entrypoint.entrypoint_spec,
                        "required_base_type": self._required_base_type.__name__,
                        "resolved_type": candidate.__name__,
                    },
                )
            try:
                instance = candidate()
            except Exception as exc:
                self._sdk_failure(
                    reason_code="sdk_non_instantiable",
                    message="Resolved strategy class is not instantiable.",
                    details={
                        "entrypoint": entrypoint.entrypoint_spec,
                        "class_name": candidate.__name__,
                        "exception_type": type(exc).__name__,
                        "exception_message": str(exc),
                    },
                )
            validated_type = candidate.__name__
        else:
            if self._required_base_type is not None:
                self._sdk_failure(
                    reason_code="sdk_protocol_mismatch",
                    message=(
                        "Strategy entrypoint must resolve to a strategy class "
                        f"(subclass of {self._required_base_type.__name__}), not a bare callable."
                    ),
                    details={
                        "entrypoint": entrypoint.entrypoint_spec,
                        "required_base_type": self._required_base_type.__name__,
                        "resolved_kind": type(candidate).__name__,
                    },
                )
            instance = candidate
            validated_type = type(candidate).__name__

        self._validate_required_methods(instance=instance, entrypoint=entrypoint)
        mypy_validation = self._collect_mypy_validation(entrypoint=entrypoint)
        self._validate_mypy_compatibility(
            entrypoint=entrypoint,
            mypy_validation=mypy_validation,
        )
        marker = self._resolve_sdk_marker(candidate=candidate, instance=instance)
        self._validate_sdk_marker(
            marker=marker,
            entrypoint=entrypoint,
            extra_details=mypy_validation,
        )

        return SdkContractValidationResult(
            is_valid=True,
            sdk_version_marker=marker,
            validated_type=validated_type,
            details={
                "required_methods": self._required_methods,
                "required_base_type": (
                    None
                    if self._required_base_type is None
                    else self._required_base_type.__name__
                ),
            },
        )

    def _collect_mypy_validation(
        self, *, entrypoint: EntrypointLoadResult
    ) -> dict[str, Any] | None:
        module_file_raw = entrypoint.details.get("module_file")
        if not isinstance(module_file_raw, str) or not module_file_raw.strip():
            return None

        module_file = Path(module_file_raw).resolve()
        if not module_file.is_file() or module_file.suffix != ".py":
            return None

        runner = Path(mypy_validation_runner.__file__).resolve()

        proc = subprocess.run(
            [
                sys.executable,
                str(runner),
                str(module_file),
                "--work-root",
                str(self._work_root),
            ],
            capture_output=True,
            text=True,
            cwd=str(module_file.parent),
            check=False,
        )
        output = ((proc.stdout or "") + (proc.stderr or "")).strip()
        return {
            "entrypoint": entrypoint.entrypoint_spec,
            "module_file": str(module_file),
            "mypy_result_path": str(
                mypy_validation_runner.mypy_result_path_for(work_root=self._work_root)
            ),
            "mypy_exit_code": proc.returncode,
            "mypy_output": output[:4000],
        }

    def _validate_mypy_compatibility(
        self,
        *,
        entrypoint: EntrypointLoadResult,
        mypy_validation: Mapping[str, Any] | None,
    ) -> None:
        _ = entrypoint
        if mypy_validation is None:
            return
        if int(mypy_validation.get("mypy_exit_code", 0) or 0) == 0:
            return
        output = str(mypy_validation.get("mypy_output", ""))
        if "No module named mypy" in output or "No module named 'mypy'" in output:
            return
        self._sdk_failure(
            reason_code="sdk_protocol_mismatch",
            message="Strategy code failed mypy compatibility validation.",
            details=dict(mypy_validation),
        )

    def _validate_required_methods(
        self, *, instance: Any, entrypoint: EntrypointLoadResult
    ) -> None:
        for method_name in self._required_methods:
            method = self._resolve_method(
                instance=instance,
                method_name=method_name,
                entrypoint=entrypoint,
            )
            signature = inspect.signature(method)
            if not self._signature_is_compatible(
                method_name=method_name, signature=signature
            ):
                self._sdk_failure(
                    reason_code="sdk_invalid_signature",
                    message="SDK-required method signature is incompatible.",
                    details={
                        "entrypoint": entrypoint.entrypoint_spec,
                        "method": method_name,
                        "signature": str(signature),
                        "expected": self._expected_signature_description(method_name),
                    },
                )

    def _resolve_method(
        self,
        *,
        instance: Any,
        method_name: str,
        entrypoint: EntrypointLoadResult,
    ) -> Any:
        # Function entrypoints satisfy run(context) directly.
        if method_name == "run" and inspect.isfunction(instance):
            return instance
        method = getattr(instance, method_name, None)
        if method is None or not callable(method):
            self._sdk_failure(
                reason_code="sdk_missing_required_method",
                message="SDK-required method is missing.",
                details={
                    "entrypoint": entrypoint.entrypoint_spec,
                    "missing_method": method_name,
                    "resolved_type": type(instance).__name__,
                },
            )
        return method

    def _expected_signature_description(self, method_name: str) -> str:
        if method_name == "run":
            return (
                "legacy run(context): exactly one required positional parameter "
                "(in addition to self on methods), or use a Strategy subclass with the default validator"
            )
        return f"callable {method_name!r} present (no additional signature checks)"

    def _signature_is_compatible(
        self, *, method_name: str, signature: inspect.Signature
    ) -> bool:
        if method_name != "run":
            return True
        positional = [
            parameter
            for parameter in signature.parameters.values()
            if parameter.kind
            in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
        ]
        required = [
            param for param in positional if param.default is inspect.Parameter.empty
        ]
        # Legacy ``run(context)`` (module-level or static-like): one required argument.
        return len(required) == 1

    def _resolve_sdk_marker(self, *, candidate: Any, instance: Any) -> str | None:
        candidate_marker = getattr(candidate, "__strategy_sdk_version__", None)
        if isinstance(candidate_marker, str):
            return candidate_marker
        instance_marker = getattr(instance, "__strategy_sdk_version__", None)
        if isinstance(instance_marker, str):
            return instance_marker
        module_name = getattr(candidate, "__module__", None)
        if isinstance(module_name, str):
            try:
                module = importlib.import_module(module_name)
            except Exception:
                return None
            module_marker = getattr(module, "__strategy_sdk_version__", None)
            if isinstance(module_marker, str):
                return module_marker
        return None

    def _validate_sdk_marker(
        self,
        *,
        marker: str | None,
        entrypoint: EntrypointLoadResult,
        extra_details: Mapping[str, Any] | None = None,
    ) -> None:
        merged_details = dict(extra_details or {})
        if marker is None:
            return
        major_fragment = marker.split(".", 1)[0]
        if not major_fragment.isdigit():
            self._sdk_failure(
                reason_code="sdk_marker_invalid",
                message="SDK version marker is malformed.",
                details={
                    **merged_details,
                    "entrypoint": entrypoint.entrypoint_spec,
                    "sdk_version_marker": marker,
                },
            )
        if int(major_fragment) != self._required_sdk_major:
            self._sdk_failure(
                reason_code="sdk_marker_incompatible",
                message="SDK version marker is incompatible with runtime contract.",
                details={
                    **merged_details,
                    "entrypoint": entrypoint.entrypoint_spec,
                    "sdk_version_marker": marker,
                    "required_sdk_major": self._required_sdk_major,
                },
            )

    def _sdk_failure(
        self, *, reason_code: str, message: str, details: Mapping[str, Any]
    ) -> None:
        raise SDKContractFailure(
            reason_code=reason_code,
            message=message,
            retryable=False,
            details=details,
        )


@dataclass(frozen=True, slots=True)
class BootstrapPipelineSuccess:
    artifact: ArtifactFetchResult
    verification: ArtifactVerificationResult
    entrypoint: EntrypointLoadResult
    sdk_validation: SdkContractValidationResult


@dataclass(frozen=True, slots=True)
class BootstrapPipelineResult:
    success: bool
    success_payload: BootstrapPipelineSuccess | None = None
    failure: BootstrapFailure | None = None


class BootstrapPersistencePort(Protocol):
    def on_bootstrap_start(
        self, launch_spec: LaunchSpec, *, observed_at: datetime
    ) -> None: ...

    def on_bootstrap_failure(
        self,
        launch_spec: LaunchSpec,
        failure: BootstrapFailure,
        *,
        occurred_at: datetime,
        observed_at: datetime,
    ) -> None: ...

    def on_bootstrap_success(
        self,
        launch_spec: LaunchSpec,
        success: BootstrapPipelineSuccess,
        *,
        occurred_at: datetime,
        observed_at: datetime,
    ) -> None: ...


class BootstrapPipeline:
    def __init__(
        self,
        *,
        fetcher: ArtifactFetcher,
        verifier: ArtifactVerifier,
        entrypoint_loader: EntrypointLoader,
        sdk_validator: SdkContractValidator,
        persistence: BootstrapPersistencePort | None = None,
    ) -> None:
        self._fetcher = fetcher
        self._verifier = verifier
        self._entrypoint_loader = entrypoint_loader
        self._sdk_validator = sdk_validator
        self._persistence = persistence

    def _utc_now(self) -> datetime:
        return datetime.now(timezone.utc)

    def run(self, launch_spec: LaunchSpec) -> BootstrapPipelineResult:
        started_at = self._utc_now()
        if self._persistence is not None:
            self._persistence.on_bootstrap_start(launch_spec, observed_at=started_at)

        try:
            artifact = self._fetcher.fetch(launch_spec)
            verification = self._verifier.verify(artifact)
            entrypoint = self._entrypoint_loader.load(
                launch_spec=launch_spec,
                fetch_result=artifact,
            )
            sdk_validation = self._sdk_validator.validate(entrypoint)
        except BootstrapFailure as exc:
            observed_at = self._utc_now()
            if self._persistence is not None:
                self._persistence.on_bootstrap_failure(
                    launch_spec=launch_spec,
                    failure=exc,
                    occurred_at=observed_at,
                    observed_at=observed_at,
                )
            return BootstrapPipelineResult(success=False, failure=exc)

        result = BootstrapPipelineResult(
            success=True,
            success_payload=BootstrapPipelineSuccess(
                artifact=artifact,
                verification=verification,
                entrypoint=entrypoint,
                sdk_validation=sdk_validation,
            ),
        )
        observed_at = self._utc_now()
        if self._persistence is not None:
            assert result.success_payload is not None
            self._persistence.on_bootstrap_success(
                launch_spec=launch_spec,
                success=result.success_payload,
                occurred_at=observed_at,
                observed_at=observed_at,
            )
        return result

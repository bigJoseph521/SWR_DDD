from __future__ import annotations

import importlib
import inspect
import sys
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Mapping

from runtime.infrastructure.strategy_loader.artifact_fetcher import ArtifactFetchResult
from runtime.bootstrap.failures import EntrypointLoadFailure
from runtime.bootstrap.launch_spec import LaunchSpec


@dataclass(frozen=True, slots=True)
class EntrypointLoadResult:
    entrypoint_spec: str
    module_name: str
    symbol_name: str
    symbol: Any
    details: Mapping[str, Any] = field(default_factory=dict)


class EntrypointLoader:
    def load(
        self,
        *,
        launch_spec: LaunchSpec,
        fetch_result: ArtifactFetchResult,
    ) -> EntrypointLoadResult:
        module_name, symbol_name = self._parse_entrypoint_spec(launch_spec.entrypoint)
        with self._materialized_import_path(fetch_result.materialized_root):
            try:
                module = importlib.import_module(module_name)
            except Exception as exc:
                raise EntrypointLoadFailure(
                    reason_code="entrypoint_import_failed",
                    message="Entrypoint module import failed.",
                    retryable=False,
                    details={
                        "entrypoint": launch_spec.entrypoint,
                        "module_name": module_name,
                        "exception_type": type(exc).__name__,
                        "exception_message": str(exc),
                    },
                ) from exc

            if not hasattr(module, symbol_name):
                raise EntrypointLoadFailure(
                    reason_code="entrypoint_symbol_not_found",
                    message="Entrypoint symbol was not found in module.",
                    retryable=False,
                    details={
                        "entrypoint": launch_spec.entrypoint,
                        "module_name": module_name,
                        "symbol_name": symbol_name,
                    },
                )

            symbol = getattr(module, symbol_name)
            if not self._is_valid_symbol(symbol):
                raise EntrypointLoadFailure(
                    reason_code="entrypoint_symbol_invalid",
                    message="Entrypoint symbol is not valid for bootstrap.",
                    retryable=False,
                    details={
                        "entrypoint": launch_spec.entrypoint,
                        "module_name": module_name,
                        "symbol_name": symbol_name,
                        "symbol_type": type(symbol).__name__,
                    },
                )

        return EntrypointLoadResult(
            entrypoint_spec=launch_spec.entrypoint,
            module_name=module_name,
            symbol_name=symbol_name,
            symbol=symbol,
            details={
                "artifact_reference": fetch_result.artifact_reference,
                "module_file": (
                    str(Path(getattr(module, "__file__", "")).resolve())
                    if getattr(module, "__file__", None)
                    else None
                ),
            },
        )

    def _parse_entrypoint_spec(self, entrypoint: str) -> tuple[str, str]:
        parts = entrypoint.split(":")
        if len(parts) != 2:
            self._malformed_entrypoint_failure(
                entrypoint=entrypoint,
                reason="expected_format_module_colon_symbol",
            )
        module_name, symbol_name = parts[0].strip(), parts[1].strip()
        if not module_name or not symbol_name:
            self._malformed_entrypoint_failure(
                entrypoint=entrypoint,
                reason="module_or_symbol_missing",
            )
        return module_name, symbol_name

    def _is_valid_symbol(self, symbol: Any) -> bool:
        return inspect.isclass(symbol) or callable(symbol)

    @contextmanager
    def _materialized_import_path(self, path: Path) -> Iterator[None]:
        resolved = str(path.resolve())
        sys.path.insert(0, resolved)
        try:
            yield
        finally:
            try:
                sys.path.remove(resolved)
            except ValueError:
                pass

    def _malformed_entrypoint_failure(self, *, entrypoint: str, reason: str) -> None:
        raise EntrypointLoadFailure(
            reason_code="entrypoint_invalid",
            message="Entrypoint specification is malformed.",
            retryable=False,
            details={"entrypoint": entrypoint, "reason": reason},
        )

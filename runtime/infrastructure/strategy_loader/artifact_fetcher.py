from __future__ import annotations

import hashlib
import shutil
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Protocol
from urllib.parse import unquote, urlparse
from urllib.request import url2pathname

from runtime.infrastructure.strategy_loader.digest_validation_env import (
    skip_artifact_digest_validation,
)
from runtime.domain.bootstrap_failures import ArtifactFetchFailure
from runtime.domain.launch_spec import LaunchSpec
from runtime.infrastructure.sdk.strategy_params_yaml import (
    discover_params_yaml_adjacent_to_module_file,
)

# Portable artifact refs from strategy-registry local backend (see strategy-registry-service).
REGISTRY_LOCAL_URI_PREFIX = "registry-local:///"


def resolve_registry_local_uri_to_path(artifact_uri: str, *, local_base: Path) -> Path:
    """
    Map ``registry-local:///<relative logical path>`` to an absolute filesystem path under
    ``local_base`` (same rules as ``ARTIFACT_LOCAL_BASE_PATH`` in strategy-registry-service).
    """
    raw = artifact_uri.strip()
    if not raw.startswith(REGISTRY_LOCAL_URI_PREFIX):
        raise ArtifactProviderFetchError(f"Not a registry-local URI: {artifact_uri!r}")
    rest = raw[len(REGISTRY_LOCAL_URI_PREFIX) :].lstrip("/")
    if not rest:
        raise ArtifactProviderFetchError(
            "registry-local artifact_uri must include a path after registry-local:///"
        )
    base = local_base.resolve()
    if not base.is_absolute():
        raise ArtifactProviderFetchError(
            "artifact local base path must be absolute for registry-local resolution"
        )
    for seg in rest.split("/"):
        if not seg or seg == "..":
            raise ArtifactProviderFetchError(
                "registry-local path must not contain empty or `..` segments"
            )
    return base.joinpath(*rest.split("/"))


class ArtifactProviderNotFoundError(Exception):
    pass


class ArtifactProviderAccessDeniedError(Exception):
    pass


class ArtifactProviderFetchError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class ArtifactMaterialization:
    source_reference: str
    materialized_root: Path
    provenance: Mapping[str, Any] = field(default_factory=dict)
    #: For a single-file ``artifact_uri``, digest verification hashes these paths relative
    #: to ``materialized_root``: the primary file, and ``{stem}.yaml`` (or legacy
    #: ``params.yaml``) when it existed next to the source file.
    #: Directory artifacts use ``None`` (full tree under ``materialized_root``).
    digest_relative_paths: frozenset[str] | None = None


class ArtifactProvider(Protocol):
    def fetch(
        self, *, artifact_reference: str, target_dir: Path
    ) -> ArtifactMaterialization: ...


class LocalArtifactProvider:
    """
    Deterministic local provider for `file://` and plain local paths.
    """

    def fetch(
        self, *, artifact_reference: str, target_dir: Path
    ) -> ArtifactMaterialization:
        source_path = self._resolve_source_path(artifact_reference)

        if not source_path.exists():
            raise ArtifactProviderNotFoundError(
                f"Artifact not found: {artifact_reference}"
            )

        target_dir.mkdir(parents=True, exist_ok=True)
        self._materialize(source=source_path, destination_root=target_dir)
        digest_paths: frozenset[str] | None = None
        if source_path.is_file():
            digest_names = [source_path.name]
            sibling_yaml = discover_params_yaml_adjacent_to_module_file(source_path)
            if sibling_yaml is not None:
                digest_names.append(sibling_yaml.name)
            digest_paths = frozenset(digest_names)
        return ArtifactMaterialization(
            source_reference=artifact_reference,
            materialized_root=target_dir,
            provenance={
                "provider": "local",
                "source_path": str(source_path),
            },
            digest_relative_paths=digest_paths,
        )

    def _resolve_source_path(self, artifact_reference: str) -> Path:
        parsed = urlparse(artifact_reference)
        if parsed.scheme == "file":
            if parsed.netloc:
                # UNC-style file URI.
                return Path(f"//{parsed.netloc}{unquote(parsed.path)}")
            raw_path = unquote(parsed.path)
            if raw_path.startswith("/") and len(raw_path) > 2 and raw_path[2] == ":":
                # file:///C:/... should resolve to C:/... on Windows.
                raw_path = raw_path[1:]
            return Path(raw_path)
        if parsed.scheme:
            raise ArtifactProviderFetchError(
                f"Unsupported provider scheme for LocalArtifactProvider: {parsed.scheme}"
            )
        return Path(artifact_reference)

    def _materialize(self, *, source: Path, destination_root: Path) -> None:
        if destination_root.exists():
            shutil.rmtree(destination_root)
        destination_root.mkdir(parents=True, exist_ok=True)

        if source.is_dir():
            entries = sorted(source.iterdir(), key=lambda p: p.name)
            for entry in entries:
                destination = destination_root / entry.name
                if entry.is_dir():
                    shutil.copytree(entry, destination)
                else:
                    shutil.copy2(entry, destination)
            return

        shutil.copy2(source, destination_root / source.name)
        # Single-file strategies keep defaults in ``{stem}.yaml`` next to the module (zip stem);
        # copy it so replay can merge ``properties`` / ``indicator_params`` into :class:`RuntimeParamsContext`.
        sibling_params = discover_params_yaml_adjacent_to_module_file(source)
        if sibling_params is not None:
            shutil.copy2(sibling_params, destination_root / sibling_params.name)


class RegistryLocalArtifactProvider:
    """Resolves ``registry-local:///…`` under a configured absolute base, then materializes locally."""

    def __init__(self, local_base: Path) -> None:
        self._local_base = local_base.resolve()
        self._delegate = LocalArtifactProvider()

    def fetch(
        self, *, artifact_reference: str, target_dir: Path
    ) -> ArtifactMaterialization:
        source_path = resolve_registry_local_uri_to_path(
            artifact_reference, local_base=self._local_base
        )
        mat = self._delegate.fetch(
            artifact_reference=source_path.resolve().as_uri(),
            target_dir=target_dir,
        )
        return ArtifactMaterialization(
            source_reference=artifact_reference,
            materialized_root=mat.materialized_root,
            provenance={
                **dict(mat.provenance),
                "provider": "registry-local",
                "registry_local_base": str(self._local_base),
                "resolved_path": str(source_path),
            },
            digest_relative_paths=mat.digest_relative_paths,
        )


@dataclass(frozen=True, slots=True)
class ArtifactFetchResult:
    artifact_reference: str
    materialized_root: Path
    expected_digest: str | None
    diagnostics: Mapping[str, Any] = field(default_factory=dict)
    digest_relative_paths: frozenset[str] | None = None
    #: When True, :class:`ArtifactVerifier` trusts digest checks done during fetch (e.g. raw ZIP SHA-256).
    preverified_digest: bool = False


class ArtifactFetcher:
    def __init__(
        self,
        *,
        work_root: Path,
        providers: Mapping[str, ArtifactProvider] | None = None,
        strategy_bundle_base: Path | None = None,
        artifact_local_base: Path | None = None,
    ) -> None:
        self._work_root = work_root
        self._strategy_bundle_base = strategy_bundle_base
        self._artifact_local_base = (
            artifact_local_base.resolve() if artifact_local_base is not None else None
        )
        self._providers: dict[str, ArtifactProvider] = dict(
            providers or {"file": LocalArtifactProvider(), "": LocalArtifactProvider()}
        )
        if (
            self._artifact_local_base is not None
            and "registry-local" not in self._providers
        ):
            self._providers["registry-local"] = RegistryLocalArtifactProvider(
                self._artifact_local_base
            )

    def _relative_artifact_base(self) -> Path:
        if self._strategy_bundle_base is not None:
            return self._strategy_bundle_base.resolve()
        return Path.cwd()

    def fetch(self, launch_spec: LaunchSpec) -> ArtifactFetchResult:
        artifact_uri = launch_spec.artifact_uri
        if self._strategy_bundle_base is not None:
            zip_result = self._maybe_fetch_strategy_bundle_zip(launch_spec)
            if zip_result is not None:
                return zip_result

        uri = artifact_uri.strip()
        if (
            uri.startswith(REGISTRY_LOCAL_URI_PREFIX)
            and self._artifact_local_base is None
        ):
            raise ArtifactFetchFailure(
                reason_code="artifact_fetch_failed",
                message=(
                    "registry-local artifact_uri requires SWR_ARTIFACT_LOCAL_BASE_PATH "
                    "or ARTIFACT_LOCAL_BASE_PATH (absolute directory matching strategy-registry)."
                ),
                retryable=False,
                details={
                    "artifact_uri": artifact_uri,
                    "scheme": "registry-local",
                },
            )

        zip_general = self._maybe_fetch_generic_verified_zip(launch_spec)
        if zip_general is not None:
            return zip_general

        provider = self._resolve_provider(artifact_uri)
        target_dir = self._materialization_dir(launch_spec=launch_spec)

        try:
            materialization = provider.fetch(
                artifact_reference=artifact_uri,
                target_dir=target_dir,
            )
        except ArtifactProviderNotFoundError as exc:
            raise ArtifactFetchFailure(
                reason_code="artifact_not_found",
                message="Artifact could not be located by approved provider.",
                retryable=False,
                details={
                    "artifact_uri": artifact_uri,
                    "launch_attempt": launch_spec.launch_attempt,
                    "provider_error": str(exc),
                },
            ) from exc
        except ArtifactProviderAccessDeniedError as exc:
            raise ArtifactFetchFailure(
                reason_code="artifact_access_denied",
                message="Artifact access was denied by provider policy.",
                retryable=False,
                details={
                    "artifact_uri": artifact_uri,
                    "provider_error": str(exc),
                },
            ) from exc
        except ArtifactProviderFetchError as exc:
            raise ArtifactFetchFailure(
                reason_code="artifact_fetch_failed",
                message="Artifact fetch failed before bootstrap execution.",
                retryable=True,
                details={
                    "artifact_uri": artifact_uri,
                    "provider_error": str(exc),
                },
            ) from exc
        except OSError as exc:
            raise ArtifactFetchFailure(
                reason_code="artifact_materialization_failed",
                message="Artifact fetch failed during local materialization.",
                retryable=True,
                details={
                    "artifact_uri": artifact_uri,
                    "os_error": str(exc),
                    "exception_type": type(exc).__name__,
                },
            ) from exc

        return ArtifactFetchResult(
            artifact_reference=materialization.source_reference,
            materialized_root=materialization.materialized_root,
            expected_digest=launch_spec.artifact_digest,
            diagnostics={
                "runtime_id": launch_spec.runtime_id,
                "launch_attempt": launch_spec.launch_attempt,
                "provenance": dict(materialization.provenance),
            },
            digest_relative_paths=materialization.digest_relative_paths,
            preverified_digest=False,
        )

    def _maybe_fetch_strategy_bundle_zip(
        self, launch_spec: LaunchSpec
    ) -> ArtifactFetchResult | None:
        """
        When ``artifact_uri`` resolves to a ``.zip`` under ``<strategy_bundle_base>/strategy_bundle/``,
        verify raw SHA-256 of the zip against ``artifact_digest``, extract into that folder, and
        use the bundle directory as the import root (entrypoint + SDK validation follow).
        """
        base = self._strategy_bundle_base
        if base is None:
            return None
        bundle_root = (base / "strategy_bundle").resolve()
        zip_path = self._resolve_local_artifact_path(launch_spec.artifact_uri, base)
        if (
            zip_path is None
            or zip_path.suffix.lower() != ".zip"
            or not zip_path.is_file()
        ):
            return None
        try:
            zip_resolved = zip_path.resolve()
            zip_resolved.relative_to(bundle_root)
        except ValueError:
            return None

        return self._verified_zip_extract(
            zip_resolved=zip_resolved,
            launch_spec=launch_spec,
            extract_root=bundle_root,
            provenance_provider="strategy_bundle_zip",
            digest_required_message="artifact_digest is required to verify strategy_bundle zip.",
            digest_unsupported_message="strategy_bundle zip verification requires sha256:… digest.",
            digest_mismatch_message="strategy_bundle zip SHA-256 does not match artifact_digest.",
        )

    def _maybe_fetch_generic_verified_zip(
        self, launch_spec: LaunchSpec
    ) -> ArtifactFetchResult | None:
        """
        Verified ``.zip`` artifacts addressed by ``registry-local:///…``, ``file://…``, or paths
        relative to :meth:`_relative_artifact_base` (extracted under the per-launch materialization
        directory so imports resolve against extracted package roots).
        """
        try:
            zip_path = self._resolve_artifact_zip_path(launch_spec)
        except ArtifactProviderFetchError as exc:
            raise ArtifactFetchFailure(
                reason_code="artifact_fetch_failed",
                message="Could not resolve artifact_uri to a local zip path.",
                retryable=False,
                details={
                    "artifact_uri": launch_spec.artifact_uri,
                    "provider_error": str(exc),
                },
            ) from exc
        if (
            zip_path is None
            or zip_path.suffix.lower() != ".zip"
            or not zip_path.is_file()
        ):
            return None
        zip_resolved = zip_path.resolve()
        dest = self._materialization_dir(launch_spec=launch_spec)
        return self._verified_zip_extract(
            zip_resolved=zip_resolved,
            launch_spec=launch_spec,
            extract_root=dest,
            provenance_provider="artifact_zip",
            digest_required_message="artifact_digest is required to verify artifact zip.",
            digest_unsupported_message="artifact zip verification requires sha256:… digest.",
            digest_mismatch_message="artifact zip SHA-256 does not match artifact_digest.",
        )

    def _resolve_artifact_zip_path(self, launch_spec: LaunchSpec) -> Path | None:
        uri = launch_spec.artifact_uri.strip()
        if not uri:
            return None
        if uri.startswith(REGISTRY_LOCAL_URI_PREFIX):
            if self._artifact_local_base is None:
                return None
            return resolve_registry_local_uri_to_path(
                uri, local_base=self._artifact_local_base
            )
        return self._resolve_local_artifact_path(uri, self._relative_artifact_base())

    def _verified_zip_extract(
        self,
        *,
        zip_resolved: Path,
        launch_spec: LaunchSpec,
        extract_root: Path,
        provenance_provider: str,
        digest_required_message: str,
        digest_unsupported_message: str,
        digest_mismatch_message: str,
    ) -> ArtifactFetchResult:
        skip_digest = skip_artifact_digest_validation()
        expected = launch_spec.artifact_digest
        if not expected or not expected.strip():
            if not skip_digest:
                raise ArtifactFetchFailure(
                    reason_code="expected_digest_missing",
                    message=digest_required_message,
                    retryable=False,
                    details={
                        "artifact_uri": launch_spec.artifact_uri,
                        "zip_path": str(zip_resolved),
                    },
                )
            self._extract_zip_to_directory(zip_resolved, extract_root)
            return ArtifactFetchResult(
                artifact_reference=launch_spec.artifact_uri,
                materialized_root=extract_root,
                expected_digest=None,
                diagnostics={
                    "runtime_id": launch_spec.runtime_id,
                    "launch_attempt": launch_spec.launch_attempt,
                    "provenance": {
                        "provider": provenance_provider,
                        "zip_path": str(zip_resolved),
                        "extracted_to": str(extract_root),
                        "digest_validation_skipped": True,
                    },
                },
                digest_relative_paths=None,
                preverified_digest=True,
            )

        if skip_digest:
            self._extract_zip_to_directory(zip_resolved, extract_root)
            return ArtifactFetchResult(
                artifact_reference=launch_spec.artifact_uri,
                materialized_root=extract_root,
                expected_digest=expected.strip(),
                diagnostics={
                    "runtime_id": launch_spec.runtime_id,
                    "launch_attempt": launch_spec.launch_attempt,
                    "provenance": {
                        "provider": provenance_provider,
                        "zip_path": str(zip_resolved),
                        "extracted_to": str(extract_root),
                        "digest_validation_skipped": True,
                    },
                },
                digest_relative_paths=None,
                preverified_digest=True,
            )

        algo, expected_hex = self._parse_sha256_digest(expected)
        if algo != "sha256":
            raise ArtifactFetchFailure(
                reason_code="digest_unsupported",
                message=digest_unsupported_message,
                retryable=False,
                details={"artifact_digest": expected, "algorithm": algo},
            )

        actual_hex = hashlib.sha256(zip_resolved.read_bytes()).hexdigest()
        if actual_hex != expected_hex:
            raise ArtifactFetchFailure(
                reason_code="digest_mismatch",
                message=digest_mismatch_message,
                retryable=False,
                details={
                    "artifact_uri": launch_spec.artifact_uri,
                    "zip_path": str(zip_resolved),
                    "expected_digest": expected,
                    "actual_digest": f"sha256:{actual_hex}",
                },
            )

        self._extract_zip_to_directory(zip_resolved, extract_root)

        return ArtifactFetchResult(
            artifact_reference=launch_spec.artifact_uri,
            materialized_root=extract_root,
            expected_digest=expected,
            diagnostics={
                "runtime_id": launch_spec.runtime_id,
                "launch_attempt": launch_spec.launch_attempt,
                "provenance": {
                    "provider": provenance_provider,
                    "zip_path": str(zip_resolved),
                    "extracted_to": str(extract_root),
                },
            },
            digest_relative_paths=None,
            preverified_digest=True,
        )

    @staticmethod
    def _parse_sha256_digest(digest: str) -> tuple[str, str]:
        if digest.count(":") != 1:
            raise ArtifactFetchFailure(
                reason_code="digest_unsupported",
                message="Digest must be algorithm:hex.",
                retryable=False,
                details={"artifact_digest": digest},
            )
        algorithm, encoded = digest.split(":", 1)
        algo = algorithm.strip().lower()
        hex_part = encoded.strip().lower()
        if (
            algo != "sha256"
            or not hex_part
            or any(c not in "0123456789abcdef" for c in hex_part)
        ):
            raise ArtifactFetchFailure(
                reason_code="digest_unsupported",
                message="Verified zip artifacts require a lowercase sha256:hex digest.",
                retryable=False,
                details={"artifact_digest": digest},
            )
        return algo, hex_part

    @staticmethod
    def _resolve_local_artifact_path(artifact_uri: str, base_dir: Path) -> Path | None:
        raw = artifact_uri.strip()
        if not raw:
            return None
        if raw.startswith("file://"):
            parsed = urlparse(raw)
            local = Path(url2pathname(parsed.path))
            if not local.is_absolute():
                local = (base_dir / local).resolve()
            return local
        p = Path(raw)
        if p.is_absolute():
            return p.resolve()
        candidate = (base_dir / raw).resolve()
        return candidate

    @staticmethod
    def _extract_zip_to_directory(zip_path: Path, dest_dir: Path) -> None:
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_resolved = dest_dir.resolve()
        with zipfile.ZipFile(zip_path, "r") as zf:
            for info in zf.infolist():
                name = info.filename
                if not name or name.startswith("/"):
                    raise ArtifactFetchFailure(
                        reason_code="artifact_unsafe_zip",
                        message="Zip entry has an unsafe path.",
                        retryable=False,
                        details={"zip_path": str(zip_path), "entry": name},
                    )
                rel = Path(name)
                if any(part == ".." for part in rel.parts):
                    raise ArtifactFetchFailure(
                        reason_code="artifact_unsafe_zip",
                        message="Zip entry escapes target directory.",
                        retryable=False,
                        details={"zip_path": str(zip_path), "entry": name},
                    )
                target = (dest_resolved / rel).resolve()
                try:
                    target.relative_to(dest_resolved)
                except ValueError as exc:
                    raise ArtifactFetchFailure(
                        reason_code="artifact_unsafe_zip",
                        message="Zip entry resolves outside strategy_bundle.",
                        retryable=False,
                        details={"zip_path": str(zip_path), "entry": name},
                    ) from exc
                if info.is_dir():
                    target.mkdir(parents=True, exist_ok=True)
                else:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with zf.open(info) as src, target.open("wb") as out:
                        shutil.copyfileobj(src, out)

    def _resolve_provider(self, artifact_reference: str) -> ArtifactProvider:
        parsed = urlparse(artifact_reference)
        scheme = parsed.scheme
        provider = self._providers.get(scheme)
        if provider is not None:
            return provider
        # plain path without URI scheme uses default provider
        if ":" not in artifact_reference and self._providers.get("") is not None:
            return self._providers[""]
        raise ArtifactFetchFailure(
            reason_code="artifact_provider_unsupported",
            message="No approved provider is registered for artifact reference.",
            retryable=False,
            details={"artifact_reference": artifact_reference, "scheme": scheme},
        )

    def _materialization_dir(self, *, launch_spec: LaunchSpec) -> Path:
        deterministic_id = f"{launch_spec.runtime_id}-{launch_spec.launch_attempt}"
        return self._work_root / deterministic_id / "artifact"

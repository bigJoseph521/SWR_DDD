"""Static import-boundary checks for strategy-worker-runtime DDD layers."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_ROOT = REPO_ROOT / "runtime"

RUNTIME_LAYERS = ("domain", "application", "interface", "infrastructure", "bootstrap")

# file path (posix) -> imported module -> reason
ALLOWED_IMPORT_VIOLATIONS: dict[str, dict[str, str]] = {}


@dataclass(frozen=True)
class ImportRecord:
    file_path: str
    line_no: int
    imported_module: str
    source_layer: str
    forbidden_prefix: str
    rule_id: str
    suggestion: str


_LAYER_RULES: dict[str, tuple[str, tuple[str, ...], str]] = {
    "domain": (
        "domain_has_no_outer_layer_imports",
        (
            "runtime.application",
            "runtime.infrastructure",
            "runtime.interface",
            "runtime.bootstrap",
        ),
        "Keep runtime/domain pure: models, specs, events, and policies only. "
        "Move orchestration to runtime/application and adapters to runtime/infrastructure or runtime/interface.",
    ),
    "application": (
        "application_has_no_infrastructure_interface_or_bootstrap_imports",
        (
            "runtime.infrastructure",
            "runtime.interface",
            "runtime.bootstrap",
        ),
        "runtime/application must depend on domain and ports only. "
        "Wire concrete adapters in runtime/bootstrap (or narrow transitional allowlist).",
    ),
    "interface": (
        "interface_has_no_infrastructure_imports",
        ("runtime.infrastructure",),
        "runtime/interface inbound adapters may use runtime/application and runtime/domain. "
        "Construct concrete outbound adapters in runtime/bootstrap unless allowlisted.",
    ),
    "infrastructure": (
        "infrastructure_has_no_bootstrap_imports",
        ("runtime.bootstrap",),
        "runtime/infrastructure outbound adapters may use runtime/application ports and runtime/domain types. "
        "Pass bootstrap-built specs via constructors instead of importing runtime/bootstrap.",
    ),
}


def _is_allowed(file_path: str, imported_module: str) -> bool:
    allowed_for_file = ALLOWED_IMPORT_VIOLATIONS.get(file_path, {})
    for allowed_module, _reason in allowed_for_file.items():
        if imported_module == allowed_module or imported_module.startswith(allowed_module + "."):
            return True
    return False


def _matches_forbidden(imported_module: str, forbidden_prefixes: Iterable[str]) -> str | None:
    for prefix in forbidden_prefixes:
        if imported_module == prefix or imported_module.startswith(prefix + "."):
            return prefix
    return None


def _layer_from_path(file_path: Path) -> str | None:
    try:
        rel = file_path.relative_to(RUNTIME_ROOT)
    except ValueError:
        return None
    parts = rel.parts
    if not parts:
        return None
    layer = parts[0]
    return layer if layer in RUNTIME_LAYERS else None


def _iter_runtime_python_files() -> Iterable[Path]:
    for layer in RUNTIME_LAYERS:
        layer_root = RUNTIME_ROOT / layer
        if not layer_root.is_dir():
            continue
        for path in sorted(layer_root.rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            yield path


def _extract_runtime_imports(path: Path) -> list[tuple[str, int]]:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    found: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("runtime."):
                    found.append((alias.name, node.lineno))
        elif isinstance(node, ast.ImportFrom):
            if node.level != 0 or not node.module:
                continue
            if node.module.startswith("runtime."):
                found.append((node.module, node.lineno))
    return found


def collect_runtime_imports(
    root: Path = RUNTIME_ROOT,
    *,
    allowlist_disabled: bool = False,
) -> list[ImportRecord]:
    """Collect import-boundary violations under runtime layer directories."""
    _ = root  # reserved for callers/tools; scans all layer dirs under RUNTIME_ROOT
    records: list[ImportRecord] = []
    for path in _iter_runtime_python_files():
        layer = _layer_from_path(path)
        if layer is None or layer not in _LAYER_RULES:
            continue
        rule_id, forbidden_prefixes, suggestion = _LAYER_RULES[layer]
        file_path = path.relative_to(REPO_ROOT).as_posix()
        for imported_module, line_no in _extract_runtime_imports(path):
            forbidden_prefix = _matches_forbidden(imported_module, forbidden_prefixes)
            if forbidden_prefix is None:
                continue
            if not allowlist_disabled and _is_allowed(file_path, imported_module):
                continue
            records.append(
                ImportRecord(
                    file_path=file_path,
                    line_no=line_no,
                    imported_module=imported_module,
                    source_layer=layer,
                    forbidden_prefix=forbidden_prefix,
                    rule_id=rule_id,
                    suggestion=suggestion,
                )
            )
    return records


def _format_violations(records: list[ImportRecord]) -> str:
    lines = ["DDD import boundary violations:", ""]
    for record in sorted(records, key=lambda r: (r.file_path, r.line_no, r.imported_module)):
        lines.append(f"  {record.file_path}:{record.line_no}")
        lines.append(f"    import: {record.imported_module}")
        lines.append(f"    rule: {record.rule_id} (forbidden: {record.forbidden_prefix})")
        lines.append(f"    hint: {record.suggestion}")
        lines.append("")
    lines.append(
        "If a violation is an intentional transitional exception, add a narrow entry to "
        "ALLOWED_IMPORT_VIOLATIONS in tests/unit/test_ddd_import_boundaries.py with a removal reason."
    )
    return "\n".join(lines)


def _violations_for_layer(layer: str) -> list[ImportRecord]:
    return [record for record in collect_runtime_imports() if record.source_layer == layer]


def test_domain_has_no_outer_layer_imports() -> None:
    violations = _violations_for_layer("domain")
    assert not violations, _format_violations(violations)


def test_application_has_no_infrastructure_interface_or_bootstrap_imports() -> None:
    violations = _violations_for_layer("application")
    assert not violations, _format_violations(violations)


def test_infrastructure_does_not_import_bootstrap_except_allowlist() -> None:
    violations = _violations_for_layer("infrastructure")
    assert not violations, _format_violations(violations)


def test_interface_does_not_import_infrastructure_except_allowlist() -> None:
    violations = _violations_for_layer("interface")
    assert not violations, _format_violations(violations)


def test_bootstrap_is_allowed_to_wire_layers() -> None:
    """Bootstrap is the composition root and may import from every other layer."""
    imports_by_prefix: dict[str, set[str]] = {
        "runtime.domain": set(),
        "runtime.application": set(),
        "runtime.infrastructure": set(),
    }
    bootstrap_root = RUNTIME_ROOT / "bootstrap"
    for path in sorted(bootstrap_root.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        for imported_module, _line_no in _extract_runtime_imports(path):
            for prefix in imports_by_prefix:
                if imported_module == prefix or imported_module.startswith(prefix + "."):
                    imports_by_prefix[prefix].add(imported_module)

    missing = [prefix for prefix, modules in imports_by_prefix.items() if not modules]
    assert not missing, (
        "bootstrap composition root should wire domain, application, and infrastructure; "
        "missing imports from: " + ", ".join(missing)
    )

    # bootstrap is not subject to forbidden-prefix rules
    assert not _violations_for_layer("bootstrap")


def test_allowlist_entries_reference_real_violations() -> None:
    """Stale allowlist keys fail fast so exceptions do not linger after refactors."""
    current = {
        (record.file_path, record.imported_module)
        for record in collect_runtime_imports(allowlist_disabled=True)
    }
    stale: list[str] = []
    for file_path, modules in ALLOWED_IMPORT_VIOLATIONS.items():
        for imported_module in modules:
            if (file_path, imported_module) not in current:
                stale.append(f"{file_path} -> {imported_module}")
    assert not stale, "Stale allowlist entries (no longer violating):\n  " + "\n  ".join(stale)

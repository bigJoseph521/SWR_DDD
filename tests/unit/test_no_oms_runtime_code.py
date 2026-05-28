"""Ensure strategy-worker-runtime production code has no OMS egress surface."""

from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_ROOT = REPO_ROOT / "runtime"

FORBIDDEN_OMS_TOKENS: tuple[str, ...] = (
    "oms_client",
    "OmsClient",
    "OMSClient",
    "order_management",
    "OrderManagement",
    "OMS_ORDER",
    "OMS_URL",
    "OMS_GRPC",
    "OMS_HTTP",
    "OMS_EXECUTION",
    "submit_to_oms",
    "oms_gateway",
    "OmsGateway",
    "build_oms",
    "FakeOms",
    "no_oms_client",
    "OMS_BYPASS",
    "SWR_OMS",
)


def _iter_runtime_python_files() -> list[Path]:
    paths: list[Path] = []
    for path in sorted(RUNTIME_ROOT.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        paths.append(path)
    return paths


def _code_lines(path: Path) -> list[tuple[int, str]]:
    """Return source lines excluding comments and docstrings."""
    source = path.read_bytes()
    tree = ast.parse(source, filename=str(path))
    docstring_lines: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(
            node,
            (
                ast.Module,
                ast.FunctionDef,
                ast.AsyncFunctionDef,
                ast.ClassDef,
            ),
        ):
            body = node.body
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                end = body[0].end_lineno or body[0].lineno
                for line_no in range(body[0].lineno, end + 1):
                    docstring_lines.add(line_no)

    lines: list[tuple[int, str]] = []
    for line_no, line in enumerate(source.decode("utf-8").splitlines(), start=1):
        if line_no in docstring_lines:
            continue
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        lines.append((line_no, line))
    return lines


def _find_forbidden_tokens(path: Path) -> list[tuple[int, str, str]]:
    hits: list[tuple[int, str, str]] = []
    for line_no, line in _code_lines(path):
        for token in FORBIDDEN_OMS_TOKENS:
            if token in line:
                hits.append((line_no, token, line.strip()))
    return hits


def test_runtime_production_code_has_no_oms_tokens() -> None:
    violations: list[str] = []
    for path in _iter_runtime_python_files():
        rel = path.relative_to(REPO_ROOT).as_posix()
        for line_no, token, snippet in _find_forbidden_tokens(path):
            violations.append(f"{rel}:{line_no}: forbidden '{token}' in: {snippet}")
    assert not violations, "OMS-related tokens found in runtime:\n" + "\n".join(
        violations
    )


def test_runtime_has_no_oms_worker_proto_imports() -> None:
    violations: list[str] = []
    for path in _iter_runtime_python_files():
        rel = path.relative_to(REPO_ROOT).as_posix()
        for line_no, line in _code_lines(path):
            if "oms_worker" in line:
                violations.append(f"{rel}:{line_no}: {line.strip()}")
    assert not violations, "oms_worker imports in runtime:\n" + "\n".join(violations)

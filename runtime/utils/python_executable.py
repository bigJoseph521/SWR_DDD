"""Resolve the Python interpreter for subprocesses and error messages.

Local development may use a project ``.venv``; Docker/Kubernetes images use
``/opt/venv`` on ``PATH`` with ``sys.executable`` already pointing there. This
module avoids requiring a host-style ``.venv`` layout at runtime.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Iterable


def _venv_python_candidates(roots: Iterable[Path]) -> Iterable[Path]:
    for root in roots:
        yield root / ".venv" / "bin" / "python"
        yield root / ".venv" / "Scripts" / "python.exe"
        yield root / ".venv" / "Scripts" / "python"


def _fixed_venv_candidates() -> tuple[Path, ...]:
    return (
        Path("/app/.venv/bin/python"),
        Path("/app/.venv/Scripts/python.exe"),
        Path("/opt/venv/bin/python"),
    )


def resolve_python_executable() -> str:
    """Return a Python executable path for child processes and diagnostics.

    Resolution order:

    1. Non-empty ``PYTHON_EXECUTABLE`` environment variable.
    2. ``.venv`` interpreter under :func:`os.getcwd` only (Unix ``bin/python`` or
       Windows ``Scripts`` layout). Parent directories are not scanned, so an
       unrelated ``.venv`` higher in the filesystem is never selected.
    3. ``/app/.venv`` then ``/opt/venv`` (common container layouts).
    4. ``sys.executable`` (current interpreter, e.g. image default when
       ``PATH`` includes ``/opt/venv/bin``).
    """
    explicit = (os.environ.get("PYTHON_EXECUTABLE") or "").strip()
    if explicit:
        return explicit

    cwd = Path.cwd().resolve()
    for candidate in _venv_python_candidates((cwd,)):
        try:
            if candidate.exists():
                return str(candidate.resolve())
        except OSError:
            continue

    for candidate in _fixed_venv_candidates():
        try:
            if candidate.exists():
                return str(candidate.resolve())
        except OSError:
            continue

    return sys.executable

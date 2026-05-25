"""Run mypy on a Python file and save the full report to repo mypy_result.txt."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MYPY_RESULT_PATH = PROJECT_ROOT / "mypy_result.txt"


def mypy_result_path_for(target: Path) -> Path:
    """Path where the mypy report is written when type-checking ``target``."""
    _ = target
    return MYPY_RESULT_PATH


# SDK wheels often omit py.typed; strategy code is still checked without failing on those imports.
# Follow imports for type information only: do not fail bootstrap on mypy issues inside alphovex_sdk.
_MYPY_BASE_ARGS: tuple[str, ...] = (
    "--disable-error-code=import-untyped",
    "--follow-imports=silent",
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Type-check a Python file with mypy; results are written to repo mypy_result.txt.",
    )
    parser.add_argument(
        "file",
        type=Path,
        help="Path to the Python file to validate (relative or absolute).",
    )
    args = parser.parse_args()
    target = args.file.expanduser().resolve()
    result_path = mypy_result_path_for(target)

    if not target.is_file():
        err = f"Error: not a file: {target}\n"
        result_path.write_text(err, encoding="utf-8")
        print(err, end="", file=sys.stderr)
        return 1

    proc = subprocess.run(
        [sys.executable, "-m", "mypy", *_MYPY_BASE_ARGS, str(target)],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
    )
    out = (proc.stdout or "") + (proc.stderr or "")
    result_path.write_text(out, encoding="utf-8")
    print(out, end="")
    return proc.returncode


if __name__ == "__main__":
    raise SystemExit(main())

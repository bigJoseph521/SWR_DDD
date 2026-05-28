"""Package entry: backtest-runner subprocess or platform worker CLI."""

from __future__ import annotations

import argparse
import json
import sys


def _parse_subprocess_cli(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "strategy-worker-runtime: backtest-runner subprocess (stdio JSONL) "
            "or platform worker (PAPER/LIVE/BACKTEST)."
        ),
    )
    parser.add_argument(
        "--transport",
        default="stdio-jsonl",
        choices=("stdio-jsonl",),
        help="BACKTEST subprocess transport (backtest-runner child only).",
    )
    parser.add_argument(
        "--mode",
        default=None,
        help="BACKTEST for subprocess; PAPER/LIVE for platform worker.",
    )
    parser.add_argument(
        "--artifact-path",
        default=None,
        help="Strategy bundle path (backtest-runner subprocess bootstrap).",
    )
    parser.add_argument(
        "--entrypoint",
        default=None,
        help="Strategy entrypoint module:ClassName (subprocess bootstrap).",
    )
    return parser.parse_args(argv)


def _stderr_error(
    code: str,
    message: str,
    *,
    details: dict[str, object] | None = None,
) -> None:
    body: dict[str, object] = {"error": code, "message": message}
    if details:
        body["details"] = details
    print(
        json.dumps(body, separators=(",", ":"), ensure_ascii=True),
        file=sys.stderr,
        flush=True,
    )


def _run_backtest_runner_subprocess(argv: list[str] | None = None) -> int:
    from runtime.bootstrap.backtest_runner_subprocess import (
        StrategyBootstrapError,
        build_runner_session,
        load_strategy_from_cli,
    )
    from runtime.interface.stdio.backtest_runner.host import BacktestRunnerStdioHost
    from runtime.interface.stdio.backtest_runner.protocol_stdio import (
        install_protocol_stdio,
    )

    args = _parse_subprocess_cli(argv)
    transport = str(args.transport).strip().lower()
    if transport != "stdio-jsonl":
        _stderr_error(
            "UNSUPPORTED_TRANSPORT",
            f"only stdio-jsonl is supported; got {args.transport!r}",
        )
        return 2

    artifact_path = args.artifact_path
    entrypoint = args.entrypoint
    if artifact_path is None and entrypoint is None:
        _stderr_error(
            "CLI_BOOTSTRAP_INCOMPLETE",
            "backtest-runner subprocess requires --artifact-path and --entrypoint",
        )
        return 2
    if not artifact_path or not entrypoint:
        _stderr_error(
            "CLI_BOOTSTRAP_INCOMPLETE",
            "both --artifact-path and --entrypoint are required for subprocess bootstrap",
        )
        return 2

    install_protocol_stdio()

    try:
        loaded = load_strategy_from_cli(
            artifact_path=str(artifact_path),
            entrypoint=str(entrypoint),
            mode=str(args.mode or "BACKTEST"),
        )
    except StrategyBootstrapError as exc:
        _stderr_error(exc.code, exc.message, details=exc.details or None)
        return 2

    session = build_runner_session(loaded)
    print(
        "strategy CLI bootstrap complete; entering backtest-runner stdio loop",
        file=sys.stderr,
        flush=True,
    )
    return BacktestRunnerStdioHost(session=session).run()


def main(argv: list[str] | None = None) -> int:
    args = _parse_subprocess_cli(argv)
    if args.artifact_path is not None or args.entrypoint is not None:
        return _run_backtest_runner_subprocess(argv)

    from runtime.interface.cli.main import main as platform_main

    platform_main()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Thin CLI entry: protobuf well-known types, then bootstrap composition."""

from __future__ import annotations

# Load protobuf well-known types before any generated *_pb2 (descriptor pool).
import google.protobuf.empty_pb2  # noqa: F401
import google.protobuf.struct_pb2  # noqa: F401
import google.protobuf.timestamp_pb2  # noqa: F401

from runtime.bootstrap.runtime_entrypoint import run_runtime_from_cli


def main() -> None:
    run_runtime_from_cli()


if __name__ == "__main__":
    main()

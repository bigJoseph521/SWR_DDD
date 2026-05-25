"""
Symbol → Redis realtime partition.

Must stay aligned with **both** Rust services:

- ``market-data-service`` — ``src/application/realtime/realtime_partition.rs``
- ``paper-execution-service`` — ``src/market_data/realtime_partition.rs``

Algorithm: **IEEE CRC32** over UTF-8 bytes of ``symbol.strip()`` (same as
``crc32fast::hash`` default in those crates), then ``% partition_count``.
"""

from __future__ import annotations

import zlib


def symbol_crc32(data: bytes) -> int:
    """IEEE CRC32 over ``data`` (matches Rust ``crc32fast::hash`` / zlib)."""
    return zlib.crc32(data) & 0xFFFFFFFF


def crc32c(data: bytes) -> int:
    """Deprecated alias for :func:`symbol_crc32` (name is historical; not CRC32-C)."""
    return symbol_crc32(data)


def market_data_partition(symbol: str, partition_count: int) -> int:
    """
    Partition index in ``0 .. partition_count - 1`` for Redis keys
    ``md:stream:*:{p}``, ``md:latest:*:{p}``, ``md:realtime:*:{p}``.

    Same formula as ``market_data_partition`` in market-data-service and
    paper-execution-service (IEEE CRC32 via ``crc32fast::hash`` + trim + modulo).
    """
    if partition_count < 1:
        raise ValueError("partition_count must be >= 1")
    h = symbol_crc32(symbol.strip().encode("utf-8"))
    return int(h % partition_count)

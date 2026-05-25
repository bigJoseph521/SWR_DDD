from __future__ import annotations

import pytest

from runtime.integration.market_data_partition import (
    market_data_partition,
    symbol_crc32,
)

# IEEE CRC32 + % 128 must match Rust `crc32fast::hash(symbol.trim().as_bytes()) % 128`
# in market-data-service and paper-execution-service `realtime_partition`.
_RUST_ALIGNED_128: tuple[tuple[str, int, int], ...] = (
    ("AAPL", 92, 0xB665575C),
    ("MSFT", 123, 0xECE5B27B),
    ("BRK.A", 22, 0xF3062116),
    ("SPY", 127, 0x39B7FDFF),
    ("", 0, 0x00000000),
    ("WOLF", 86, 0xCF2BC656),
    ("GOOG", 28, 0xC318F29C),
    ("BTC", 70, 0xBD5D08C6),
    ("BRK.B", 44, 0x6A0F70AC),
    ("QQQ", 98, 0x2DF390E2),
    ("TSM", 68, 0x0D0F6CC4),
    ("NVDL", 42, 0xD9C125AA),
)


def test_symbol_crc32_ieee_vector() -> None:
    """IEEE CRC32 test vector (same polynomial as ``crc32fast`` / zlib)."""
    assert symbol_crc32(b"123456789") == 0xCBF43926


@pytest.mark.parametrize("sym,partition_128,crc32", _RUST_ALIGNED_128)
def test_partition_128_and_symbol_crc32_match_rust_realtime_partition(
    sym: str, partition_128: int, crc32: int
) -> None:
    assert symbol_crc32(sym.encode("utf-8")) == crc32
    assert market_data_partition(sym, 128) == partition_128
    if sym:
        assert market_data_partition(f"  {sym}  ", 128) == partition_128


def test_tsm_partition_matches_market_data_service_trades_stream() -> None:
    """Regression: TSM must land on partition 68 (not 64 from the old CRC32-C path)."""
    assert market_data_partition("TSM", 128) == 68


@pytest.mark.parametrize("count", [1, 2, 7, 128, 4096, 65_535])
@pytest.mark.parametrize("sym", ["AAPL", "MSFT", "BRK.A", "SPY", ""])
def test_partition_always_lt_count(sym: str, count: int) -> None:
    """Mirrors ``realtime_partition`` tests in market-data-service (same symbol set)."""
    p = market_data_partition(sym, count)
    assert 0 <= p < count


def test_partition_count_zero_raises() -> None:
    """Rust returns ``None`` for count 0; Python API uses ``ValueError``."""
    with pytest.raises(ValueError, match="partition_count"):
        market_data_partition("AAPL", 0)

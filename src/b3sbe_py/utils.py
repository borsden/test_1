"""Helper utilities for decoding little-endian numeric fields."""
from __future__ import annotations

import struct
from typing import Optional

NULL_INT64 = -(1 << 63)
NULL_UINT64 = (1 << 64) - 1
NULL_UINT32 = (1 << 32) - 1


def read_u8(buffer: memoryview, offset: int) -> int:
    return buffer[offset]


def read_i8(buffer: memoryview, offset: int) -> int:
    value = buffer[offset]
    return value - 256 if value >= 128 else value


def read_u16(buffer: memoryview, offset: int) -> int:
    return struct.unpack_from("<H", buffer, offset)[0]


def read_i16(buffer: memoryview, offset: int) -> int:
    return struct.unpack_from("<h", buffer, offset)[0]


def read_u32(buffer: memoryview, offset: int) -> int:
    return struct.unpack_from("<I", buffer, offset)[0]


def read_i32(buffer: memoryview, offset: int) -> int:
    return struct.unpack_from("<i", buffer, offset)[0]


def read_u64(buffer: memoryview, offset: int) -> int:
    return struct.unpack_from("<Q", buffer, offset)[0]


def read_i64(buffer: memoryview, offset: int) -> int:
    return struct.unpack_from("<q", buffer, offset)[0]


def read_price(buffer: memoryview, offset: int, exponent: int = -4) -> tuple[int, Optional[float]]:
    mantissa = read_i64(buffer, offset)
    if mantissa == NULL_INT64:
        return mantissa, None
    return mantissa, mantissa * (10 ** exponent)


def read_price8(buffer: memoryview, offset: int, exponent: int = -8) -> tuple[int, Optional[float]]:
    mantissa = read_i64(buffer, offset)
    if mantissa == NULL_INT64:
        return mantissa, None
    return mantissa, mantissa * (10 ** exponent)


def read_char_array(buffer: memoryview, offset: int, length: int) -> str:
    raw = bytes(buffer[offset : offset + length])
    return raw.split(b"\x00", 1)[0].decode("ascii", errors="ignore")


def read_var_string(buffer: memoryview, offset: int, max_length: int = 0) -> tuple[str, int]:
    length = buffer[offset]
    start = offset + 1
    end = start + length
    if max_length and length > max_length:
        end = start + max_length
    value = bytes(buffer[start:end]).decode("latin1", errors="ignore")
    return value, end

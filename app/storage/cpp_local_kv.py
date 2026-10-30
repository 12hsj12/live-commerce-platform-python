from __future__ import annotations

import struct
import zlib
from pathlib import Path


SNAPSHOT_MAGIC = 0x4C565332  # LVS2, native little-endian on supported C++ hosts.
WAL_MAGIC = 0x4C564B32  # LVK2.
OP_SET = 1
OP_DELETE = 2
OP_BATCH = 3
MAX_KEY_SIZE = 16 * 1024 * 1024
MAX_VALUE_SIZE = 64 * 1024 * 1024


class CorruptCppStorage(ValueError):
    """Raised for a complete record whose C++ checksum or encoding is invalid."""


def _u32(data: bytes, offset: int) -> tuple[int, int]:
    if offset + 4 > len(data):
        raise CorruptCppStorage("truncated 32-bit field")
    return struct.unpack_from("<I", data, offset)[0], offset + 4


def _sized(data: bytes, offset: int) -> tuple[bytes, int]:
    size, offset = _u32(data, offset)
    if size > MAX_VALUE_SIZE or offset + size > len(data):
        raise CorruptCppStorage("invalid length-prefixed field")
    return data[offset:offset + size], offset + size


def decode_snapshot(data: bytes) -> dict[bytes, bytes]:
    if len(data) < 4 or struct.unpack_from("<I", data, 0)[0] != SNAPSHOT_MAGIC:
        raise CorruptCppStorage("corrupt C++ snapshot header")
    body = data[4:]
    if len(body) < 12:
        raise CorruptCppStorage("truncated C++ snapshot")
    payload, checksum_bytes = body[:-4], body[-4:]
    expected = struct.unpack("<I", checksum_bytes)[0]
    if zlib.crc32(payload) & 0xFFFFFFFF != expected:
        raise CorruptCppStorage("corrupt C++ snapshot checksum")
    count = struct.unpack_from("<Q", payload, 0)[0]
    if count > 10_000_000:
        raise CorruptCppStorage("invalid C++ snapshot record count")
    offset = 8
    records: dict[bytes, bytes] = {}
    for _ in range(count):
        key_size, offset = _u32(payload, offset)
        value_size, offset = _u32(payload, offset)
        if key_size > MAX_KEY_SIZE or value_size > MAX_VALUE_SIZE or offset + key_size + value_size > len(payload):
            raise CorruptCppStorage("invalid C++ snapshot record lengths")
        key = payload[offset:offset + key_size]
        offset += key_size
        value = payload[offset:offset + value_size]
        offset += value_size
        if not key or len(key) > MAX_KEY_SIZE:
            raise CorruptCppStorage("invalid C++ snapshot key")
        records[key] = value
    if offset != len(payload):
        raise CorruptCppStorage("trailing C++ snapshot bytes")
    return records


def _decode_batch(value: bytes) -> list[tuple[int, bytes, bytes]]:
    count, offset = _u32(value, 0)
    if count == 0 or count > 100_000:
        raise CorruptCppStorage("invalid C++ WAL batch count")
    mutations: list[tuple[int, bytes, bytes]] = []
    for _ in range(count):
        if offset >= len(value):
            raise CorruptCppStorage("truncated C++ WAL batch")
        operation = value[offset]
        offset += 1
        key, offset = _sized(value, offset)
        item_value, offset = _sized(value, offset)
        if operation not in (OP_SET, OP_DELETE) or not key or len(key) > MAX_KEY_SIZE:
            raise CorruptCppStorage("invalid C++ WAL batch mutation")
        mutations.append((operation, key, item_value))
    if offset != len(value):
        raise CorruptCppStorage("trailing C++ WAL batch bytes")
    return mutations


def replay_wal(data: bytes, records: dict[bytes, bytes]) -> int | None:
    """Replay LVK2 records and return the start of an ignored torn final record."""
    offset = 0
    torn_tail_at: int | None = None
    while offset < len(data):
        record_start = offset
        if offset + 15 > len(data):
            torn_tail_at = record_start
            break
        magic, version, operation, reserved, key_size, value_size = struct.unpack_from("<IBBBII", data, offset)
        offset += 15
        if magic != WAL_MAGIC or version != 1 or reserved != 0:
            raise CorruptCppStorage("corrupt C++ WAL header")
        if key_size == 0 or key_size > MAX_KEY_SIZE or value_size > MAX_VALUE_SIZE:
            raise CorruptCppStorage("invalid C++ WAL record lengths")
        end = offset + key_size + value_size
        if end + 4 > len(data):
            torn_tail_at = record_start
            break
        key = data[offset:offset + key_size]
        value = data[offset + key_size:end]
        stored_crc = struct.unpack_from("<I", data, end)[0]
        checksum_data = data[record_start + 4:end]
        if zlib.crc32(checksum_data) & 0xFFFFFFFF != stored_crc:
            raise CorruptCppStorage("corrupt C++ WAL checksum")
        if operation == OP_BATCH and key != b"__batch__":
            raise CorruptCppStorage("invalid C++ WAL batch key")
        mutations = [(operation, key, value)] if operation != OP_BATCH else _decode_batch(value)
        if operation not in (OP_SET, OP_DELETE, OP_BATCH):
            raise CorruptCppStorage("unknown C++ WAL operation")
        # Validate the full record before applying any of its batched mutations.
        for mutation, mutation_key, mutation_value in mutations:
            if not mutation_key or len(mutation_key) > MAX_KEY_SIZE or len(mutation_value) > MAX_VALUE_SIZE:
                raise CorruptCppStorage("invalid C++ WAL mutation")
            if mutation == OP_SET:
                records[mutation_key] = mutation_value
            elif mutation == OP_DELETE:
                records.pop(mutation_key, None)
            else:
                raise CorruptCppStorage("invalid C++ WAL mutation operation")
        offset = end + 4
    return torn_tail_at


def recover_cpp_local_kv(snapshot: Path | None, wal: Path | None) -> tuple[dict[bytes, bytes], int | None]:
    records = decode_snapshot(snapshot.read_bytes()) if snapshot is not None and snapshot.exists() else {}
    torn_tail_at = replay_wal(wal.read_bytes(), records) if wal is not None and wal.exists() else None
    return records, torn_tail_at

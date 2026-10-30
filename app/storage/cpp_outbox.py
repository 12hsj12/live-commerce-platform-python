from __future__ import annotations

import struct
import zlib
from dataclasses import dataclass


OUTBOX_MAGIC = 0x4F425832  # OBX2, written little-endian on supported C++ hosts.
MAX_RECORD_SIZE = 64 * 1024 * 1024
MAX_FIELD_SIZE = 16 * 1024 * 1024


class CorruptCppOutbox(ValueError):
    """Raised when a complete C++ OBX2 record is corrupt or truncated."""


@dataclass(frozen=True)
class CppOutboxEvent:
    event_id: str | None
    topic: str | None
    key: bytes
    payload: bytes
    sequence: int
    raw_record: bytes = b""


def _read_field(data: bytes, offset: int) -> tuple[bytes, int]:
    if offset + 4 > len(data):
        raise CorruptCppOutbox("truncated OBX2 field size")
    size = struct.unpack_from("<I", data, offset)[0]
    offset += 4
    if size > MAX_FIELD_SIZE or offset + size > len(data):
        raise CorruptCppOutbox("invalid OBX2 field size")
    return data[offset:offset + size], offset + size


def decode_outbox(data: bytes) -> list[CppOutboxEvent]:
    """Decode pending records from C++ FileOutbox without changing the source."""
    events: list[CppOutboxEvent] = []
    offset = 0
    while offset < len(data):
        record_start = offset
        if offset + 8 > len(data):
            raise CorruptCppOutbox("truncated OBX2 record header")
        magic, size = struct.unpack_from("<II", data, offset)
        offset += 8
        if magic != OUTBOX_MAGIC or size > MAX_RECORD_SIZE:
            raise CorruptCppOutbox("invalid OBX2 record header")
        end = offset + size
        if end + 4 > len(data):
            raise CorruptCppOutbox("truncated OBX2 record")
        payload = data[offset:end]
        checksum = struct.unpack_from("<I", data, end)[0]
        if zlib.crc32(payload) & 0xFFFFFFFF != checksum:
            raise CorruptCppOutbox("invalid OBX2 checksum")
        cursor = 0
        event_id_raw, cursor = _read_field(payload, cursor)
        topic_raw, cursor = _read_field(payload, cursor)
        key, cursor = _read_field(payload, cursor)
        body, cursor = _read_field(payload, cursor)
        if cursor + 8 != len(payload):
            raise CorruptCppOutbox("invalid OBX2 event payload")
        sequence = struct.unpack_from("<Q", payload, cursor)[0]
        if not topic_raw:
            raise CorruptCppOutbox("OBX2 event topic is empty")
        try:
            event_id = event_id_raw.decode("utf-8")
        except UnicodeDecodeError:
            event_id = None
        try:
            topic = topic_raw.decode("utf-8")
        except UnicodeDecodeError:
            topic = None
        events.append(CppOutboxEvent(event_id, topic, key, body, sequence, data[record_start:end + 4]))
        offset = end + 4
    return events

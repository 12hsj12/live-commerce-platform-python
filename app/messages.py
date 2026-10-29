from __future__ import annotations

import struct
from dataclasses import dataclass


MAGIC = 0x464F5131  # FOQ1; wire-compatible with messaging/order_message.cc.


@dataclass(frozen=True)
class FlashSaleMessage:
    promotion_id: str
    order_id: str
    idempotency_key: str
    user_id: str
    sku_id: str
    quantity: int = 1
    version: int = 1

    def validate(self) -> None:
        fields = ((self.promotion_id, 64), (self.order_id, 64), (self.idempotency_key, 128),
                  (self.user_id, 64), (self.sku_id, 64))
        if self.version != 1 or any(not value or len(value.encode()) > limit for value, limit in fields):
            raise ValueError("invalid flash-sale order message")
        if self.quantity <= 0 or self.quantity > 0xFFFFFFFF:
            raise ValueError("invalid flash-sale order message")

    def encode(self) -> bytes:
        self.validate()
        output = bytearray(struct.pack(">IH", MAGIC, self.version))
        for value, _ in ((self.promotion_id, 64), (self.order_id, 64), (self.idempotency_key, 128),
                         (self.user_id, 64), (self.sku_id, 64)):
            encoded = value.encode()
            output += struct.pack(">I", len(encoded)) + encoded
        output += struct.pack(">I", self.quantity)
        return bytes(output)

    @classmethod
    def decode(cls, payload: bytes) -> "FlashSaleMessage":
        if len(payload) < 6 or len(payload) > 32 * 1024:
            raise ValueError("invalid flash-sale order payload")
        magic, version = struct.unpack_from(">IH", payload, 0)
        if magic != MAGIC:
            raise ValueError("malformed flash-sale order payload")
        offset = 6
        fields: list[str] = []
        for _ in range(5):
            if offset + 4 > len(payload):
                raise ValueError("malformed flash-sale order payload")
            length = struct.unpack_from(">I", payload, offset)[0]
            offset += 4
            if length > 4096 or offset + length > len(payload):
                raise ValueError("malformed flash-sale order payload")
            fields.append(payload[offset:offset + length].decode())
            offset += length
        if offset + 4 != len(payload):
            raise ValueError("malformed flash-sale order quantity")
        quantity = struct.unpack_from(">I", payload, offset)[0]
        message = cls(*fields, quantity=quantity, version=version)
        message.validate()
        return message

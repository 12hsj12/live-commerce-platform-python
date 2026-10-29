from __future__ import annotations

import hashlib
import hmac
import re
import secrets
from typing import Optional


def hash_password(password: str) -> bytes:
    salt = secrets.token_bytes(16)
    derived = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 210000, dklen=32)
    encoded = f"pbkdf2-sha256$210000${salt.hex()}${derived.hex()}"
    return encoded.encode("ascii")


def verify_password(password: str, encoded: bytes | str) -> bool:
    try:
        value = encoded.decode("ascii") if isinstance(encoded, bytes) else encoded
        algorithm, rounds_text, salt_text, expected_text = value.split("$", 3)
        rounds = int(rounds_text)
        if algorithm != "pbkdf2-sha256" or not 10000 <= rounds <= 10000000:
            return False
        salt = bytes.fromhex(salt_text)
        expected = bytes.fromhex(expected_text)
        if len(salt) != 16 or len(expected) != 32:
            return False
        actual = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, rounds, dklen=32)
        return hmac.compare_digest(actual, expected)
    except (ValueError, UnicodeError):
        return False


def valid_identifier(value: str) -> bool:
    return bool(value) and len(value) <= 64 and re.fullmatch(r"[A-Za-z0-9_.-]+", value) is not None


def make_order_id(campaign_id: str, user_id: str) -> str:
    digest = hashlib.sha256(campaign_id.encode() + b"\0" + user_id.encode()).hexdigest()[:32]
    return f"flash-{digest}"


def verify_timestamped_hmac(secret: str, timestamp: str, raw_body: bytes, signature: str) -> bool:
    expected = hmac.new(secret.encode(), timestamp.encode() + b"." + raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)

from __future__ import annotations

import secrets
import time
from dataclasses import dataclass

from sqlalchemy import select

from ..models import User
from ..security import hash_password, verify_password


@dataclass(frozen=True)
class AuthUser:
    id: str
    username: str
    role: str
    enabled: bool


class AuthService:
    SESSION_TTL_SECONDS = 24 * 60 * 60

    def __init__(self, database, redis=None):
        self.database = database
        self.redis = redis
        self.local_sessions: dict[str, tuple[str, int]] = {}

    async def register(self, username: str, password: str) -> AuthUser:
        if (not username or len(username.encode()) > 128 or not password or len(password.encode()) > 1024
                or any(ch in username for ch in "\t\r\n")):
            raise ValueError("username, password and output are required")
        encoded = hash_password(password)
        async with self.database.sessions.begin() as session:
            existing = (await session.execute(select(User).where(User.username == username))).scalar_one_or_none()
            if existing is not None:
                raise FileExistsError("username already exists")
            if self.redis is not None:
                # Preserve the C++ Redis username claim and shared user encoding.
                user_id = "user-" + secrets.token_hex(12)
                claimed = await self.redis.set(f"user:username:{username}", user_id, nx=True)
                if not claimed:
                    raise FileExistsError("username already exists")
            else:
                user_id = "user-1"
                rows = (await session.execute(select(User.id))).scalars().all()
                numeric_ids = [int(item[5:]) for item in rows if item.startswith("user-") and item[5:].isdigit()]
                user_id = f"user-{max(numeric_ids, default=0) + 1}"
            user = User(id=user_id, username=username, role="customer", password_hash=encoded, enabled=True)
            session.add(user)
            try:
                await session.flush()
            except Exception:
                if self.redis is not None:
                    await self.redis.delete(f"user:username:{username}")
                raise
            if self.redis is not None:
                await self.redis.set(f"user:{user_id}", self._serialize_user(user))
        return AuthUser(user.id, user.username, user.role, user.enabled)

    async def login(self, username: str, password: str) -> tuple[str, AuthUser, int]:
        async with self.database.sessions() as session:
            user = (await session.execute(select(User).where(User.username == username))).scalar_one_or_none()
            if user is None and self.redis is not None:
                user_id = await self.redis.get(f"user:username:{username}")
                if user_id:
                    raw = await self.redis.get(f"user:{self._text(user_id)}")
                    shared = self._deserialize_user(raw) if raw else None
                    if shared is not None:
                        user = (await session.execute(select(User).where(User.id == shared.id))).scalar_one_or_none()
                        if user is None:
                            user = User(id=shared.id, username=shared.username, role=shared.role,
                                        password_hash=shared.password_hash, enabled=shared.enabled)
                            session.add(user)
                            await session.commit()
            if user is None or not user.enabled or not verify_password(password, user.password_hash):
                raise PermissionError("invalid credentials")
            user_snapshot = AuthUser(user.id, user.username, user.role, user.enabled)
        token = secrets.token_hex(32)
        expires_at = int(time.time()) + self.SESSION_TTL_SECONDS
        if self.redis is not None:
            await self.redis.set(f"session:{token}", user_snapshot.id, ex=self.SESSION_TTL_SECONDS)
        else:
            self.local_sessions[token] = (user_snapshot.id, expires_at)
        return token, user_snapshot, expires_at

    async def authenticate(self, token: str) -> AuthUser:
        if not token:
            raise PermissionError("invalid or expired session")
        if self.redis is not None:
            user_id = await self.redis.get(f"session:{token}")
            user_id = self._text(user_id) if user_id else None
        else:
            value = self.local_sessions.get(token)
            if value is None:
                raise PermissionError("invalid session")
            user_id, expiry = value
            if expiry <= int(time.time()):
                self.local_sessions.pop(token, None)
                raise PermissionError("session expired")
        if user_id is None:
            raise PermissionError("invalid or expired session")
        async with self.database.sessions() as session:
            user = await session.get(User, user_id)
            if user is None and self.redis is not None:
                raw = await self.redis.get(f"user:{user_id}")
                shared = self._deserialize_user(raw) if raw else None
                if shared is not None and shared.id == user_id:
                    user = User(id=shared.id, username=shared.username, role=shared.role,
                                password_hash=shared.password_hash, enabled=shared.enabled)
                    session.add(user)
                    await session.commit()
            if user is None or not user.enabled:
                raise PermissionError("user disabled")
            return AuthUser(user.id, user.username, user.role, user.enabled)

    async def logout(self, token: str) -> None:
        if self.redis is not None:
            await self.redis.delete(f"session:{token}")
        elif token not in self.local_sessions:
            raise LookupError("session not found")
        else:
            self.local_sessions.pop(token, None)

    @staticmethod
    def _serialize_user(user: User) -> str:
        roles = {"customer": 0, "merchant": 1, "admin": 2}
        password_hash = user.password_hash.decode("ascii") if isinstance(user.password_hash, bytes) else user.password_hash
        return f"{user.id}\t{user.username}\t{password_hash}\t{roles.get(user.role, 0)}\t{1 if user.enabled else 0}"

    @staticmethod
    def _deserialize_user(raw) -> User | None:
        try:
            value = AuthService._text(raw)
            user_id, username, encoded, role_value, enabled = value.split("\t")
            role = ("customer", "merchant", "admin")[int(role_value)]
            return User(id=user_id, username=username, password_hash=encoded.encode("ascii"),
                        role=role, enabled=enabled == "1")
        except (ValueError, IndexError, UnicodeError):
            return None

    @staticmethod
    def _text(value) -> str:
        return value.decode() if isinstance(value, bytes) else str(value)

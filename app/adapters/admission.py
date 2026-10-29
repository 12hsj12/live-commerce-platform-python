from __future__ import annotations

import asyncio
import time

from sqlalchemy import delete, func, select

from ..models import Campaign, CampaignAdmission
from ..messages import FlashSaleMessage


class LocalTokenBucket:
    def __init__(self, rate: float, capacity: float):
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self.lock:
            now = time.monotonic()
            self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.rate)
            self.last = now
            if self.tokens < 1:
                return False
            self.tokens -= 1
            return True


class AdmissionService:
    _gate_acquire = """
    local state=redis.call('HGET',KEYS[1],'state');
    if state=='created' or state=='preheating' then return -1 end;
    if state=='finished' then return -2 end; if state=='paused' then return -3 end;
    if redis.call('SISMEMBER',KEYS[2],ARGV[1])==1 then return 2 end;
    local admitted=tonumber(redis.call('HGET',KEYS[1],'admitted')) or 0;
    local quota=tonumber(redis.call('HGET',KEYS[1],'quota')) or 0;
    if admitted>=quota then return 0 end;
    redis.call('SADD',KEYS[2],ARGV[1]); redis.call('HINCRBY',KEYS[1],'admitted',1); return 1
    """
    _gate_rollback = "if redis.call('SREM',KEYS[2],ARGV[1])==1 then redis.call('HINCRBY',KEYS[1],'admitted',-1) end; return 1"
    _gate_configure = """
    if redis.call('EXISTS',KEYS[1])==1 then
      if redis.call('HGET',KEYS[1],'quota')~=ARGV[1] then
        if redis.call('HGET',KEYS[1],'state')~='created' or (tonumber(redis.call('HGET',KEYS[1],'admitted')) or 0)>0 then return 0 end;
        redis.call('HSET',KEYS[1],'quota',ARGV[1]); return 3
      end; return 2
    end
    redis.call('HSET',KEYS[1],'state','created','quota',ARGV[1],'admitted','0'); return 1
    """
    _gate_transition = """
    local state=redis.call('HGET',KEYS[1],'state');
    if state==ARGV[2] then return 1 end;
    if ARGV[2]=='preheating' and (state=='running' or state=='finished') then return 1 end;
    if ARGV[2]=='running' and state=='finished' then return 1 end;
    if ARGV[2]=='finished' and state=='paused' then redis.call('HSET',KEYS[1],'state','finished'); return 1 end;
    if state~=ARGV[1] then return 0 end;
    redis.call('HSET',KEYS[1],'state',ARGV[2]); return 1
    """
    _bucket = """
    local t=redis.call('TIME'); local now=tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000);
    local capacity=tonumber(ARGV[1]); local rate=tonumber(ARGV[2]); local cost=tonumber(ARGV[3]);
    local tokens=tonumber(redis.call('HGET',KEYS[1],'tokens')) or capacity;
    local last=tonumber(redis.call('HGET',KEYS[1],'last_ms')) or now;
    if now>last then tokens=math.min(capacity,tokens+(now-last)*rate/1000); last=now end;
    local allowed=0; if tokens>=cost then tokens=tokens-cost; allowed=1 end;
    redis.call('HSET',KEYS[1],'tokens',tokens,'last_ms',last); redis.call('PEXPIRE',KEYS[1],ARGV[4]); return allowed
    """

    def __init__(self, database, redis=None, rate_redis=None):
        self.database = database
        self.redis = redis
        self.rate_redis = rate_redis if rate_redis is not None else redis
        self.local_gate_lock = asyncio.Lock()

    async def configure(self, campaign: Campaign) -> None:
        if self.redis is not None:
            meta = f"flash_admission:{{{campaign.id}}}:meta"
            users = f"flash_admission:{{{campaign.id}}}:users"
            configured = int(await self.redis.eval(self._gate_configure, 2, meta, users, campaign.participant_limit))
            if configured == 0:
                raise RuntimeError("Redis admission quota mismatch")
            state = campaign.state.lower()
            await self.redis.hset(meta, "state", state)

    async def acquire(self, campaign_id: str, user_id: str) -> str:
        if self.redis is not None:
            meta = f"flash_admission:{{{campaign_id}}}:meta"
            users = f"flash_admission:{{{campaign_id}}}:users"
            result = int(await self.redis.eval(self._gate_acquire, 2, meta, users, user_id))
            return {1: "accepted", 2: "duplicate", 0: "exhausted", -2: "finished", -3: "paused", -1: "not_started"}.get(result, "not_started")
        async with self.local_gate_lock:
            return await self._acquire_local(campaign_id, user_id)

    async def _acquire_local(self, campaign_id: str, user_id: str) -> str:
        async with self.database.sessions.begin() as session:
            campaign = await session.get(Campaign, campaign_id, with_for_update=True)
            if campaign is None:
                raise LookupError("promotion not found")
            if campaign.state in {"CREATED", "PREHEATING"}:
                return "not_started"
            if campaign.state == "FINISHED":
                return "finished"
            if campaign.state == "PAUSED":
                return "paused"
            existing = await session.get(CampaignAdmission, (campaign_id, user_id))
            if existing is not None:
                return "duplicate"
            count = await session.scalar(select(func.count()).select_from(CampaignAdmission).where(CampaignAdmission.campaign_id == campaign_id))
            if (count or 0) >= campaign.participant_limit:
                return "exhausted"
            session.add(CampaignAdmission(campaign_id=campaign_id, user_id=user_id))
            return "accepted"

    async def rollback(self, campaign_id: str, user_id: str) -> None:
        if self.redis is not None:
            await self.redis.eval(self._gate_rollback, 2,
                f"flash_admission:{{{campaign_id}}}:meta", f"flash_admission:{{{campaign_id}}}:users", user_id)
            return
        async with self.database.sessions.begin() as session:
            await session.execute(delete(CampaignAdmission).where(
                CampaignAdmission.campaign_id == campaign_id, CampaignAdmission.user_id == user_id))

    async def transition(self, campaign_id: str, state: str, expected: str | None = None) -> None:
        if self.redis is not None:
            key = f"flash_admission:{{{campaign_id}}}:meta"
            if expected is None:
                await self.redis.hset(key, "state", state.lower())
            else:
                result = int(await self.redis.eval(self._gate_transition, 1, key, expected.lower(), state.lower()))
                if result != 1:
                    raise RuntimeError("invalid Redis gate transition")

    async def take_redis_bucket(self, key: str, rate: float, burst: float) -> bool:
        if self.rate_redis is None:
            raise RuntimeError("Redis token bucket is not configured")
        rate_milli = round(rate * 1000)
        capacity_milli = round(burst * 1000)
        if rate_milli <= 0 or capacity_milli <= 0:
            return False
        ttl_ms = max(60000, round(capacity_milli * 1000 / rate_milli * 2))
        return int(await self.rate_redis.eval(self._bucket, 1, key, capacity_milli, rate_milli, 1000, ttl_ms)) == 1


class RedisWaitingRoom:
    ENQUEUE = """
    local rank=redis.call('ZRANK',KEYS[1],ARGV[1]); if rank then return rank+1 end;
    if redis.call('ZCARD',KEYS[1])>=tonumber(ARGV[3]) then return -1 end;
    local seq=redis.call('INCR',KEYS[5]); redis.call('ZADD',KEYS[1],seq,ARGV[1]);
    redis.call('HSET',KEYS[2],ARGV[1],ARGV[2]); return redis.call('ZCARD',KEYS[1])
    """
    CLAIM = """
    local t=redis.call('TIME'); local now=tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000);
    local candidates=redis.call('ZRANGE',KEYS[1],0,tonumber(ARGV[3])-1);
    for _,ticket in ipairs(candidates) do
      local expires=tonumber(redis.call('ZSCORE',KEYS[4],ticket) or '0');
      if expires<=now then local payload=redis.call('HGET',KEYS[2],ticket);
        if payload then redis.call('HSET',KEYS[3],ticket,ARGV[1]); redis.call('ZADD',KEYS[4],now+tonumber(ARGV[2]),ticket);
          return string.len(ticket)..':'..ticket..payload;
        else redis.call('ZREM',KEYS[1],ticket); end
      end
    end; return nil
    """
    COMPLETE = """
    if redis.call('HGET',KEYS[3],ARGV[1])~=ARGV[2] then return 0 end;
    redis.call('ZREM',KEYS[1],ARGV[1]); redis.call('HDEL',KEYS[2],ARGV[1]);
    redis.call('HDEL',KEYS[3],ARGV[1]); redis.call('ZREM',KEYS[4],ARGV[1]);
    if redis.call('ZCARD',KEYS[1])==0 then redis.call('DEL',KEYS[1],KEYS[2],KEYS[3],KEYS[4],KEYS[5]); end; return 1
    """
    RELEASE = """
    if redis.call('HGET',KEYS[1],ARGV[1])~=ARGV[2] then return 0 end;
    redis.call('HDEL',KEYS[1],ARGV[1]); redis.call('ZREM',KEYS[2],ARGV[1]); return 1
    """
    POSITION = "local rank=redis.call('ZRANK',KEYS[1],ARGV[1]); if not rank then return 0 end; return rank+1"

    def __init__(self, redis, campaign_id: str, capacity: int):
        self.redis = redis
        tag = f"{{{campaign_id}}}"
        base = "flashsale:waiting:" + tag
        self.keys = (base + ":queue", base + ":payloads", base + ":leases", base + ":lease-expiry", base + ":sequence")
        self.campaign_id = campaign_id
        self.capacity = capacity

    async def enqueue(self, message: FlashSaleMessage) -> int:
        result = int(await self.redis.eval(self.ENQUEUE, 5, *self.keys, message.idempotency_key,
                                          message.encode(), self.capacity))
        if result < 0:
            raise OverflowError("flash-sale waiting room is full")
        return result

    async def position(self, ticket: str) -> int:
        return int(await self.redis.eval(self.POSITION, 1, self.keys[0], ticket))

    async def claim(self, owner: str, lease_ms: int = 30000) -> tuple[str, FlashSaleMessage] | None:
        value = await self.redis.eval(self.CLAIM, 4, *self.keys[:4], owner, lease_ms, 64)
        if value is None:
            return None
        raw = value.decode() if isinstance(value, bytes) else value
        colon = raw.find(":")
        length = int(raw[:colon])
        ticket = raw[colon + 1:colon + 1 + length]
        payload = raw[colon + 1 + length:].encode()
        return ticket, FlashSaleMessage.decode(payload)

    async def complete(self, ticket: str, owner: str) -> bool:
        return int(await self.redis.eval(self.COMPLETE, 5, *self.keys, ticket, owner)) == 1

    async def release(self, ticket: str, owner: str) -> bool:
        return int(await self.redis.eval(self.RELEASE, 2, self.keys[2], self.keys[3], ticket, owner)) == 1


async def find_waiting_campaign(database, redis, request_id: str, capacity: int) -> tuple[str | None, int]:
    if redis is None:
        return None, 0
    async with database.sessions() as session:
        campaigns = (await session.execute(select(Campaign.id))).scalars().all()
    for campaign_id in campaigns:
        position = await RedisWaitingRoom(redis, campaign_id, capacity).position(request_id)
        if position:
            return campaign_id, position
    return None, 0

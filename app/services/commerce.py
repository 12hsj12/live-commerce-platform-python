from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import func, select, update

from ..adapters.admission import AdmissionService
from ..adapters.inventory import InventoryGateway
from ..adapters.queue import LocalOrderQueue, RedisOrderQueue
from ..messages import FlashSaleMessage
from ..models import (AdminAuditLog, Campaign, CampaignAdmission, FlashSaleRequest, Inventory,
                      Order, OrderItem, OutboxEvent, Payment, Product)
from ..security import make_order_id, valid_identifier
from .request_status import RequestStatus, RequestStatusStore, TERMINAL


class ServiceFault(Exception):
    def __init__(self, status_code: int, message: str):
        super().__init__(message)
        self.status_code = status_code
        self.message = message


@dataclass
class OrderView:
    id: str
    user_id: str
    campaign_id: str
    idempotency_key: str
    state: str
    sku_id: str
    quantity: int
    unit_price_cents: int


class PromotionService:
    TRANSITIONS = {
        ("CREATED", "PREHEATING"),
        ("PREHEATING", "RUNNING"),
        ("RUNNING", "PAUSED"),
        ("PAUSED", "RUNNING"),
        ("RUNNING", "FINISHED"),
        ("PAUSED", "FINISHED"),
    }

    def __init__(self, database, inventory: InventoryGateway, admission: AdmissionService):
        self.database = database
        self.inventory = inventory
        self.admission = admission
        self.allocation_lock = asyncio.Lock()

    async def get(self, campaign_id: str) -> Campaign | None:
        async with self.database.sessions() as session:
            return await session.get(Campaign, campaign_id)

    async def list(self) -> list[Campaign]:
        async with self.database.sessions() as session:
            return list((await session.execute(select(Campaign).order_by(Campaign.id))).scalars())

    async def create(self, campaign_id: str, sku_id: str, limit: int, starts_at: int, ends_at: int, actor: str) -> None:
        if (not valid_identifier(campaign_id) or not valid_identifier(sku_id) or limit <= 0 or starts_at < 0 or
                ends_at < 0 or (ends_at and starts_at >= ends_at)):
            raise ServiceFault(400, "invalid campaign fields or scheduled window; starts_at and ends_at must be Unix seconds")
        async with self.allocation_lock:
            candidate = Campaign(id=campaign_id, sku_id=sku_id, participant_limit=limit,
                                 state="CREATED", starts_at=starts_at, ends_at=ends_at)
            # The C++ PromotionService configures its admission gate before it
            # persists the campaign. Keep that ordering for Redis-backed gates.
            await self.admission.configure(candidate)
            async with self.database.sessions.begin() as session:
                existing = await session.get(Campaign, campaign_id)
                if existing is not None:
                    raise ServiceFault(409, "campaign already exists")
                product = await session.get(Product, sku_id)
                if product is None or not product.on_sale:
                    raise ServiceFault(404, "active SKU not found")
                stock = await session.get(Inventory, sku_id, with_for_update=True)
                if stock is None or stock.available < limit:
                    raise ServiceFault(409, "SKU stock is insufficient for campaign quota")
                allocation = await self._allocated_quota(session, sku_id, exclude=None)
                if stock.available < allocation + limit:
                    raise ServiceFault(409, "campaign quotas exceed remaining SKU stock")
                session.add(Campaign(id=campaign_id, sku_id=sku_id, participant_limit=limit,
                    state="CREATED", starts_at=starts_at, ends_at=ends_at))
                if self.database.settings.mysql_mode:
                    session.add(AdminAuditLog(actor=actor[:128], action="CAMPAIGN_CREATED", resource_type="campaign",
                        resource_id=campaign_id, previous_value="", new_value=f"{sku_id}:{limit}:{starts_at}:{ends_at}"))

    async def update(self, campaign_id: str, sku_id: str, limit: int, starts_at: int, ends_at: int, actor: str) -> None:
        if (not valid_identifier(campaign_id) or not valid_identifier(sku_id) or limit <= 0 or starts_at < 0 or
                ends_at < 0 or (ends_at and starts_at >= ends_at)):
            raise ServiceFault(400, "invalid campaign fields or scheduled window; starts_at and ends_at must be Unix seconds")
        async with self.allocation_lock:
            previous = await self.get(campaign_id)
            if previous is None:
                raise ServiceFault(404, "promotion not found")
            if previous.state != "CREATED":
                raise ServiceFault(409, "only created promotions can be edited")
            candidate = Campaign(id=campaign_id, sku_id=sku_id, participant_limit=limit,
                                 state="CREATED", starts_at=starts_at, ends_at=ends_at)
            await self.admission.configure(candidate)
            try:
                async with self.database.sessions.begin() as session:
                    campaign = await session.get(Campaign, campaign_id, with_for_update=True)
                    if campaign is None:
                        raise ServiceFault(404, "promotion not found")
                    if campaign.state != "CREATED":
                        raise ServiceFault(409, "only created promotions can be edited")
                    product = await session.get(Product, sku_id)
                    if product is None or not product.on_sale:
                        raise ServiceFault(404, "active SKU not found")
                    stock = await session.get(Inventory, sku_id, with_for_update=True)
                    if stock is None or stock.available < limit:
                        raise ServiceFault(409, "SKU stock is insufficient for campaign quota")
                    allocation = await self._allocated_quota(session, sku_id, exclude=campaign_id)
                    if stock.available < allocation + limit:
                        raise ServiceFault(409, "campaign quotas exceed remaining SKU stock")
                    before = f"{campaign.sku_id}:{campaign.participant_limit}:{campaign.starts_at}:{campaign.ends_at}"
                    after = f"{sku_id}:{limit}:{starts_at}:{ends_at}"
                    campaign.sku_id, campaign.participant_limit = sku_id, limit
                    campaign.starts_at, campaign.ends_at = starts_at, ends_at
                    if self.database.settings.mysql_mode:
                        session.add(AdminAuditLog(actor=actor[:128], action="CAMPAIGN_UPDATED", resource_type="campaign",
                            resource_id=campaign_id, previous_value=before, new_value=after))
            except Exception:
                try:
                    await self.admission.configure(previous)
                except Exception:
                    pass
                raise

    async def _allocated_quota(self, session, sku_id: str, exclude: str | None) -> int:
        now = int(time.time())
        query = select(Campaign).where(Campaign.sku_id == sku_id, Campaign.state != "FINISHED",
                                       (Campaign.ends_at == 0) | (Campaign.ends_at > now))
        if exclude is not None:
            query = query.where(Campaign.id != exclude)
        campaigns = list((await session.execute(query)).scalars())
        if self.database.settings.mysql_mode:
            allocated = 0
            for campaign in campaigns:
                count = await session.scalar(select(func.count()).select_from(Order).where(
                    Order.campaign_id == campaign.id, Order.state.in_(("PENDING_PAYMENT", "PAID"))))
                allocated += max(campaign.participant_limit - int(count or 0), 0)
            return allocated
        return sum(campaign.participant_limit for campaign in campaigns)

    async def transition(self, campaign_id: str, action: str, actor: str) -> Campaign:
        target = {"preheat": "PREHEATING", "start": "RUNNING", "pause": "PAUSED",
                  "resume": "RUNNING", "finish": "FINISHED"}[action]
        async with self.allocation_lock:
            current = await self.get(campaign_id)
            if current is None:
                raise ServiceFault(404, "promotion not found")
            previous = current.state
            if (previous, target) not in self.TRANSITIONS:
                raise ServiceFault(409, f"invalid promotion {action} transition")
            try:
                await self.admission.transition(campaign_id, target, expected=previous)
            except Exception as exc:
                raise ServiceFault(409, "invalid promotion transition") from exc
            try:
                async with self.database.sessions.begin() as session:
                    changed = await session.execute(update(Campaign).where(
                        Campaign.id == campaign_id, Campaign.state == previous).values(state=target))
                    if changed.rowcount != 1:
                        raise ServiceFault(409, "campaign state changed concurrently or campaign is missing")
                    if self.database.settings.mysql_mode:
                        session.add(AdminAuditLog(actor=actor[:128], action="CAMPAIGN_STATE_CHANGED", resource_type="campaign",
                            resource_id=campaign_id, previous_value=previous, new_value=target))
            except Exception:
                try:
                    await self.admission.transition(campaign_id, previous)
                except Exception:
                    pass
                raise
            return await self.get(campaign_id)

    async def list_audit(self, limit: int) -> list[AdminAuditLog]:
        if not self.database.settings.mysql_mode:
            raise ServiceFault(503, "audit storage requires MySQL")
        async with self.database.sessions() as session:
            return list((await session.execute(select(AdminAuditLog).order_by(AdminAuditLog.id.desc()).limit(limit))).scalars())


class FlashSaleService:
    def __init__(self, database, queue: LocalOrderQueue | RedisOrderQueue, admission: AdmissionService,
                 statuses: RequestStatusStore, promotions: PromotionService):
        self.database = database
        self.queue = queue
        self.admission = admission
        self.statuses = statuses
        self.promotions = promotions

    async def submit(self, campaign_id: str, sku_id: str, user_id: str, request_id: str,
                     quantity: int = 1) -> dict:
        if (not campaign_id or not sku_id or not user_id or not request_id or quantity != 1 or
                len(campaign_id.encode()) > 64 or len(sku_id.encode()) > 64 or len(user_id.encode()) > 64 or
                len(request_id.encode()) > 128):
            raise ServiceFault(400, "invalid flash-sale order request; this campaign permits one unit per user")
        existing = await self.statuses.get(request_id)
        waiting_claim = False
        submission_claim = False
        if existing is not None:
            if existing.user_id != user_id:
                raise ServiceFault(403, "idempotency key belongs to another user")
            if existing.state == "WAITING":
                waiting_claim = True
                if existing.promotion_id and existing.promotion_id != campaign_id:
                    raise ServiceFault(409, "waiting-room ticket belongs to another campaign")
            elif existing.state == "SUBMITTING":
                submission_claim = True
            else:
                return {"code": "DUPLICATE", "request_id": existing.request_id, "order_id": existing.order_id,
                        "message": f"request already exists with state {existing.state}"}

        campaign = await self.promotions.get(campaign_id)
        if campaign is None:
            raise ServiceFault(503, "promotion not found")
        if campaign.sku_id != sku_id:
            raise ServiceFault(503, "SKU does not belong to the requested campaign")
        now = int(time.time())
        if campaign.starts_at and now < campaign.starts_at:
            return {"code": "NOT_STARTED", "order_id": make_order_id(campaign_id, user_id), "request_id": request_id,
                    "message": "campaign has not reached its scheduled start time"}
        if campaign.ends_at and now >= campaign.ends_at:
            return {"code": "FINISHED", "order_id": make_order_id(campaign_id, user_id), "request_id": request_id,
                    "message": "campaign has passed its scheduled end time"}

        order_id = make_order_id(campaign_id, user_id)
        if self.statuses.redis is not None and not waiting_claim and not submission_claim:
            claim = RequestStatus(request_id, user_id, order_id, "SUBMITTING", "request submission is in progress", campaign_id)
            if not await self.statuses.put_if_absent(claim):
                raced = await self.statuses.get(request_id)
                if raced is None:
                    raise ServiceFault(503, "request status unavailable")
                if raced.user_id != user_id:
                    raise ServiceFault(403, "idempotency key belongs to another user")
                if raced.state == "WAITING":
                    waiting_claim = True
                elif raced.state == "SUBMITTING":
                    submission_claim = True
                else:
                    return {"code": "DUPLICATE", "request_id": raced.request_id, "order_id": raced.order_id,
                            "message": f"request already exists with state {raced.state}"}
        elif self.statuses.redis is None and not waiting_claim and not submission_claim:
            claim = RequestStatus(request_id, user_id, order_id, "SUBMITTING", "request submission is in progress", campaign_id)
            if not await self.statuses.put_if_absent(claim):
                raced = await self.statuses.get(request_id)
                if raced is None:
                    raise ServiceFault(503, "request status unavailable")
                if raced.user_id != user_id:
                    raise ServiceFault(403, "idempotency key belongs to another user")
                if raced.state == "WAITING":
                    waiting_claim = True
                elif raced.state == "SUBMITTING":
                    submission_claim = True
                else:
                    return {"code": "DUPLICATE", "request_id": raced.request_id, "order_id": raced.order_id,
                            "message": f"request already exists with state {raced.state}"}

        decision = await self.admission.acquire(campaign_id, user_id)
        code_map = {"accepted": "QUEUED", "duplicate": "DUPLICATE", "finished": "FINISHED",
                    "exhausted": "EXHAUSTED", "not_started": "NOT_STARTED", "paused": "REJECTED"}
        result_code = code_map[decision]
        if decision == "duplicate" and not submission_claim and not waiting_claim:
            return {"code": "DUPLICATE", "request_id": "", "order_id": order_id, "message": "user already admitted"}
        if decision not in {"accepted", "duplicate"}:
            state = "SOLD_OUT" if decision == "exhausted" else "REJECTED"
            if (submission_claim or (not waiting_claim and not submission_claim)) and state:
                await self.statuses.transition(request_id, "SUBMITTING", state)
            return {"code": result_code, "request_id": request_id, "order_id": order_id,
                    "message": "promotion admission rejected"}

        message = FlashSaleMessage(campaign_id, order_id, request_id, user_id, sku_id, quantity)
        try:
            await self.queue.push(message)
        except (OverflowError, asyncio.QueueFull) as exc:
            if decision == "accepted":
                await self.admission.rollback(campaign_id, user_id)
            raise ServiceFault(503, str(exc)) from exc
        if not await self.statuses.transition(request_id, "WAITING" if waiting_claim else "SUBMITTING", "QUEUED"):
            # Broker acceptance may already have happened. Retrying this same request ID is safe.
            raise ServiceFault(503, "request status transition failed")
        return {"code": "QUEUED", "request_id": request_id, "order_id": order_id,
                "message": "order accepted and queued"}


class OrderService:
    def __init__(self, database, inventory: InventoryGateway, statuses: RequestStatusStore):
        self.database = database
        self.inventory = inventory
        self.statuses = statuses

    async def get(self, order_id: str) -> OrderView | None:
        async with self.database.sessions() as session:
            row = (await session.execute(select(Order, OrderItem).join(OrderItem, OrderItem.order_id == Order.id)
                                         .where(Order.id == order_id))).first()
            if row is None:
                return None
            order, item = row
            return OrderView(order.id, order.user_id, order.campaign_id, order.idempotency_key, order.state,
                             item.sku_id, item.quantity, item.unit_price_cents)

    async def persist_queued_order(self, message: FlashSaleMessage) -> OrderView:
        async with self.database.sessions.begin() as session:
            row = (await session.execute(select(Order, OrderItem).join(OrderItem, OrderItem.order_id == Order.id)
                .where(Order.idempotency_key == message.idempotency_key).with_for_update())).first()
            if row is not None:
                order, item = row
                if (order.id != message.order_id or order.user_id != message.user_id or item.sku_id != message.sku_id or
                        item.quantity != message.quantity):
                    raise ServiceFault(409, "flash-sale message conflicts with persisted order")
                return OrderView(order.id, order.user_id, order.campaign_id, order.idempotency_key, order.state,
                                 item.sku_id, item.quantity, item.unit_price_cents)

            request = await session.get(FlashSaleRequest, message.idempotency_key, with_for_update=True)
            if request is not None:
                if request.user_id != message.user_id:
                    raise ServiceFault(409, "request id belongs to another user")
                if request.state in TERMINAL:
                    raise ServiceFault(409, "terminal flash-sale request cannot create an order")

            if self.inventory.redis is not None:
                changed = await session.execute(update(Inventory).where(
                    Inventory.sku_id == message.sku_id, Inventory.available >= message.quantity).values(
                    available=Inventory.available - message.quantity,
                    reserved=Inventory.reserved + message.quantity,
                    version=Inventory.version + 1))
                if changed.rowcount != 1:
                    raise ServiceFault(409, "insufficient or missing MySQL inventory for flash-sale order")

            product = await session.get(Product, message.sku_id)
            unit_price = product.price_cents if product is not None else 0
            order = Order(id=message.order_id, user_id=message.user_id, campaign_id=message.promotion_id,
                          idempotency_key=message.idempotency_key, state="PENDING_PAYMENT",
                          expires_at=datetime.utcnow() + timedelta(minutes=15))
            session.add(order)
            await session.flush()
            item = OrderItem(order_id=message.order_id, sku_id=message.sku_id, quantity=message.quantity,
                             unit_price_cents=unit_price)
            session.add(item)
            if request is None:
                session.add(FlashSaleRequest(request_id=message.idempotency_key, user_id=message.user_id,
                    order_id=message.order_id, state="PENDING_PAYMENT", message="order created; payment is pending"))
            else:
                request.order_id = message.order_id
                request.state = "PENDING_PAYMENT"
                request.message = "order created; payment is pending"
            self._append_event(session, f"flash-order-created-{message.order_id}", "order.events", message.order_id,
                f"OrderCreated|{message.order_id}|{message.user_id}|{message.sku_id}|{message.quantity}")
            return OrderView(order.id, order.user_id, order.campaign_id, order.idempotency_key, order.state,
                             item.sku_id, item.quantity, item.unit_price_cents)

    async def transition(self, order_id: str, next_state: str) -> OrderView:
        if next_state not in {"PAID", "EXPIRED", "CANCELLED"}:
            raise ServiceFault(400, "invalid order transition")
        async with self.database.sessions.begin() as session:
            row = (await session.execute(select(Order, OrderItem).join(OrderItem, OrderItem.order_id == Order.id)
                                         .where(Order.id == order_id).with_for_update())).first()
            if row is None:
                raise ServiceFault(404, "order not found")
            order, item = row
            if order.state == next_state:
                return OrderView(order.id, order.user_id, order.campaign_id, order.idempotency_key, order.state,
                                 item.sku_id, item.quantity, item.unit_price_cents)
            if order.state != "PENDING_PAYMENT":
                raise ServiceFault(409, "order state changed concurrently")
            order.state = next_state
            order.expiry_claimed_by = None
            order.expiry_lease_until = None
            if self.database.settings.mysql_mode and self.inventory.redis is not None:
                if next_state == "PAID":
                    values = {"reserved": Inventory.reserved - item.quantity,
                              "sold": Inventory.sold + item.quantity,
                              "version": Inventory.version + 1}
                else:
                    values = {"available": Inventory.available + item.quantity,
                              "reserved": Inventory.reserved - item.quantity,
                              "version": Inventory.version + 1}
                changed = await session.execute(update(Inventory).where(
                    Inventory.sku_id == item.sku_id, Inventory.reserved >= item.quantity).values(**values))
                if changed.rowcount != 1:
                    raise ServiceFault(409, "durable inventory reservation is inconsistent")
            if next_state == "PAID" and self.database.settings.mysql_mode:
                amount = item.unit_price_cents * item.quantity
                session.add(Payment(id="payment-" + order_id, order_id=order_id, provider="mock",
                    provider_transaction_id="mock-tx-" + order_id, amount_cents=amount, state="SUCCEEDED"))
            if self.database.settings.mysql_mode:
                event_type = "StockConfirm" if next_state == "PAID" else "StockRelease"
                prefix = "stock-confirm-" if next_state == "PAID" else "stock-release-"
                self._append_event(session, prefix + order_id, "inventory.events", order_id,
                    f"{event_type}|{order_id}|{item.sku_id}|{item.quantity}")
                request = await session.get(FlashSaleRequest, order.idempotency_key, with_for_update=True)
                if request is not None:
                    request.state = next_state
                    request.message = {"PAID": "mock payment succeeded", "EXPIRED": "payment window expired",
                                       "CANCELLED": "order cancelled"}[next_state]
            return OrderView(order.id, order.user_id, order.campaign_id, order.idempotency_key, order.state,
                             item.sku_id, item.quantity, item.unit_price_cents)

    async def confirm_callback(self, order_id: str, provider: str, transaction_id: str, amount: int) -> OrderView:
        if not self.database.settings.mysql_mode:
            raise ServiceFault(503, "durable payment callbacks require MySQL")
        if not provider or len(provider) > 32 or not transaction_id or len(transaction_id) > 128 or amount < 0:
            raise ServiceFault(400, "invalid payment confirmation")
        async with self.database.sessions.begin() as session:
            row = (await session.execute(select(Order, OrderItem).join(OrderItem, OrderItem.order_id == Order.id)
                                         .where(Order.id == order_id).with_for_update())).first()
            if row is None:
                raise ServiceFault(404, "order not found")
            order, item = row
            if order.state == "PAID":
                payment = (await session.execute(select(Payment).where(Payment.order_id == order_id,
                    Payment.state == "SUCCEEDED"))).scalar_one_or_none()
                if payment is not None and payment.provider == provider and payment.provider_transaction_id == transaction_id and payment.amount_cents == amount:
                    return OrderView(order.id, order.user_id, order.campaign_id, order.idempotency_key, order.state,
                                     item.sku_id, item.quantity, item.unit_price_cents)
                raise ServiceFault(409, "paid order was confirmed by a different payment transaction")
            if order.state != "PENDING_PAYMENT":
                raise ServiceFault(409, "order is not pending payment")
            expected = item.unit_price_cents * item.quantity
            if amount != expected:
                raise ServiceFault(409, "payment amount does not match order total")
            order.state = "PAID"
            order.expiry_claimed_by = None
            order.expiry_lease_until = None
            if self.inventory.redis is not None:
                changed = await session.execute(update(Inventory).where(
                    Inventory.sku_id == item.sku_id, Inventory.reserved >= item.quantity).values(
                    reserved=Inventory.reserved - item.quantity, sold=Inventory.sold + item.quantity,
                    version=Inventory.version + 1))
                if changed.rowcount != 1:
                    raise ServiceFault(409, "durable inventory reservation is inconsistent")
            session.add(Payment(id="payment-" + order_id, order_id=order_id, provider=provider,
                provider_transaction_id=transaction_id, amount_cents=amount, state="SUCCEEDED"))
            self._append_event(session, "stock-confirm-" + order_id, "inventory.events", order_id,
                f"StockConfirm|{order_id}|{item.sku_id}|{item.quantity}")
            request = await session.get(FlashSaleRequest, order.idempotency_key, with_for_update=True)
            if request is not None:
                request.state = "PAID"
                request.message = "payment callback confirmed"
            return OrderView(order.id, order.user_id, order.campaign_id, order.idempotency_key, order.state,
                             item.sku_id, item.quantity, item.unit_price_cents)

    @staticmethod
    def _append_event(session, event_id: str, topic: str, event_key: str, payload: str) -> None:
        session.add(OutboxEvent(event_id=event_id, topic=topic, event_key=event_key,
                                payload=payload.encode(), published=False, attempts=0))

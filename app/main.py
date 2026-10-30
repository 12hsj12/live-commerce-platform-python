from __future__ import annotations

import asyncio
import hmac
import re
import time
import uuid
from contextlib import asynccontextmanager
from urllib.parse import parse_qs

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse, Response
from sqlalchemy import select, update

from .adapters.admission import RedisWaitingRoom
from .adapters.inventory import Stock
from .config import Settings
from .models import AdminAuditLog, Campaign, FlashSaleRequest, Inventory, Order, OrderItem, Product
from .observability import log_http_error
from .messages import FlashSaleMessage
from .runtime import Runtime
from .security import valid_identifier, verify_timestamped_hmac
from .services.commerce import ServiceFault
from .services.request_status import RequestStatus, TERMINAL
from .workers.service import consumer_loop, expiry_loop, notification_loop, outbox_loop, waiting_room_loop


@asynccontextmanager
async def lifespan(app: FastAPI):
    runtime = Runtime()
    await runtime.start()
    app.state.runtime = runtime
    tasks: list[asyncio.Task] = []
    if runtime.settings.process_role in {"all", "worker"}:
        tasks.extend(asyncio.create_task(consumer_loop(runtime, i), name=f"flash-consumer-{i}")
                     for i in range(1 if runtime.queue_mode == "rocketmq" else runtime.settings.consumer_workers))
        tasks.append(asyncio.create_task(outbox_loop(runtime), name="outbox-dispatcher"))
        tasks.append(asyncio.create_task(expiry_loop(runtime), name="order-expiry"))
        if runtime.redis is not None and runtime.settings.waiting_room_enabled and runtime.queue_mode != "local":
            tasks.extend(asyncio.create_task(waiting_room_loop(runtime, i), name=f"waiting-room-{i}")
                         for i in range(runtime.settings.waiting_workers))
        if runtime.kafka_producer is not None:
            tasks.append(asyncio.create_task(notification_loop(runtime), name="notification-fanout"))
    try:
        yield
    finally:
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await runtime.close()


app = FastAPI(title="Live Commerce Flash Sale API", docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)


def _runtime(request: Request) -> Runtime:
    return request.app.state.runtime


async def _form(request: Request) -> dict[str, str]:
    raw = await request.body()
    parsed = parse_qs(raw.decode("utf-8", errors="replace"), keep_blank_values=True)
    return {key: values[0] for key, values in parsed.items() if values}


def _json(status: int, value: dict | list | None = None, *, body: str | None = None) -> JSONResponse:
    if body is None:
        return JSONResponse(status_code=status, content=value if value is not None else {})
    return JSONResponse(status_code=status, content=__import__("json").loads(body))


def _bearer(request: Request) -> str:
    value = request.headers.get("authorization", "")
    if not value.startswith("Bearer ") or len(value) <= 7:
        raise ServiceFault(401, "Bearer token is required")
    return value[7:]


async def _current_user(request: Request):
    runtime = _runtime(request)
    try:
        return await runtime.auth.authenticate(_bearer(request))
    except PermissionError as exc:
        raise ServiceFault(401, str(exc)) from exc


def _admin_credentials(settings: Settings) -> list[tuple[str, str, str]]:
    rows: list[tuple[str, str, str]] = []
    if settings.admin_token:
        rows.append(("local-admin", "admin", settings.admin_token))
    for record in settings.admin_credentials.split(";"):
        fields = record.split("=", 2)
        if len(fields) == 3 and fields[0] and fields[2] and fields[1] in {"viewer", "operator", "admin"}:
            rows.append((fields[0], fields[1], fields[2]))
    return rows


def _authorize_admin(request: Request, required_role: str) -> tuple[int, str | None]:
    settings = _runtime(request).settings
    credentials = _admin_credentials(settings)
    if not credentials:
        return 503, None
    provided = request.headers.get("x-admin-token")
    if provided is None:
        return 401, None
    rank = {"viewer": 1, "operator": 2, "admin": 3}
    for actor, role, expected in credentials:
        if hmac.compare_digest(provided, expected):
            if rank[role] < rank[required_role]:
                return 403, None
            return 0, actor
    return 401, None


def _admin_error(code: int) -> JSONResponse:
    if code == 503:
        return _json(503, {"error": "admin credentials are not configured"})
    if code == 403:
        return _json(403, {"error": "admin role is insufficient"})
    return _json(401, {"error": "invalid admin credentials"})


def _client_ip(request: Request, trusted_proxies: tuple[str, ...]) -> str:
    peer = request.client.host if request.client is not None else ""
    if peer in trusted_proxies:
        forwarded = request.headers.get("x-forwarded-for", "")
        if forwarded:
            return forwarded.split(",", 1)[0].strip()
    return peer


async def _limited_entry(runtime: Runtime, request: Request, user_id: str, campaign_id: str) -> bool:
    """Return True when global/campaign pressure requires the shared waiting-room path."""
    if runtime.redis is None:
        if not await runtime.global_limiter.acquire():
            raise ServiceFault(429, "rate limited")
        return False
    client_ip = _client_ip(request, runtime.settings.trusted_proxy_ips)
    if not client_ip:
        raise ServiceFault(503, "client address is unavailable")
    try:
        if not await runtime.admission.take_redis_bucket(f"flashsale:ip-rate:{client_ip}",
                runtime.settings.ip_rate, runtime.settings.ip_burst):
            raise ServiceFault(429, "client IP rate limited")
        if not await runtime.admission.take_redis_bucket(f"flashsale:user-rate:{user_id}",
                runtime.settings.user_rate, runtime.settings.user_burst):
            raise ServiceFault(429, "user rate limited")
        global_allowed = await runtime.admission.take_redis_bucket("flashsale:{global}:entry_rate",
                runtime.settings.global_rate, runtime.settings.global_burst)
        if not global_allowed:
            return True
        campaign_allowed = await runtime.admission.take_redis_bucket(
            f"flashsale:{{{campaign_id}}}:entry_rate", runtime.settings.campaign_rate,
            runtime.settings.campaign_burst)
        return not campaign_allowed
    except ServiceFault:
        raise
    except Exception as exc:
        raise ServiceFault(503, "distributed admission limiter unavailable") from exc


def _campaign_json(campaign: Campaign) -> dict:
    return {"campaign_id": campaign.id, "sku_id": campaign.sku_id, "state": campaign.state,
            "participant_limit": campaign.participant_limit, "starts_at": campaign.starts_at,
            "ends_at": campaign.ends_at}


@app.middleware("http")
async def count_http(request: Request, call_next):
    started = time.perf_counter()
    runtime = getattr(request.app.state, "runtime", None)
    if runtime is not None:
        runtime.metrics["http_inflight_requests"] += 1
    try:
        response = await call_next(request)
    finally:
        if runtime is not None:
            runtime.metrics["http_inflight_requests"] -= 1
    if runtime is not None:
        runtime.metrics["http_requests"] += 1
        runtime.metrics[f"http_responses_{response.status_code // 100}xx"] += 1
        elapsed_ms = (time.perf_counter() - started) * 1000
        runtime.metrics["http_request_duration_ms_sum"] += elapsed_ms
        runtime.metrics["http_request_duration_ms_count"] += 1
        for bound in (0.5, 1.0, 2.0, 5.0, 10.0, 50.0, 100.0, 500.0):
            if elapsed_ms <= bound:
                runtime.metrics[f"http_request_duration_ms_bucket_{bound:g}"] += 1
        if response.status_code >= 400:
            target = request.url.path
            if request.url.query:
                target += "?" + request.url.query
            log_http_error(request.method, target, response.status_code, int(elapsed_ms * 1000))
    return response


@app.exception_handler(ServiceFault)
async def service_fault_handler(_request: Request, exc: ServiceFault):
    return _json(exc.status_code, {"error": exc.message})


@app.post("/users/register")
async def register_user(request: Request):
    fields = await _form(request)
    runtime = _runtime(request)
    try:
        user = await runtime.auth.register(fields.get("username", ""), fields.get("password", ""))
    except FileExistsError as exc:
        return _json(409, {"error": str(exc)})
    except ValueError as exc:
        return _json(409, {"error": str(exc)})
    except Exception as exc:
        return _json(409, {"error": str(exc)})
    return _json(201, {"user_id": user.id})


@app.post("/users/login")
async def login_user(request: Request):
    fields = await _form(request)
    runtime = _runtime(request)
    try:
        token, user, expires_at = await runtime.auth.login(fields.get("username", ""), fields.get("password", ""))
    except Exception:
        return _json(401, {"error": "invalid credentials"})
    return _json(200, {"token": token, "user_id": user.id, "expires_at": expires_at})


@app.post("/users/logout")
async def logout_user(request: Request):
    try:
        await _runtime(request).auth.logout(_bearer(request))
    except ServiceFault as exc:
        return _json(exc.status_code, {"error": exc.message})
    except LookupError:
        pass
    except Exception as exc:
        return _json(401, {"error": str(exc)})
    return Response(status_code=204)


@app.get("/health")
async def health(request: Request):
    _runtime(request).metrics["http_health_requests"] += 1
    return _json(200, {"status": "ok"})


@app.get("/ready")
async def ready(request: Request):
    runtime = _runtime(request)
    runtime.metrics["http_health_requests"] += 1
    unavailable = await runtime.check_ready()
    if unavailable:
        return _json(503, {"status": "not_ready", "unavailable": unavailable})
    return _json(200, {"status": "ready"})


@app.get("/products/sku-1")
async def get_product(request: Request):
    runtime = _runtime(request)
    try:
        product = await runtime.products.get("sku-1")
    except LookupError:
        return _json(404, {"error": "product not found"})
    except Exception:
        return _json(404, {"error": "product not found"})
    runtime.metrics["product_query_requests"] += 1
    return _json(200, {"id": product.id, "name": product.name, "price_cents": product.price_cents,
                       "on_sale": product.on_sale})


@app.get("/stock/sku-1")
async def get_stock(request: Request):
    runtime = _runtime(request)
    if runtime.settings.mysql_mode:
        async with runtime.database.sessions() as session:
            stock = await session.get(Inventory, "sku-1")
        value = None if stock is None else Stock(stock.available, stock.reserved, stock.sold)
    else:
        value = await runtime.inventory.query("sku-1")
    if value is None:
        return _json(404, {"error": "sku not found"})
    return _json(200, {"available": value.available, "reserved": value.reserved, "sold": value.sold})


@app.post("/flash-sale/orders")
async def submit_flash_sale(request: Request):
    runtime = _runtime(request)
    current = await _current_user(request)
    fields = await _form(request)
    campaign_id = fields.get("promotion_id") or "promo-1"
    sku_id = fields.get("sku_id") or "sku-1"
    request_id = fields.get("idempotency_key", "")
    try:
        quantity = int(fields.get("quantity", "1"))
    except ValueError:
        return _json(400, {"error": "invalid quantity"})
    if not request_id:
        return _json(400, {"error": "idempotency_key is required"})
    if (quantity != 1 or len(campaign_id.encode()) > 64 or len(sku_id.encode()) > 64 or
            len(request_id.encode()) > 128 or not valid_identifier(campaign_id) or not valid_identifier(sku_id)):
        return _json(400, {"error": "invalid flash-sale request fields"})

    if runtime.redis is not None:
        client_ip = _client_ip(request, runtime.settings.trusted_proxy_ips)
        if not client_ip:
            return _json(503, {"error": "client address is unavailable"})
        try:
            ip_allowed = await runtime.admission.take_redis_bucket(f"flashsale:ip-rate:{client_ip}",
                    runtime.settings.ip_rate, runtime.settings.ip_burst)
        except Exception:
            return _json(503, {"error": "distributed IP limiter unavailable"})
        if not ip_allowed:
            return _json(429, {"error": "client IP rate limited"})
        try:
            user_allowed = await runtime.admission.take_redis_bucket(f"flashsale:user-rate:{current.id}",
                    runtime.settings.user_rate, runtime.settings.user_burst)
        except Exception:
            return _json(503, {"error": "distributed user limiter unavailable"})
        if not user_allowed:
            return _json(429, {"error": "user rate limited"})

    try:
        previous = await runtime.ingress_statuses.get(request_id)
    except Exception:
        return _json(503, {"error": "request status unavailable"})
    if previous is not None:
        if previous.user_id != current.id:
            return _json(403, {"error": "idempotency key belongs to another user"})
        if previous.state == "WAITING":
            if previous.promotion_id and previous.promotion_id != campaign_id:
                return _json(409, {"error": "waiting-room ticket belongs to another campaign"})
            if runtime.redis is None:
                return _json(503, {"error": "waiting room requires Redis"})
            _, position = await _find_waiting(runtime, request_id)
            return _json(202, {"request_id": previous.request_id, "state": "WAITING", "queue_position": position})
        if previous.state != "SUBMITTING":
            return _json(200, {"request_id": previous.request_id, "order_id": previous.order_id,
                               "state": previous.state, "message": previous.message})
    elif runtime.redis is None and runtime.settings.mysql_mode:
        # The durable read failed only when the connection fails; an absent row is normal.
        pass

    try:
        waiting_needed = False
        if runtime.redis is not None:
            global_allowed = await runtime.admission.take_redis_bucket("flashsale:{global}:entry_rate",
                runtime.settings.global_rate, runtime.settings.global_burst)
            if not global_allowed:
                waiting_needed = True
            else:
                campaign_allowed = await runtime.admission.take_redis_bucket(
                    f"flashsale:{{{campaign_id}}}:entry_rate", runtime.settings.campaign_rate,
                    runtime.settings.campaign_burst)
                waiting_needed = not campaign_allowed
        elif not await runtime.global_limiter.acquire():
            return _json(429, {"error": "rate limited"})
        if waiting_needed:
            can_wait = (runtime.redis is not None and runtime.settings.waiting_room_enabled and
                        runtime.queue_mode != "local")
            if can_wait:
                waiting = RequestStatus(request_id, current.id, "", "WAITING",
                    "request is waiting for admission capacity", campaign_id)
                inserted = await runtime.ingress_statuses.put_if_absent(waiting)
                if not inserted:
                    raced = await runtime.ingress_statuses.get(request_id)
                    if raced is None:
                        return _json(503, {"error": "request status unavailable"})
                    if raced.user_id != current.id:
                        return _json(403, {"error": "idempotency key belongs to another user"})
                    if raced.state == "SUBMITTING":
                        await runtime.ingress_statuses.transition(request_id, "SUBMITTING", "WAITING")
                    elif raced.state != "WAITING":
                        return _json(200, {"request_id": raced.request_id, "order_id": raced.order_id,
                                           "state": raced.state})
                # C++ constructs the waiting-room message without order_id, but its
                # FOQ1 validator requires order_id. Preserve the observed 503 and
                # REJECTED status instead of silently changing the established API.
                try:
                    waiting_message = FlashSaleMessage(campaign_id, "", request_id, current.id, sku_id, quantity)
                    room = RedisWaitingRoom(runtime.redis, campaign_id, runtime.settings.waiting_capacity)
                    position = await room.enqueue(waiting_message)
                except Exception as exc:
                    await runtime.ingress_statuses.put(RequestStatus(request_id, current.id, "", "REJECTED",
                        "invalid flash-sale order message", campaign_id))
                    return _json(503, {"error": "invalid flash-sale order message"})
                return _json(202, {"request_id": request_id, "state": "WAITING", "queue_position": position})
            return _json(429, {"error": "admission capacity exceeded"})
        result = await runtime.flash_sale.submit(campaign_id, sku_id, current.id, request_id, quantity)
    except ServiceFault as exc:
        return _json(exc.status_code, {"error": exc.message})
    except Exception as exc:
        return _json(503, {"error": str(exc)})
    code = result["code"]
    state = "QUEUED" if code == "QUEUED" else ("DUPLICATE" if code == "DUPLICATE" else "REJECTED")
    status_code = 202 if code == "QUEUED" else (200 if code == "DUPLICATE" else 409)
    return _json(status_code, {"request_id": result.get("request_id", ""), "order_id": result.get("order_id", ""),
                               "state": state, "message": result.get("message", "")})


async def _find_waiting(runtime: Runtime, request_id: str) -> tuple[str | None, int]:
    async with runtime.database.sessions() as session:
        campaign_ids = list((await session.execute(select(Campaign.id))).scalars())
    for campaign_id in campaign_ids:
        position = await RedisWaitingRoom(runtime.redis, campaign_id, runtime.settings.waiting_capacity).position(request_id)
        if position:
            return campaign_id, position
    return None, 0


@app.get("/flash-sale/requests")
async def get_flash_sale_request(request: Request):
    runtime = _runtime(request)
    current = await _current_user(request)
    request_id = request.query_params.get("request_id", "")
    if not request_id:
        return _json(503, {"error": "invalid request status lookup"})
    try:
        result = await runtime.statuses.get(request_id)
    except Exception as exc:
        return _json(503, {"error": str(exc)})
    if result is None:
        return _json(404, {"error": "flash-sale request not found"})
    if result.user_id != current.id:
        return _json(403, {"error": "request belongs to another user"})
    payload = {"request_id": result.request_id, "order_id": result.order_id,
               "state": result.state, "message": result.message}
    if result.state == "WAITING":
        if runtime.redis is None:
            return _json(503, {"error": "waiting room requires Redis"})
        _campaign_id, position = await _find_waiting(runtime, result.request_id)
        payload["queue_position"] = position
    return _json(200, payload)


@app.get("/metrics")
async def metrics(request: Request):
    runtime = _runtime(request)
    counter_names = ("http_requests", "http_responses_2xx", "http_responses_3xx",
        "http_responses_4xx", "http_responses_5xx", "http_health_requests",
        "product_query_requests", "reconcile_success", "reconcile_failure",
        "reconcile_mismatch_total", "reconcile_repaired_total", "reconcile_blocked_total",
        "redis_dlq_replayed", "rocketmq_dlq_replayed", "kafka_dlq_replayed")
    lines: list[str] = []
    for name in counter_names:
        rendered = name if name.endswith("_total") else f"{name}_total"
        lines.extend((f"# TYPE {rendered} counter", f"{rendered} {runtime.metrics[name]}"))
    histogram = "http_request_duration_ms"
    lines.append(f"# TYPE {histogram} histogram")
    for bound in (0.5, 1.0, 2.0, 5.0, 10.0, 50.0, 100.0, 500.0):
        lines.append(f'{histogram}_bucket{{le="{bound:g}"}} '
                     f"{runtime.metrics[f'{histogram}_bucket_{bound:g}']}")
    lines.append(f'{histogram}_bucket{{le="+Inf"}} {runtime.metrics[f"{histogram}_count"]}')
    lines.append(f"{histogram}_sum {runtime.metrics[f'{histogram}_sum']}")
    lines.append(f"{histogram}_count {runtime.metrics[f'{histogram}_count']}")
    try:
        pending = runtime.queue.pending()
        if asyncio.iscoroutine(pending):
            pending = await pending
    except Exception:
        pending = None
    queue_depth_known = runtime.queue_mode != "rocketmq" and pending is not None
    gauges = {
        # ASGI workers don't expose the C++ TCP connection and request queue internals.
        # -1 plus *_known=0 means unknown; http_inflight_requests is the app-layer equivalent.
        "http_active_connections": -1,
        "http_active_connections_known": 0,
        "http_worker_queue_depth": -1,
        "http_worker_queue_depth_known": 0,
        "http_inflight_requests": runtime.metrics["http_inflight_requests"],
        "flash_sale_queue_depth": pending if queue_depth_known else -1,
        "flash_sale_queue_depth_known": 1 if queue_depth_known else 0,
        "flash_sale_consumer_processed": runtime.metrics["flash_sale_consumer_processed"],
        "flash_sale_consumer_rejected": runtime.metrics["flash_sale_consumer_rejected"],
        "flash_sale_consumer_dead_lettered": runtime.metrics["flash_sale_consumer_dead_lettered"],
    }
    for name, value in gauges.items():
        lines.extend((f"# TYPE {name} gauge", f"{name} {value}"))
    return PlainTextResponse("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")


@app.get("/debug/stats")
async def debug_stats(request: Request):
    runtime = _runtime(request)
    if runtime.queue_mode == "rocketmq":
        pending = None
    else:
        pending = runtime.queue.pending()
        if asyncio.iscoroutine(pending):
            pending = await pending
    return _json(200, {"http_requests": runtime.metrics["http_requests"],
        "http_active_connections": None,
        "http_active_connections_known": False,
        "http_inflight_requests": runtime.metrics["http_inflight_requests"],
        "http_worker_queue_depth": None,
        "http_worker_queue_depth_known": False,
        "flash_sale_queue_depth": pending,
        "flash_sale_consumer_processed": runtime.metrics["flash_sale_consumer_processed"],
        "flash_sale_consumer_rejected": runtime.metrics["flash_sale_consumer_rejected"],
        "flash_sale_consumer_dead_lettered": runtime.metrics["flash_sale_consumer_dead_lettered"]})


@app.get("/campaigns")
async def get_campaign(request: Request):
    campaign_id = request.query_params.get("campaign_id", "")
    if not campaign_id:
        return _json(400, {"error": "campaign_id is required"})
    try:
        campaign = await _runtime(request).promotions.get(campaign_id)
    except Exception:
        return _json(503, {"error": "campaign query failed"})
    if campaign is None:
        return _json(404, {"error": "promotion not found"})
    return _json(200, _campaign_json(campaign))


@app.get("/promotions/promo-1")
async def get_default_promotion(request: Request):
    campaign = await _runtime(request).promotions.get("promo-1")
    if campaign is None:
        return _json(404, {"error": "promotion not found"})
    state = campaign.state if campaign.state in {"RUNNING", "PAUSED", "FINISHED"} else "PREHEATING"
    return _json(200, {"promotion_id": campaign.id, "state": state, "limit": campaign.participant_limit})


@app.get("/orders")
async def get_order(request: Request):
    current = await _current_user(request)
    try:
        order = await _runtime(request).orders.get(request.query_params.get("order_id", ""))
    except Exception:
        return _json(404, {"error": "order not found"})
    if order is None:
        return _json(404, {"error": "order not found"})
    if order.user_id != current.id:
        return _json(403, {"error": "order belongs to another user"})
    return _json(200, {"order_id": order.id, "state": order.state})


@app.post("/agent/ask")
async def ask_agent(request: Request):
    current = await _current_user(request)
    try:
        body = await request.json()
    except Exception:
        return _json(400, {"error": "a JSON object with a message is required"})
    if not isinstance(body, dict) or not isinstance(body.get("message"), str):
        return _json(400, {"error": "a JSON object with a message is required"})
    message = body["message"].strip()
    if not message or len(message) > 2000:
        return _json(400, {"error": "message must contain 1 to 2000 characters"})
    supplied_id = request.headers.get("x-request-id", "")
    request_id = supplied_id if re.fullmatch(r"[A-Za-z0-9._:-]{1,96}", supplied_id) else uuid.uuid4().hex
    runtime = _runtime(request)
    try:
        result = await runtime.agent.ask(current.id, request_id, message)
    except TimeoutError:
        return _json(504, {"request_id": request_id, "error": "assistant timed out"})
    except Exception:
        return _json(502, {"request_id": request_id, "error": "assistant provider failed"})
    return _json(200, {"request_id": request_id, "answer": result.answer, "tool_trace": result.tool_trace})


@app.post("/orders/pay")
async def pay_order(request: Request):
    current = await _current_user(request)
    fields = await _form(request)
    runtime = _runtime(request)
    order = await runtime.orders.get(fields.get("order_id", ""))
    if order is None:
        return _json(404, {"error": "order not found"})
    if order.user_id != current.id:
        return _json(403, {"error": "order belongs to another user"})
    try:
        transitioned = await runtime.orders.transition(order.id, "PAID")
    except ServiceFault as exc:
        return _json(409, {"error": exc.message})
    except Exception as exc:
        return _json(409, {"error": str(exc)})
    try:
        await runtime.statuses.put(RequestStatus(order.idempotency_key, order.user_id, order.id, "PAID", "mock payment succeeded"))
    except Exception:
        return _json(503, {"error": "payment committed; request status cache update pending"})
    try:
        await runtime.inventory.confirm(order.sku_id, order.id)
    except Exception:
        return _json(503, {"error": "payment recorded; inventory confirmation pending"})
    return _json(200, {"order_id": transitioned.id, "state": "PAID", "payment": "MOCK_SUCCEEDED"})


@app.post("/payments/callback/mock")
async def payment_callback(request: Request):
    runtime = _runtime(request)
    if not runtime.settings.mysql_mode:
        return _json(503, {"error": "durable payment callbacks require MySQL"})
    secret = runtime.settings.payment_callback_secret
    if not secret:
        return _json(503, {"error": "payment callback secret is not configured"})
    raw = await request.body()
    timestamp_text = request.headers.get("x-payment-timestamp")
    signature = request.headers.get("x-payment-signature")
    try:
        timestamp = int(timestamp_text) if timestamp_text is not None else None
    except ValueError:
        timestamp = None
    now = int(time.time())
    if (timestamp is None or signature is None or timestamp < now - 300 or timestamp > now + 300 or
            not verify_timestamped_hmac(secret, timestamp_text or "", raw, signature)):
        return _json(401, {"error": "invalid or expired payment callback signature"})
    fields = parse_qs(raw.decode("utf-8", errors="replace"), keep_blank_values=True)
    field = lambda key: fields.get(key, [""])[0]
    try:
        amount = int(field("amount_cents"))
    except ValueError:
        amount = -1
    order_id, transaction_id = field("order_id"), field("transaction_id")
    if not order_id or not transaction_id or len(transaction_id.encode()) > 128 or amount < 0:
        return _json(400, {"error": "invalid callback payload"})
    try:
        paid = await runtime.orders.confirm_callback(order_id, "mock", transaction_id, amount)
    except ServiceFault as exc:
        return _json(exc.status_code, {"error": exc.message})
    except Exception as exc:
        return _json(409, {"error": str(exc)})
    try:
        await runtime.statuses.put(RequestStatus(paid.idempotency_key, paid.user_id, paid.id,
                                                 "PAID", "payment callback confirmed"))
    except Exception:
        return _json(503, {"error": "payment committed; request status cache update pending"})
    try:
        await runtime.inventory.confirm(paid.sku_id, paid.id)
    except Exception:
        return _json(503, {"error": "payment committed; inventory projection will reconcile"})
    return _json(200, {"order_id": paid.id, "state": "PAID"})


@app.post("/orders/cancel")
async def cancel_order(request: Request):
    current = await _current_user(request)
    fields = await _form(request)
    runtime = _runtime(request)
    order = await runtime.orders.get(fields.get("order_id", ""))
    if order is None:
        return _json(404, {"error": "order not found"})
    if order.user_id != current.id:
        return _json(403, {"error": "order belongs to another user"})
    try:
        cancelled = await runtime.orders.transition(order.id, "CANCELLED")
    except ServiceFault as exc:
        return _json(409, {"error": exc.message})
    except Exception as exc:
        return _json(409, {"error": str(exc)})
    try:
        await runtime.statuses.put(RequestStatus(order.idempotency_key, order.user_id, order.id,
                                                 "CANCELLED", "order cancelled"))
    except Exception:
        return _json(503, {"error": "order cancelled; request status cache update pending"})
    try:
        await runtime.inventory.release(order.sku_id, order.id)
    except Exception:
        return _json(503, {"error": "order cancelled; inventory release pending"})
    return _json(200, {"order_id": cancelled.id, "state": "CANCELLED"})


@app.get("/admin/campaigns")
async def admin_campaigns(request: Request):
    code, _actor = _authorize_admin(request, "viewer")
    if code:
        return _admin_error(code)
    campaigns = await _runtime(request).promotions.list()
    return _json(200, {"campaigns": [_campaign_json(item) for item in campaigns]})


@app.get("/admin/audit")
async def admin_audit(request: Request):
    code, _actor = _authorize_admin(request, "admin")
    if code:
        return _admin_error(code)
    try:
        limit = int(request.query_params.get("limit", "100"))
    except ValueError:
        return _json(400, {"error": "invalid limit"})
    if limit <= 0:
        return _json(400, {"error": "limit must be positive"})
    try:
        records = await _runtime(request).promotions.list_audit(min(limit, 1000))
    except ServiceFault as exc:
        return _json(exc.status_code, {"error": exc.message})
    return _json(200, {"records": [{"id": row.id, "actor": row.actor, "action": row.action,
        "resource_type": row.resource_type, "resource_id": row.resource_id,
        "previous_value": row.previous_value, "new_value": row.new_value,
        "created_at": row.created_at.isoformat(sep=" ")} for row in records]})


@app.post("/admin/campaigns")
async def create_campaign(request: Request):
    code, actor = _authorize_admin(request, "operator")
    if code:
        return _admin_error(code)
    fields = await _form(request)
    try:
        limit = int(fields.get("participant_limit", ""))
        start_text, end_text = fields.get("starts_at", ""), fields.get("ends_at", "")
        if bool(start_text) != bool(end_text):
            raise ValueError
        starts_at = int(start_text) if start_text else 0
        ends_at = int(end_text) if end_text else 0
        if start_text and (starts_at <= 0 or ends_at <= starts_at):
            raise ValueError
    except ValueError:
        return _json(400, {"error": "invalid campaign fields or scheduled window; starts_at and ends_at must be Unix seconds"})
    try:
        await _runtime(request).promotions.create(fields.get("campaign_id", ""), fields.get("sku_id", ""),
            limit, starts_at, ends_at, actor)
    except ServiceFault as exc:
        return _json(exc.status_code, {"error": exc.message})
    except Exception as exc:
        return _json(503, {"error": str(exc)})
    return _json(201, {"campaign_id": fields.get("campaign_id", ""), "state": "CREATED"})


@app.patch("/admin/campaigns")
async def update_campaign(request: Request):
    code, actor = _authorize_admin(request, "operator")
    if code:
        return _admin_error(code)
    fields = await _form(request)
    try:
        limit = int(fields.get("participant_limit", ""))
        start_text, end_text = fields.get("starts_at", ""), fields.get("ends_at", "")
        if bool(start_text) != bool(end_text):
            raise ValueError
        starts_at = int(start_text) if start_text else 0
        ends_at = int(end_text) if end_text else 0
        if start_text and (starts_at <= 0 or ends_at <= starts_at):
            raise ValueError
    except ValueError:
        return _json(400, {"error": "invalid campaign fields or scheduled window; starts_at and ends_at must be Unix seconds"})
    try:
        await _runtime(request).promotions.update(fields.get("campaign_id", ""), fields.get("sku_id", ""),
            limit, starts_at, ends_at, actor)
    except ServiceFault as exc:
        return _json(exc.status_code, {"error": exc.message})
    except Exception as exc:
        return _json(409, {"error": str(exc)})
    return _json(200, {"campaign_id": fields.get("campaign_id", ""), "state": "CREATED"})


async def _admin_campaign_action(request: Request, action: str):
    code, actor = _authorize_admin(request, "operator")
    if code:
        return _admin_error(code)
    fields = await _form(request)
    campaign_id = fields.get("campaign_id") or fields.get("promotion_id", "")
    if not campaign_id:
        return _json(400, {"error": "promotion_id is required"})
    try:
        campaign = await _runtime(request).promotions.transition(campaign_id, action, actor)
    except ServiceFault as exc:
        return _json(exc.status_code, {"error": exc.message})
    state = "RUNNING" if campaign is not None and campaign.state == "RUNNING" else (
        "PAUSED" if campaign is not None and campaign.state == "PAUSED" else "FINISHED")
    return _json(200, {"promotion_id": campaign_id, "state": state})


def _build_campaign_action(action: str):
    async def handler(request: Request):
        return await _admin_campaign_action(request, action)
    return handler


for _action in ("preheat", "start", "pause", "resume", "finish"):
    app.add_api_route(f"/admin/campaigns/{_action}", _build_campaign_action(_action), methods=["POST"])


@app.get("/admin/dlq")
async def inspect_dlq(request: Request):
    code, _actor = _authorize_admin(request, "admin")
    if code:
        return _admin_error(code)
    runtime = _runtime(request)
    if runtime.queue_mode == "redis":
        try:
            limit = int(request.query_params.get("limit", "100"))
        except ValueError:
            return _json(400, {"error": "invalid limit"})
        if limit <= 0:
            return _json(400, {"error": "limit must be positive"})
        try:
            payloads = await runtime.queue.inspect_dlq(min(limit, 1000))
        except Exception as exc:
            return _json(503, {"error": str(exc)})
        messages = []
        for payload in payloads:
            try:
                decoded = FlashSaleMessage.decode(payload)
                messages.append({"valid": True, "order_id": decoded.order_id,
                    "request_id": decoded.idempotency_key, "user_id": decoded.user_id,
                    "sku_id": decoded.sku_id, "quantity": decoded.quantity})
            except Exception:
                messages.append({"valid": False, "payload": payload.decode(errors="replace")})
        return _json(200, {"queue": "redis", "messages": messages})
    if runtime.queue_mode == "rocketmq":
        try:
            limit = int(request.query_params.get("limit", "100"))
        except ValueError:
            return _json(400, {"error": "invalid limit"})
        if limit <= 0:
            return _json(400, {"error": "limit must be positive"})
        try:
            records = await runtime.queue.inspect_dlq(min(limit, 1000))
        except Exception as exc:
            return _json(503, {"error": str(exc)})
        messages = []
        for record in records:
            try:
                decoded = FlashSaleMessage.decode(record.payload)
                messages.append({"valid": True, "message_id": record.message_id,
                    "order_id": decoded.order_id, "request_id": decoded.idempotency_key,
                    "user_id": decoded.user_id, "sku_id": decoded.sku_id,
                    "quantity": decoded.quantity})
            except Exception:
                messages.append({"valid": False, "message_id": record.message_id,
                                 "payload_size": len(record.payload)})
        return _json(200, {"queue": "rocketmq", "messages": messages})
    if runtime.kafka_dlq is not None:
        topic = f"{runtime.settings.kafka_flash_sale_topic}.DLQ"
        try:
            limit = int(request.query_params.get("limit", "100"))
        except ValueError:
            return _json(400, {"error": "invalid limit"})
        if limit <= 0:
            return _json(400, {"error": "limit must be positive"})
        try:
            events = await runtime.kafka_dlq.peek(topic, min(limit, 1000))
        except Exception as exc:
            return _json(503, {"error": str(exc)})
        messages = []
        for event in events:
            try:
                decoded = FlashSaleMessage.decode(event.payload)
                messages.append({"valid": True, "event_id": event.event_id,
                    "order_id": decoded.order_id, "request_id": decoded.idempotency_key,
                    "user_id": decoded.user_id, "sku_id": decoded.sku_id,
                    "quantity": decoded.quantity})
            except Exception:
                messages.append({"valid": False, "event_id": event.event_id,
                    "payload": event.payload.decode(errors="replace")})
        return _json(200, {"queue": "kafka", "topic": topic, "messages": messages})
    return _json(503, {"error": "DLQ inspection is not available for the configured transport"})


@app.post("/admin/dlq/replay")
async def replay_dlq(request: Request):
    code, _actor = _authorize_admin(request, "admin")
    if code:
        return _admin_error(code)
    runtime = _runtime(request)
    fields = await _form(request)
    try:
        max_messages = int(fields.get("max_messages", "10"))
    except ValueError:
        return _json(400, {"error": "invalid max_messages"})
    max_messages = max(1, min(max_messages, 1000))
    if runtime.queue_mode == "redis":
        try:
            replayed = await runtime.queue.replay_dlq(runtime.queue, max_messages)
        except Exception as exc:
            return _json(409, {"replayed": 0, "error": str(exc)})
        runtime.metrics["redis_dlq_replayed"] += replayed
        return _json(200, {"replayed": replayed})
    if runtime.queue_mode == "rocketmq":
        try:
            replayed = await runtime.queue.replay_dlq(max_messages)
        except Exception as exc:
            return _json(503, {"replayed": 0, "error": str(exc)})
        runtime.metrics["rocketmq_dlq_replayed"] += replayed
        return _json(200, {"replayed": replayed})
    if runtime.kafka_dlq is None:
        return _json(503, {"error": "Kafka is not configured"})
    dlq_topic = fields.get("topic") or f"{runtime.settings.kafka_flash_sale_topic}.DLQ"
    target_topic = fields.get("target_topic") or runtime.settings.kafka_flash_sale_topic
    try:
        replayed = await runtime.kafka_dlq.replay(dlq_topic, target_topic, max_messages)
    except Exception as exc:
        return _json(503, {"error": str(exc)})
    runtime.metrics["kafka_dlq_replayed"] += replayed
    return _json(200, {"replayed": replayed})


@app.post("/admin/reconciliation/run")
@app.get("/admin/reconcile")
async def reconcile_inventory(request: Request):
    code, actor = _authorize_admin(request, "admin")
    if code:
        return _admin_error(code)
    runtime = _runtime(request)
    requested = request.query_params.get("skus", "sku-1")
    skus = [item.strip() for item in requested.split(",") if item.strip()]
    if runtime.settings.mysql_mode and runtime.inventory.redis is not None:
        checked = mismatches = repaired = blocked = 0
        try:
            for sku_id in skus:
                checked += 1
                async with runtime.database.sessions() as session:
                    durable = await session.get(Inventory, sku_id)
                cached = await runtime.inventory.query(sku_id)
                if durable is None or cached is None:
                    raise LookupError("sku not found")
                if (durable.available, durable.reserved, durable.sold) == (cached.available, cached.reserved, cached.sold):
                    continue
                mismatches += 1
                lua = """
                local reserved=tonumber(redis.call('HGET',KEYS[1],'reserved')) or -1;
                if reserved~=tonumber(ARGV[2]) then return 0 end;
                redis.call('HSET',KEYS[1],'available',ARGV[1],'reserved',ARGV[2],'sold',ARGV[3],
                  'total',tonumber(ARGV[1])+tonumber(ARGV[2])+tonumber(ARGV[3])); return 1
                """
                stock_key, _ = runtime.inventory._keys(sku_id)
                result = await runtime.redis.eval(lua, 1, stock_key, durable.available, durable.reserved, durable.sold)
                if int(result) == 1:
                    repaired += 1
                else:
                    blocked += 1
        except Exception as exc:
            runtime.metrics["reconcile_failure"] += 1
            return _json(409, {"status": "failed", "error": str(exc)})
        runtime.metrics["reconcile_success"] += 1
        runtime.metrics["reconcile_mismatch_total"] += mismatches
        runtime.metrics["reconcile_repaired_total"] += repaired
        runtime.metrics["reconcile_blocked_total"] += blocked
        details = f"checked={checked},mismatches={mismatches},repaired={repaired},blocked={blocked}"
        async with runtime.database.sessions.begin() as session:
            session.add(AdminAuditLog(actor=actor or "", action="INVENTORY_RECONCILIATION", resource_type="inventory",
                resource_id=requested, previous_value="", new_value=details))
        return _json(200, {"status": "ok", "checked": checked, "mismatches": mismatches,
                           "repaired": repaired, "blocked": blocked})
    async with runtime.database.sessions() as session:
        stocks = list((await session.execute(select(Inventory))).scalars())
    valid = all(row.available >= 0 and row.reserved >= 0 and row.sold >= 0 for row in stocks)
    runtime.metrics["reconcile_success" if valid else "reconcile_failure"] += 1
    if runtime.settings.mysql_mode:
        async with runtime.database.sessions.begin() as session:
            session.add(AdminAuditLog(actor=actor or "", action="INVENTORY_VALIDATION" if valid else "INVENTORY_VALIDATION_FAILED",
                resource_type="inventory", resource_id="all", previous_value="",
                new_value="invariants=valid" if valid else "inventory invariant failed"))
    if not valid:
        return _json(500, {"status": "failed", "error": "inventory invariant failed"})
    return _json(200, {"status": "ok"})

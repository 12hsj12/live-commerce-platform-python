from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class ToolCall:
    id: str
    name: str
    arguments: dict


@dataclass(frozen=True)
class Completion:
    content: str | None
    tool_calls: tuple[ToolCall, ...] = ()


class ProviderError(RuntimeError):
    pass


class ModelProvider(Protocol):
    async def complete(self, messages: list[dict], tools: list[dict]) -> Completion: ...


class FakeProvider:
    """Deterministic offline provider for local demos and tests; it is not an LLM."""

    async def complete(self, messages: list[dict], tools: list[dict]) -> Completion:
        last = messages[-1]
        if last.get("role") == "tool":
            try:
                result = json.loads(last.get("content", "{}"))
            except json.JSONDecodeError:
                result = {"error": "query_failed"}
            return Completion("查询结果：" + json.dumps(result, ensure_ascii=False, sort_keys=True))

        message = str(last.get("content", ""))
        if re.search(r"支付|付款|取消订单|下单|抢购|扣减库存|修改活动|改活动|执行代码|绕过权限", message):
            return Completion("我只能查询商品、活动、库存和当前账户自己的订单，不能执行交易或管理操作。")

        suffix = re.search(
            r"(?:sku[-_][A-Za-z0-9._-]+|promo[-_][A-Za-z0-9._-]+|order[-_][A-Za-z0-9._-]+|flash[-_][A-Fa-f0-9._-]+)",
            message,
        )
        identifier = suffix.group(0) if suffix else None
        if "订单" in message:
            if identifier and (identifier.startswith("order") or identifier.startswith("flash")):
                return Completion(None, (ToolCall("fake-1", "get_my_order", {"order_id": identifier}),))
            return Completion(None, (ToolCall("fake-1", "list_my_orders", {"limit": 5}),))
        if "库存" in message or "有货" in message:
            sku_id = identifier if identifier and identifier.startswith("sku") else "sku-1"
            return Completion(None, (ToolCall("fake-1", "query_stock", {"sku_id": sku_id}),))
        if "活动" in message or "促销" in message:
            campaign_id = identifier if identifier and identifier.startswith("promo") else "promo-1"
            return Completion(None, (ToolCall("fake-1", "query_campaign", {"campaign_id": campaign_id}),))
        if "商品" in message or "价格" in message:
            sku_id = identifier if identifier and identifier.startswith("sku") else "sku-1"
            return Completion(None, (ToolCall("fake-1", "query_product", {"sku_id": sku_id}),))
        return Completion("我可以查询商品、活动、库存或你自己的订单。请补充要查询的对象。")


class OpenAICompatibleProvider:
    """OpenAI Chat Completions compatible provider; key is kept in process memory only."""

    def __init__(self, base_url: str, model: str, api_key: str, timeout_seconds: int):
        if not api_key:
            raise ValueError("ECOMMERCE_AGENT_API_KEY is required for the configured provider")
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds

    async def complete(self, messages: list[dict], tools: list[dict]) -> Completion:
        import httpx

        payload = {"model": self.model, "messages": messages, "tools": tools, "tool_choice": "auto"}
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await client.post(
                    self.base_url + "/chat/completions",
                    headers={"Authorization": "Bearer " + self.api_key},
                    json=payload,
                )
            response.raise_for_status()
            data = response.json()
            choice = data["choices"][0]["message"]
            calls = []
            for call in choice.get("tool_calls", []):
                function = call["function"]
                arguments = json.loads(function.get("arguments", "{}"))
                if not isinstance(arguments, dict):
                    raise ValueError("tool arguments must be a JSON object")
                calls.append(ToolCall(str(call.get("id", "")), function["name"], arguments))
            content = choice.get("content")
            return Completion(content if isinstance(content, str) else None, tuple(calls))
        except Exception as exc:
            # Never include request headers, provider response bodies, or prompts in errors/logs.
            raise ProviderError(type(exc).__name__) from None


def build_provider(settings) -> ModelProvider:
    if settings.agent_provider == "fake":
        return FakeProvider()
    if settings.agent_provider in {"openai", "openai-compatible"}:
        return OpenAICompatibleProvider(settings.agent_base_url, settings.agent_model,
                                        settings.agent_api_key or "", settings.agent_timeout_seconds)
    raise ValueError("ECOMMERCE_AGENT_PROVIDER must be fake or openai-compatible")

# 只读电商 Agent

## 调用链路

```text
已登录用户
  → POST /agent/ask（JSON message、可选 X-Request-Id）
  → FastAPI 从 session 取得当前 user_id
  → AgentService 设置只读工具白名单并调用模型 provider
  → AgentService 校验工具名、参数、重复调用、轮数和超时
  → 既有 ProductService / PromotionService / InventoryGateway / OrderService
  → 返回 answer、request_id 和不含工具参数的 tool_trace
```

可用工具只有 `query_product`、`query_stock`、`query_campaign`、`get_my_order`、`list_my_orders`。所有参数都是 JSON schema 定义的结构化对象，服务端再次校验字段集合、标识符和范围。订单读取调用 `OrderService.get_for_user(order_id, authenticated_user_id)`，数据库查询同时约束两个字段；订单列表也始终以当前用户过滤。未知工具、无效参数、重复调用、读取错误和超时都会返回受限结果或通用错误，不会执行模型生成的 SQL 或代码。

Agent 没有下单、库存预留、支付、取消订单或活动修改工具。系统提示要求模型忽略提示注入，但安全边界依赖服务端工具白名单、参数检查和订单归属查询；提示词本身不视为安全边界。

## Provider 配置

默认 `ECOMMERCE_AGENT_PROVIDER=fake`，用于离线演示和测试。它根据少量中文关键词调用同一组查询服务，不是 LLM，不会访问外部网络。

可选配置 `ECOMMERCE_AGENT_PROVIDER=openai-compatible`，并通过进程环境变量提供：

```text
ECOMMERCE_AGENT_PROVIDER=openai-compatible
ECOMMERCE_AGENT_BASE_URL=https://api.openai.com/v1
ECOMMERCE_AGENT_MODEL=gpt-4o-mini
ECOMMERCE_AGENT_API_KEY=<secret-from-secret-manager>
ECOMMERCE_AGENT_TIMEOUT_SECONDS=15
ECOMMERCE_AGENT_MAX_TOOL_ROUNDS=4
```

密钥也可从标准 `OPENAI_API_KEY` 环境变量读取。密钥不应写进仓库、命令历史或客户端请求。OpenAI 兼容 provider 会将 key 只放在服务进程的授权请求头中；HTTP 响应体、提示、headers 和 key 不进入日志。外部模型 provider 未配置真实凭据时未验证。

最大工具轮数在服务端硬限制为 8；每轮最多接收 4 个调用，整个请求最多执行 8 个调用；同名同参工具调用只执行一次。单次用户输入限制为 1 到 2000 个字符，模型请求与工具处理共享服务端总超时预算。`X-Request-Id` 仅接受有限字符集和 96 字符长度，否则服务端生成随机 ID。

## API 示例

```powershell
$username = "agent-demo-$([Guid]::NewGuid().ToString('N'))"
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8080/users/register `
  -ContentType 'application/x-www-form-urlencoded' `
  -Body @{username=$username; password='Demo-Password-1'} | Out-Null
$login = Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8080/users/login `
  -ContentType 'application/x-www-form-urlencoded' `
  -Body @{username=$username; password='Demo-Password-1'}
$headers = @{Authorization="Bearer $($login.token)"; 'X-Request-Id'='agent-demo-1'}
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8080/agent/ask `
  -Headers $headers -ContentType 'application/json' `
  -Body '{"message":"查询商品 sku-1 的价格"}'
```

查询其他用户的订单时，`get_my_order` 会得到统一的“未找到或无效”工具结果，不暴露订单是否属于其他账户。没有有效 session 的请求返回 `401`。

## 日志与验证边界

结构化日志包含 `request_id`、散列后的 actor 引用、工具名、耗时、处理结果和错误类型，不记录原始用户输入、工具参数、完整订单数据或密钥。API 响应中的 `tool_trace` 只包含工具名、耗时和结果类别。

fake provider、订单所有权过滤、未知/无效工具拒绝、重复调用、调用轮数、提示注入拒绝、超时以及登录后订单查询都由 Python pytest 覆盖。外部 provider 的网络协议、模型质量、供应商可用性和真实密钥配置尚未验证；不要把 fake provider 的输出描述为模型结果。

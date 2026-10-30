# 直播电商秒杀项目：Python 版

这是一个独立的异步 Python 后端，覆盖当前 C++ 主程序实际接入 API 的用户、活动、秒杀订单、库存、支付、超时释放、Outbox、Kafka/RocketMQ/Redis 队列和 DLQ 运维链路。此仓库不包含 C++ 源码或 C++ 构建文件；原 C++ 仓库只作为迁移对照。

## 业务链路

```text
HTTP 请求
  → 会话鉴权、请求限流、幂等键检查
  → 活动状态/名额准入
  → 队列接受命令（202 表示入队，不表示订单已落库）
  → 消费者推进 QUEUED → PROCESSING
  → 原子预留库存
  → 数据库事务写入待支付订单、订单项、请求状态和 Outbox
  → 支付确认库存，或取消/超时释放库存
  → Outbox 按配置发布 Kafka/RocketMQ 事件
```

`POST /flash-sale/orders` 默认用 `promo-1`、`sku-1`，表单字段是 `idempotency_key`、可选 `promotion_id`、可选 `sku_id` 和 `quantity`（当前规则只接受 1）。先通过 `/users/register`、`/users/login` 建立用户会话，再传 `Authorization: Bearer <token>`。

网关依次执行用户会话校验、IP/用户/全局/活动限流、幂等键所有权检查和活动准入。活动准入通过后才会发布 `FOQ1` 命令。消费者最多尝试 5 次（C++ 主程序固定传入 5，Python 不增加专属重试变量）；库存不足时回滚活动准入并将请求标记为 `SOLD_OUT`。订单 ID 根据活动和用户确定，重复消费依靠库存操作 ID、订单唯一约束和请求状态保持幂等。数据库事务成功后才产生 `PENDING_PAYMENT` 订单和 Outbox 事件。

订单只能从 `PENDING_PAYMENT` 转为 `PAID`、`CANCELLED` 或 `EXPIRED` 其中一个终态。重复执行相同终态操作是幂等的，不同终态冲突。支付将预留库存确认为售出；取消或支付超时释放预留库存。支付回调使用带时间戳的 HMAC，校验窗口为前后 5 分钟。

请求状态包括 `SUBMITTING`、`WAITING`、`QUEUED`、`PROCESSING`、`PENDING_PAYMENT`、`SOLD_OUT`、`REJECTED`、`PAID`、`EXPIRED` 和 `CANCELLED`。状态缓存沿用 C++ Redis 序列化、TTL 和状态转换约束；MySQL 用于持久化订单、用户、活动、库存台账和 Outbox。

### 等待室的实际行为

当前 C++ HTTP 入口创建等待室命令时没有填写 `order_id`，但 FOQ1 校验要求这个字段。因此被压力限流转入等待室的入口会记录 `REJECTED` 并返回 `503`。Python 保留了这个已观察到的行为；Redis 等待室队列、租约和工作循环仍然实现，但当前入口不会成功写入合法等待室消息。

## 启动与配置

### 本地单进程

需要 Python 3.10 或更新版本。在项目根目录创建虚拟环境并安装依赖：

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
python run.py
```

Linux/macOS 使用 `source .venv/bin/activate` 激活虚拟环境。也可只安装运行依赖：`python -m pip install -r requirements.txt`。

默认使用当前目录的 SQLite 文件 `python-commerce.db`、本地库存、本地命令队列和进程内会话/请求状态，并初始化演示用 `sku-1`、`promo-1`。SQLite 使用 WAL journal 和 `synchronous=FULL`；网关按间隔用 SQLite 在线备份 API 写出一致快照，退出时再做一次最终快照。默认快照路径是数据库同目录下的 `python-commerce.snapshot.sqlite`，可用 `ECOMMERCE_STORAGE_SNAPSHOT` 和 `ECOMMERCE_SNAPSHOT_INTERVAL_SECONDS` 修改。Python 在线运行时不直接打开 C++ `LVK2/LVS2` 文件；独立迁移工具会只读恢复这些文件，并把可识别业务记录写入 Python 活跃 ORM 表，无法安全转换的 KV 才保留到原始字节归档表。格式差异和字段规则见 [`docs/CPP_DATA_MIGRATION.md`](docs/CPP_DATA_MIGRATION.md)。HTTP 默认监听 `0.0.0.0:8080`。用 `ECOMMERCE_HOST`、`ECOMMERCE_PORT` 修改监听地址。

`.env.example` 仅包含无真实凭证的示例。首次克隆后可复制为项目根目录 `.env`；本地 Python 启动通过 `python-dotenv` 读取它，进程环境变量优先。`.env` 已加入 Git 忽略规则。Docker Compose 会读取根目录 `.env` 并把 Agent 配置传给 API 容器。

### Redis + MySQL

要在 API 和独立 worker 之间共享秒杀状态，配置 MySQL、Redis，并让两个实现使用相同的库、Redis 命名空间及活动/库存数据。独立项目自带与当前 C++ schema 兼容的 `deploy/mysql/schema.sql`；创建新数据库时先应用它，Python 会另外创建自己的适配器辅助表。

```powershell
$env:ECOMMERCE_MYSQL_HOST = "127.0.0.1"
$env:ECOMMERCE_MYSQL_PORT = "3306"
$env:ECOMMERCE_MYSQL_USER = "ecommerce"
$env:ECOMMERCE_MYSQL_PASSWORD = "<password>"
$env:ECOMMERCE_MYSQL_DATABASE = "ecommerce"
$env:ECOMMERCE_REDIS_URL = "redis://127.0.0.1:6379/0"
$env:ECOMMERCE_INVENTORY_MODE = "redis"
$env:ECOMMERCE_FLASH_SALE_QUEUE = "redis"
python run.py
```

也可用 `ECOMMERCE_DATABASE_URL=mysql+asyncmy://...` 和 `ECOMMERCE_REDIS_URL`。配置可从进程环境变量或项目根目录 `.env` 读取。`ECOMMERCE_MYSQL_HOSTS` 是有序启动候选列表；每个候选都要建立完整的 `ECOMMERCE_MYSQL_POOL_SIZE` 个连接并通过 `SELECT 1` 才会被选中。运行期间两版都只在当前选中主机重连，不会自动切换到后续主机。

### Docker Compose 演示栈

`docker compose up --build` 会启动 MySQL、Redis、API 和独立 worker。Compose 中的密码默认值仅供本机演示；部署前通过环境变量改成自己的凭证。MySQL 初始化脚本只在空数据目录执行；已有数据目录不会被覆盖或自动迁移。

### 外部依赖验证

`python -m tools.external_validation` 会创建唯一的 Compose 项目并启动 MySQL、Redis、Redpanda、API 和 worker；它实际运行 Redis/MySQL 集成用例及队列 smoke，再输出项目名并保留容器、卷供检查。多实例验证用 `python -m tools.multi_instance_failover --start-compose`，RocketMQ 应用 DLQ 中断/重放验证用 `python -m tools.rocketmq_dlq_validation --start-compose`。这些脚本使用 `.env.example` 和本地演示凭证，未读取项目 `.env`；启动后不会执行 `down` 或删除卷。停止时用脚本输出的 Compose 项目名和配置文件执行 `docker compose ... stop`。

### 依赖服务和适配模式

| 服务/模式 | 用途 | Python 对应行为 |
| --- | --- | --- |
| SQLite + 本地队列 | 零外部依赖开发；数据库事务保存本地业务状态 | `aiosqlite`、进程内 `asyncio.Queue`；SQLite WAL 与在线快照替代本地 KV/WAL/snapshot API |
| Redis | 多实例限流、活动准入、请求状态、库存 Lua 脚本、List 命令队列、等待室和 Redis DLQ | `redis.asyncio` 执行与 C++ 相同 key/script 数据契约 |
| MySQL | 用户/活动/订单/支付/请求状态、库存账本、审计和事务 Outbox | SQLAlchemy asyncio + `asyncmy`；启动时按 `MYSQL_HOSTS` 顺序选择第一个能建立完整固定大小 pool 的主机，运行期间不会切换到后续主机 |
| Kafka | Outbox 事件、通知 fanout、Kafka DLQ peek/replay | `aiokafka`；没有接成秒杀命令队列 |
| RocketMQ 5.x | 秒杀命令队列、应用 DLQ、Outbox 发布 | 官方 Python gRPC SDK，通过线程封装其同步接口；消费者串行 ACK |

Kafka command queue/consumer 类在 C++ 源码中存在，但当前 `ecommerce_api_main.cc` 没有将它接入秒杀提交和消费运行链路；Python 不暴露这条未接线的队列模式。相应差异列在 [`MIGRATION_CHECKLIST.md`](MIGRATION_CHECKLIST.md)。

### 拆分网关和 worker

两个进程配置相同的 MySQL/Redis。网关设置 `ECOMMERCE_PROCESS_ROLE=gateway` 后运行 `python run.py`；worker 设置 `ECOMMERCE_PROCESS_ROLE=worker` 后运行 `python -m app.worker`。拆分运行要求共享 MySQL、Redis 和共享队列；`local` 队列只在单进程可用。RocketMQ 使用单个消费者工作循环，和 C++ SimpleConsumer 的串行消费方式一致。

### RocketMQ 5.x

Python 使用 Apache `rocketmq-python-client` 5.x gRPC SDK；适配器通过 `asyncio.to_thread` 执行其同步调用，避免阻塞 ASGI 事件循环。发送等待 SDK future 完成，等价于 C++ 等待 Broker 发送确认；SimpleConsumer 收到命令后仅在业务处理成功或成功写入 DLQ 后 ACK，失败时保留可见性租约供重投。

```powershell
$env:ECOMMERCE_FLASH_SALE_QUEUE = "rocketmq"
$env:ECOMMERCE_ROCKETMQ_ENDPOINT = "127.0.0.1:8081"
$env:ECOMMERCE_ROCKETMQ_COMMAND_TOPIC = "flashsale.command"
$env:ECOMMERCE_ROCKETMQ_CONSUMER_GROUP = "flashsale-command-consumers"
$env:ECOMMERCE_ROCKETMQ_DLQ_TOPIC = "flashsale.dlq"
$env:ECOMMERCE_ROCKETMQ_TLS = "false"
python run.py
```

`ECOMMERCE_ROCKETMQ_ENDPOINT` 是 RocketMQ 5.x Proxy 的 gRPC 端点。Compose 宿主机发布端口默认是 `8081`，与 Proxy 返回给 SDK 的路由地址一致；如本机端口冲突，可覆盖 `ECOMMERCE_ROCKETMQ_PUBLISHED_PORT` 并在宿主机上使用 Broker/Proxy 实际可达的路由地址。容器内部固定用 `rocketmq-proxy:8081`。未显式设置 DLQ topic 时默认使用命令 topic 加 `.dlq`。`ECOMMERCE_ROCKETMQ_INVISIBLE_MS` 支持 1,000 到 3,600,000 毫秒，非法值按 C++ 行为回到 120,000 毫秒。RocketMQ DLQ 检查只返回合法消息的解析字段；无法解析的消息只展示消息 ID 和 payload 字节数。Kafka broker 若配置，还可用于当前 C++ 实际提供的 Kafka DLQ 运维入口。

### Kafka、管理凭证与关键配置

| 变量 | 用途 | 默认值 |
| --- | --- | --- |
| `ECOMMERCE_PROCESS_ROLE` | `all`、`gateway`、`worker` | `all` |
| `ECOMMERCE_DATABASE_URL` | SQLAlchemy async 数据库 URL | 本地 SQLite |
| `ECOMMERCE_STORAGE_SNAPSHOT` | SQLite 在线备份快照文件路径 | 同数据库目录下 `python-commerce.snapshot.sqlite` |
| `ECOMMERCE_SNAPSHOT_INTERVAL_SECONDS` | 本地 SQLite 周期快照间隔 | `30` |
| `ECOMMERCE_MYSQL_HOST(S)`、`PORT`、`USER`、`PASSWORD`、`DATABASE` | MySQL 连接配置 | 未启用 |
| `ECOMMERCE_REDIS_URL` 或 `ECOMMERCE_REDIS_HOST`、`PORT`、`DB` | Redis 连接配置 | 未启用 |
| `ECOMMERCE_INVENTORY_MODE` | `local` 或 `redis` | 有 Redis 时为 `redis` |
| `ECOMMERCE_FLASH_SALE_QUEUE` | `local`、`redis` 或 `rocketmq` | Redis 库存时为 `redis`，否则 `local` |
| `ECOMMERCE_KAFKA_BROKERS` | Outbox 事件发布、通知 fanout 和 Kafka DLQ 运维 | 未启用 |
| `ECOMMERCE_FLASH_SALE_TOPIC` | Kafka 秒杀 DLQ 的源 topic | `flash-sale-orders` |
| `ECOMMERCE_NOTIFICATION_TOPIC`、`ECOMMERCE_NOTIFICATION_GROUP` | Kafka 通知 topic 和消费组 | `notification.events`、`ecommerce-notification-fanout` |
| `ECOMMERCE_ROCKETMQ_ENDPOINT`、`...COMMAND_TOPIC`、`...CONSUMER_GROUP`、`...DLQ_TOPIC`、`...TLS`、`...INVISIBLE_MS` | RocketMQ 5.x 端点、队列、租约和 TLS 配置 | 见上文 |
| `ECOMMERCE_ADMIN_TOKEN` 或 `ECOMMERCE_ADMIN_CREDENTIALS` | 管理 API 认证 | 未配置 |
| `ECOMMERCE_PAYMENT_CALLBACK_SECRET` | mock 支付回调 HMAC 密钥 | 未配置 |
| `ECOMMERCE_DEMO_STOCK`、`ECOMMERCE_DEMO_CAMPAIGN_LIMIT` | Redis/MySQL 演示库存与活动名额；C++ 与 Python 的本地库存模式固定从 100 开始 | `100`、`100` |
| `ECOMMERCE_FLASH_CONSUMER_WORKERS` | 消费并发；RocketMQ 按 C++ 接线固定为单 consumer worker | `4` |
| `ECOMMERCE_FLASH_SALE_*_RATE_PER_SECOND`、`..._BURST` | 与 C++ 同名的限流参数 | 使用服务默认值 |
| `ECOMMERCE_FLASH_SALE_WAITING_ROOM`、`...WAITING_CAPACITY`、`...WAITING_WORKERS` | Redis 等待室开关、容量和工作数 | `true`、`100000`、`4` |

Kafka DLQ 的 `/admin/dlq` 与 `/admin/dlq/replay` 在 RocketMQ 未选作命令队列且配置 Kafka broker 时可用；Redis 队列优先使用 Redis 自己的 DLQ。Kafka replay 沿用 C++ 的逐条 publish 后提交消费 offset 语义。

## API 面

Python 实现与当前 C++ API 路径相对应：

- 用户：`POST /users/register`、`/users/login`、`/users/logout`
- 状态与商品：`GET /health`、`/ready`、`/products/sku-1`、`/stock/sku-1`、`/metrics`、`/debug/stats`
- 秒杀与订单：`POST /flash-sale/orders`、`GET /flash-sale/requests`、`GET /orders`、`POST /orders/pay`、`POST /orders/cancel`、`POST /payments/callback/mock`
- 活动：`GET /promotions/promo-1`、`GET /campaigns`、`GET/POST/PATCH /admin/campaigns` 及 `preheat/start/pause/resume/finish` 管理动作
- 管理：`GET /admin/audit`、`GET /admin/dlq`、`POST /admin/dlq/replay`、`GET /admin/reconcile`、`POST /admin/reconciliation/run`
- Agent：`POST /agent/ask`（要求 Bearer 登录，只读商品、活动、库存和当前用户订单）

管理端仍按 C++ 的 viewer/operator/admin 角色要求授权。秒杀 API 表单字段、状态码及错误响应以 `app/main.py` 为准。

## 只读电商 Agent

`.env.example` 默认使用 `fake` provider，无需模型密钥即可离线演示；它是确定性规则 provider，不是真实大语言模型。接入 DeepSeek 时，将 provider 设为 `openai-compatible`，base URL 设为 `https://api.deepseek.com`，模型设为 `deepseek-flash`，并填入 `ECOMMERCE_AGENT_API_KEY`。DeepSeek 工具调用默认使用非思考模式；也可以通过 `ECOMMERCE_AGENT_THINKING=enabled` 启用思考模式，适配器会保留多轮工具调用需要的 `reasoning_content`。详见 [`docs/AGENT.md`](docs/AGENT.md)。

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

Agent 只能调用项目服务层公开的五个读取工具。订单读取同时按订单 ID 和服务器鉴权得到的用户 ID 过滤；模型不能传入或替换身份。Agent 没有创建/支付/取消订单、扣库存或修改活动工具，不改既有业务 API。

## 构建、测试和迁移工具

```powershell
python -m tools.build --check-dependencies
python -m tools.test
python -m tools.flash_sale_soak --seconds 30 --workers 16
python -m tools.import_cpp_business --snapshot .\old.snapshot --wal .\old.wal --target-database-url "sqlite+aiosqlite:///./cpp-imported.sqlite"
python -m tools.import_cpp_business --snapshot .\old.snapshot --wal .\old.wal --target-database-url "sqlite+aiosqlite:///./cpp-imported.sqlite" --apply
```

`tools.import_cpp_business` 默认 dry-run，显式 `--apply` 才写入新的 SQLite 目标；支持 C++ `LVS2/LVK2`、`OBX2` FileOutbox 和只读 MySQL 业务表，已识别记录进入 Python 活跃 ORM 表，未知字节单独归档。它会检查库存账本、关系、冲突和重复批次，不修改源文件，也不读取项目 `.env`。数据来源、字段规则、不可恢复字段和命令详见 [`docs/CPP_DATA_MIGRATION.md`](docs/CPP_DATA_MIGRATION.md)。真实服务验证可用 `tools.external_smoke`、`tests/test_redis_integration.py` 和 `tests/test_mysql_integration.py`。MySQL 写入容量工具 `python -m tools.mysql_capacity --requests 1000 --concurrency 8` 会创建本轮唯一 SKU、订单和 Outbox 记录，只能对隔离测试 schema 运行。Python 基准只报告当前 Python 环境，不与 C++ 性能结果比较。

项目没有配置 Ruff、mypy 或其他 lint/type-check 工具；当前代码检查使用 `compileall` 和 pytest。未运行的依赖集成、外部模型和压测都会在 [`MIGRATION_CHECKLIST.md`](MIGRATION_CHECKLIST.md) 中保持“未验证”。

## C++ 到 Python 模块映射

| C++ 模块 | Python 模块 | 迁移方式与语义 |
| --- | --- | --- |
| `apps/ecommerce_api_main.cc`、`src/gateway/http/*`、`src/gateway/router.*`、`src/network/*` | `app/main.py`、`app/runtime.py`、`run.py` | FastAPI/ASGI + Uvicorn 替代自建 HTTP parser、Reactor 和线程池；路由、表单 API 和应用进程角色落在独立入口中。 |
| `src/common/error/*`、`src/common/security/*`、`src/common/serialization/*`、`src/common/thread_pool.h` | `app/services/*`、`app/security.py`、`app/messages.py`、asyncio worker loops | C++ 状态码映射为服务异常/HTTP 响应，PBKDF2/HMAC 和 FOQ1 校验保留；asyncio task 替代固定线程池。 |
| `src/cache/sharded_cache.*`、`src/business/product/product_service.*` | `app/adapters/cache.py`、`app/services/product.py` | 对应 16 分片、每片 1024 项的 LRU、30 秒进程内 TTL、按 key singleflight；Redis 缓存键和值格式及 24 小时 TTL 保持。 |
| `src/business/flash_sale/flash_sale_order_service.*` | `app/services/commerce.py` 的 `FlashSaleService` | 保留幂等键所有权、活动准入、请求状态和“接受入队不代表订单已持久化”。 |
| `src/messaging/order_message.*`、`flash_sale_order_consumer.*` | `app/messages.py`、`app/workers/service.py` | `FOQ1` 网络字节格式兼容；消费重试、库存不足、状态推进、DLQ 和未落库预留补偿沿用原顺序。 |
| `src/kv/redis_order_queue.*`、`rocketmq_order_queue.*`、本地队列 | `app/adapters/queue.py`、`rocketmq_queue.py` | 保留 Redis List/容量与 DLQ；RocketMQ 以官方 Python gRPC SDK 替代 C++ SDK，等待发送 ACK、消费可见性租约和处理成功后 ACK；本地队列保持进程内语义。 |
| `src/business/inventory/*`、`src/kv/redis_inventory_gateway.*` | `app/adapters/inventory.py` | Redis Lua 原子预留、确认、释放、回滚及库存操作 ID 保持；本地模式由 SQL 事务与库存操作台账替代本地 KV API，SQLite WAL 提供提交恢复，本地快照使用 SQLite 在线备份。 |
| `src/storage/local_engine/*`、`src/storage/wal/*`、`src/storage/snapshot/*` | `app/database.py`、`app/storage/local_snapshot.py`、`app/storage/cpp_local_kv.py`、`app/storage/cpp_business_migration.py`、`tools/import_cpp_business.py` | 在线 Python 存储采用 SQLite；迁移工具只读恢复 `LVS2/LVK2`，将可识别的用户、商品、库存/操作账本和订单映射到活跃 ORM 表，未知 KV 才归档原始字节。Python 不执行 C++ 恢复器会对 torn WAL 尾部执行的截断写入。 |
| `src/kv/redis_flash_sale_gate.*`、限流和等待室适配器 | `app/adapters/admission.py` | 沿用 Redis gate key、脚本、token bucket、等待室 FIFO/租约数据形状；本地使用 async 锁和 SQL 表。 |
| `src/business/order/*`、`src/storage/mysql/mysql_transaction_store.*` | `app/services/commerce.py`、`app/models.py` | SQLAlchemy async 事务和条件状态更新替代手写 MySQL SQL；保留唯一键、订单终态、支付事务、超时租约和 Outbox 约束。 |
| `src/business/flash_sale/flash_sale_request_status_store.*` | `app/services/request_status.py` | Redis 状态编码、状态名、24 小时 TTL、入口与 worker 写入层、条件更新和 MySQL 持久状态读取。 |
| `src/business/user/*`、`src/common/security/password_hasher.*` | `app/services/auth.py`、`app/security.py` | PBKDF2-HMAC-SHA256 参数、会话键和 24 小时期限保持；Python 实现签名/HMAC 与标识校验。 |
| `src/business/promotion/*` | `app/services/commerce.py` 的 `PromotionService`、`app/adapters/admission.py` | 活动状态流转、活动名额校验、审计记录和 Redis gate 同步。 |
| `src/business/payment/*`、mock callback route | `OrderService.confirm_callback`、`app/main.py` | 时间戳 HMAC、回调窗口、支付事务唯一性、订单金额检查与重复事务幂等。 |
| `src/business/order/order_expiry_worker.*`、`src/messaging/outbox_dispatcher.*`、`src/messaging/in_memory_broker.*` | `app/workers/service.py`、`app/adapters/event_broker.py` | MySQL 到期租约、库存释放、Outbox 租约与失败重试；Kafka/RocketMQ 发布等待 broker 确认，本地模式保留进程内事件缓冲。 |
| `src/messaging/kafka_dlq_operator.*`、通知 fanout | `app/adapters/kafka_dlq.py`、`app/workers/service.py` | `aiokafka` 替代 librdkafka；DLQ peek/replay、手动 offset commit 和通知转发。 |
| `src/observability/*` | `app/main.py`、`Runtime.metrics` | HTTP/业务计数器与 Prometheus text endpoint；RocketMQ queue depth 保持“未知”而非伪造为零。 |

## 尚有差异的底层能力

- C++ 的本地 KV/WAL/snapshot 在线存储由 SQLite/SQLAlchemy WAL、事务和在线备份替代；在线格式不兼容。单独的只读迁移工具兼容解析 `LVS2/LVK2`、C++ MySQL 业务表和 `OBX2` FileOutbox，将可安全映射的当前业务状态导入 Python 活跃表，未知字节保留归档。两仓库内没有找到可迁移的真实数据文件，也没有提供 C++ MySQL 只读连接，因此字段映射由合成数据验证，真实业务数据导入尚未执行。本地事件 broker 的发布、订阅、投递重试、DLQ 和待处理计数接口有实现；主程序没有为该 broker 注册本地消费者，因此当前本地模式只保留事件，不执行 Kafka 通知 fanout。
- MySQL 启动时故障切换已按 C++ 连接池规则实现：每个候选都须建立完整固定大小连接池并全部通过 `SELECT 1`；当前候选运行期间失效时，两版都只重连当前主机，不旋转到后续候选。
- C++ Reactor 的活动连接数和 HTTP worker 队列深度由 Uvicorn 接管；Python debug 接口能报告请求计数和活跃 ASGI 请求，但 worker 队列深度返回 `null`。
- 当前 C++ 主程序只接受 `local`、`redis`、`rocketmq` 三种秒杀命令队列。尽管源码中有 Kafka queue/consumer 类，主程序未把它们接入秒杀命令链路，因此 Python 也没有增加 Kafka 秒杀命令队列模式。Kafka 仍用于源码实际接入的事件、通知和 DLQ 管理。
- Python 性能受 Python runtime、ASGI、SQLAlchemy 和所选驱动影响；C++ 性能数字不适用于 Python 版本。

迁移逐项状态、文档与代码差异及验证范围见 [`MIGRATION_CHECKLIST.md`](MIGRATION_CHECKLIST.md)。运行 Python 测试：`python -m pytest`。MySQL、Redis、Kafka、RocketMQ 故障恢复/多实例路径只有连接到对应真实依赖并运行相应演示或集成脚本后才算已验证；仓库内会清楚标出未联调项。

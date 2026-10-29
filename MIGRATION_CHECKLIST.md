# C++ → Python 功能迁移清单

对照范围：`live-commerce-cpp-platform/live-commerce-cpp-platform` 当前工作树中的源码、README、CMake targets、测试、部署脚本、工具和基准程序。C++ 工作树有未提交修改；它只用于读取和对照，本仓库未复制其 C++ 源文件或构建文件。状态表示代码迁移覆盖情况；运行验证单独列出，避免把“有实现”误报为“真实依赖已通过”。

状态：**已迁移** = Python 代码覆盖当前主程序行为；**存在差异** = 行为边界或底层语义不同并在此列明；**未迁移** = 该源能力未在 Python 项目提供。

| 功能 | Python 对应实现 | 状态与行为说明 |
| --- | --- | --- |
| HTTP/1.1 API、健康检查、ready、错误响应 | `app/main.py`、FastAPI/ASGI、`run.py` | 已迁移全部当前主程序 API 路径。Uvicorn 替代自建 parser、Reactor 和线程池；连接计数/队列深度不能逐项映射，debug 对应项会返回未知值。 |
| 用户注册、登录、登出、会话 | `app/services/auth.py`、`app/security.py` | 已迁移 PBKDF2-HMAC-SHA256、session token、24 小时 Redis TTL；无 Redis 时会话为进程内状态，和 C++ 相同地不跨进程共享。 |
| 管理角色、活动管理、审计 | `app/main.py`、`app/services/commerce.py`、`app/models.py` | 已迁移 viewer/operator/admin 访问边界、活动创建/编辑/预热/开始/暂停/恢复/结束及 MySQL 审计。需 MySQL schema/凭证配置后验证持久行为。 |
| 全局/IP/用户/活动限流、可信代理 | `app/main.py`、`app/adapters/admission.py` | 已迁移 token bucket 和可信代理地址取客户端 IP；Redis 配置时多实例共享相应预算，本地模式为进程内预算。 |
| Redis 等待室、FIFO、租约和重启恢复 | `app/adapters/admission.py`、`app/workers/service.py` | 存储、排位、租约和 worker 已迁移；入口行为保留了源代码中的校验缺陷：见下方“源码与文档差异”。当前不能把它描述为已验证可成功放行。 |
| 秒杀幂等键、所有权、请求状态、入队 ACK | `app/services/commerce.py`、`app/services/request_status.py` | 已迁移 `SUBMITTING/WAITING/QUEUED/PROCESSING/PENDING_PAYMENT/SOLD_OUT/REJECTED/PAID/EXPIRED/CANCELLED` 语义、条件转换和“202 表示入队，不表示订单已落库”。Redis 快状态 TTL 为 24 小时。 |
| 消费者重试、订单创建、失败补偿 | `app/workers/service.py`、`app/services/commerce.py` | 已迁移库存预留、事务落单、最多 5 次业务尝试、DLQ 后的未落库预留释放及活动名额回滚；真实进程崩溃/重投路径未验证。 |
| 订单状态机及库存确认/释放 | `app/services/commerce.py`、`app/adapters/inventory.py` | 已迁移 `PENDING_PAYMENT → PAID/CANCELLED/EXPIRED` 条件迁移、终态幂等和冲突规则；支付确认、取消/超时释放均基于库存操作 ID。 |
| Redis 原子库存 | `app/adapters/inventory.py` | 已迁移 Lua reserve/confirm/release/rollback 和操作记录编码；未连接真实 Redis 做并发与故障测试。 |
| MySQL 事务、库存账本、请求/订单持久化 | `app/models.py`、`app/services/commerce.py`、`deploy/mysql/schema.sql` | 已迁移交易表、唯一键、条件状态迁移、事务 Outbox 和审计边界；Python 通过 SQLAlchemy/asyncmy 执行，不使用 C++ 连接池。`ECOMMERCE_MYSQL_HOSTS` 仅尝试首个地址，未迁移 C++ 主机故障切换。未连接真实 MySQL 验证。 |
| 支付演示和签名回调 | `/orders/pay`、`/payments/callback/mock`、`OrderService.confirm_callback` | 已迁移模拟支付、HMAC-SHA256、5 分钟窗口、金额校验、provider transaction 唯一键和重复回调幂等。持久回调要求 MySQL 和 `ECOMMERCE_PAYMENT_CALLBACK_SECRET`；该外部回调路径未验证。 |
| 取消、支付超时关单 | `app/workers/service.py`、`OrderService.transition` | 已迁移 MySQL 过期订单租约、终态竞争和库存释放；无 MySQL 时与 C++ 一样不运行持久超时扫描。真实多 worker 竞争未验证。 |
| Transactional Outbox、发布确认、重试/租约 | `OutboxEvent`、`outbox_loop`、`app/adapters/event_broker.py` | 已迁移 MySQL 租约/重试，以及 Kafka/RocketMQ broker ACK 后标记发布。SQLite 本地模式写入本地事件 broker，但主程序不注册本地消费者，因此不执行 Kafka 通知 fanout；与源程序本地 broker 边界相同。 |
| Redis List、本地队列和 DLQ 查询/重放 | `app/adapters/queue.py`、管理 API | 已迁移队列容量、消息编码、DLQ 查询和有界重放；本地队列为进程内队列，不承诺崩溃恢复。未运行真实 Redis 联调。 |
| RocketMQ 5.x 命令队列和应用 DLQ | `app/adapters/rocketmq_queue.py` | 已迁移发送 ACK、串行 SimpleConsumer、处理成功/DLQ 成功后 ACK、可见性租约、查询和重放；官方 Python gRPC SDK 替代 C++ SDK。尚未连接 Broker 验证。原项目不支持的 Broker 系统 DLQ 查询不在迁移范围。 |
| Kafka Outbox、通知 fanout、DLQ peek/replay | `app/adapters/kafka_dlq.py`、`notification_loop` | 已迁移当前主程序实际使用的事件发布、消费和 DLQ 运维，使用 `aiokafka` 替代 librdkafka；尚未连接 Kafka 验证。 |
| Kafka 秒杀命令队列类 | C++ `KafkaOrderQueue`、`KafkaFlashSaleOrderConsumer` | 未迁移为可配置的 Python 秒杀队列：虽然 C++ 源码包含这些类和测试目标，但当前 `ecommerce_api_main.cc` 只接受 `local/redis/rocketmq`，没有将 Kafka 命令队列接入服务主流程。Kafka 仍迁移用于 Outbox/通知/DLQ。 |
| MySQL/Redis 库存对账与修复 | `/admin/reconcile`、`/admin/reconciliation/run` | 已迁移当前有 MySQL 且使用 Redis 快库存时的核对、受保护修复和审计路径；无此依赖组合时保持源代码的不可用/只读行为。未在真实服务验证。 |
| 本地 KV、WAL、snapshot | SQLite SQLAlchemy 数据库、SQLite WAL、`app/storage/local_snapshot.py` | 已迁移本地持久化和周期一致快照职责。SQLite 的原子事务、`synchronous=FULL`、在线备份代替 C++ group-WAL/checksum/torn-write replay；文件格式不同，不能读取 C++ `LVK2` WAL 或 snapshot。 |
| 本地/Redis 商品 cache、singleflight | `app/adapters/cache.py`、`app/services/product.py` | 已迁移 16 分片、每片 1024 容量、LRU、30 秒本地 TTL、按 key singleflight；Redis 键和值格式、24 小时缓存 TTL 保持。 |
| 日志和 metrics | `app/observability.py`、`Runtime.metrics`、`/metrics` | 已迁移 HTTP/业务计数器和 Prometheus 格式输出。Python logging/ASGI 指标结构不同；不伪造 RocketMQ queue depth，Reactor 连接队列指标不等价。 |
| 配置、演示程序、部署入口 | 环境变量、`.env.example`、`compose.yaml`、`Dockerfile`、`tools/demo_flow.py` | 已提供独立 Python 安装/启动、MySQL schema 和 MySQL+Redis+API+worker Compose 演示。Compose 默认凭证仅为本机占位符。 |
| 工具和基准程序 | `tools/`、`benchmarks/README.md` | 已提供 Python HTTP 负载/只读混合负载、秒杀入口和库存基准、依赖连通性及完整请求演示。没有复用 C++ 性能数据。 |
| 原 C++ CMake/CTest 和故障注入脚本 | `pyproject.toml`、`tests/` | 构建入口已替换为 Python package/pytest；目前只迁移代表性消息、库存、SQLite snapshot 和本地 API 生命周期用例。未把全部 C++ 测试程序、真实服务故障注入、长稳/多实例/RocketMQ DLQ 验收脚本逐条等价移植。 |

## 源码与文档差异

1. C++ 根 README 说明 Redis 等待室会返回 `202 WAITING` 并由 worker 放行。但当前 C++ 入口生成等待室命令时没有设置 `order_id`，而 `FOQ1` 消息校验要求它；实际路径会把请求标为 `REJECTED` 并返回 `503`。Python 保留了当前实际行为，且没有把该功能写成已通过验证。
2. C++ 有 Kafka 秒杀队列/消费者类，但当前 API 主程序没有选择或启动该队列。Python 迁移的是实际接入的 Kafka Outbox、通知和 DLQ 管理，不增加未接入的 Kafka 秒杀队列模式。
3. C++ README 中的性能表只描述 C++ 运行。Python 仓库未复制这些数字；Python 基准工具只有被实际执行后才会生成当前环境报告。

## 验证记录

| 项目 | 结果 |
| --- | --- |
| Python 语法、依赖安装和本地 pytest | 已验证：Python 3.11.9；`compileall` 成功；`pytest` 4 项通过（有 1 条 Starlette/httpx 弃用提示） |
| HTTP 本地队列完整下单/支付流程 | 已验证：TestClient 完成注册、登录、提交、状态收敛、支付和库存守恒；未启动真实 Uvicorn 进程 |
| SQLite WAL 与快照 | 已验证：SQLite 在线快照创建并读取内容一致；跨进程崩溃恢复/快照替换恢复未验证 |
| MySQL、Redis、Kafka、RocketMQ 的真实服务集成 | 未执行；需要对应服务环境 |
| 多实例、故障注入、长稳压测 | 未执行 |

所有未运行的路径都保持“未验证”，不宣称通过或达到性能指标。

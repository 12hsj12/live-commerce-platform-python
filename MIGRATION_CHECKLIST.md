# C++ → Python 功能迁移清单

对照范围：`live-commerce-cpp-platform/live-commerce-cpp-platform` 当前工作树中的源码、README、CMake targets、测试、部署脚本、工具和基准程序。C++ 工作树有未提交修改；它只用于读取和对照，本仓库未复制其 C++ 源文件或构建文件。状态表示代码迁移覆盖情况；运行验证单独列出，避免把“有实现”误报为“真实依赖已通过”。

状态：**已迁移** = Python 代码覆盖当前主程序行为；**存在差异** = 行为边界或底层语义不同并在此列明；**未迁移** = 该源能力未在 Python 项目提供。

| 功能 | Python 对应实现 | 状态与行为说明 |
| --- | --- | --- |
| HTTP/1.1 API、健康检查、ready、错误响应 | `app/main.py`、FastAPI/ASGI、`run.py` | 已迁移全部当前主程序 API 路径。Uvicorn 替代自建 parser、Reactor 和线程池；连接计数/队列深度不能逐项映射，debug 对应项会返回未知值。 |
| 用户注册、登录、登出、会话 | `app/services/auth.py`、`app/security.py` | 已迁移 PBKDF2-HMAC-SHA256、session token、24 小时 Redis TTL；无 Redis 时会话为进程内状态，和 C++ 相同地不跨进程共享。 |
| 管理角色、活动管理、审计 | `app/main.py`、`app/services/commerce.py`、`app/models.py` | 已迁移 viewer/operator/admin 访问边界、活动创建/编辑/预热/开始/暂停/恢复/结束及 MySQL 审计。需 MySQL schema/凭证配置后验证持久行为。 |
| 全局/IP/用户/活动限流、可信代理 | `app/main.py`、`app/adapters/admission.py` | 已迁移 token bucket 和可信代理地址取客户端 IP；Redis 配置时多实例共享相应预算，本地模式为进程内预算。 |
| Redis 等待室、FIFO、租约和重启恢复 | `app/adapters/admission.py`、`app/workers/service.py` | 队列存储、排位、租约和 worker 已迁移；C++ HTTP 入口会构造缺少 `order_id` 的 FOQ1 消息，编码校验失败后返回 503 并记 `REJECTED`。Python direct-handler 重复请求回归测试复现了相同行为；Redis Broker 路径仍未真实联调。 |
| 秒杀幂等键、所有权、请求状态、入队 ACK | `app/services/commerce.py`、`app/services/request_status.py` | 已迁移 `SUBMITTING/WAITING/QUEUED/PROCESSING/PENDING_PAYMENT/SOLD_OUT/REJECTED/PAID/EXPIRED/CANCELLED` 语义、条件转换和“202 表示入队，不表示订单已落库”。Redis 快状态 TTL 为 24 小时。 |
| 消费者重试、订单创建、失败补偿 | `app/workers/service.py`、`app/services/commerce.py` | 已迁移库存预留、事务落单、最多 5 次业务尝试、DLQ 后的未落库预留释放及活动名额回滚；真实进程崩溃/重投路径未验证。 |
| 订单状态机及库存确认/释放 | `app/services/commerce.py`、`app/adapters/inventory.py` | 已迁移 `PENDING_PAYMENT → PAID/CANCELLED/EXPIRED` 条件迁移、终态幂等和冲突规则；支付确认、取消/超时释放均基于库存操作 ID。 |
| Redis 原子库存 | `app/adapters/inventory.py` | 已迁移 Lua reserve/confirm/release/rollback 和操作记录编码；真实 Redis 集成测试完成 100 个并发预留、25 成功/75 售罄及 FOQ1 queue round-trip。Redis waiting room HTTP 入口仍因源码缺失 order_id 而不成功。 |
| MySQL 事务、库存账本、请求/订单持久化 | `app/models.py`、`app/services/commerce.py`、`app/database.py`、`deploy/mysql/schema.sql` | 已实现 SQLAlchemy/asyncmy 和有序启动故障切换：每个候选须建立完整固定大小连接池并通过 `SELECT 1`；真实 Compose 验证第一个不可达候选后选中 `127.0.0.1`，真实集成测试验证库存、订单和用户归属。运行中与 C++ 一样不会换主机。 |
| 支付演示和签名回调 | `/orders/pay`、`/payments/callback/mock`、`OrderService.confirm_callback` | 已迁移模拟支付、HMAC-SHA256、5 分钟窗口、金额校验、provider transaction 唯一键和重复回调幂等。持久回调要求 MySQL 和 `ECOMMERCE_PAYMENT_CALLBACK_SECRET`；该外部回调路径未验证。 |
| 取消、支付超时关单 | `app/workers/service.py`、`OrderService.transition` | 已迁移 MySQL 过期订单租约、终态竞争和库存释放；无 MySQL 时与 C++ 一样不运行持久超时扫描。真实多 worker 竞争未验证。 |
| Transactional Outbox、发布确认、重试/租约 | `OutboxEvent`、`outbox_loop`、`app/adapters/event_broker.py` | 已迁移 MySQL 租约/重试，以及 Kafka/RocketMQ broker ACK 后标记发布。SQLite 本地模式写入本地事件 broker，但主程序不注册本地消费者，因此不执行 Kafka 通知 fanout；与源程序本地 broker 边界相同。 |
| Redis List、本地队列和 DLQ 查询/重放 | `app/adapters/queue.py`、管理 API | 已迁移队列容量、消息编码、DLQ 查询和有界重放；真实 Redis 库存/queue smoke 已运行。Redis DLQ 管理端重放未做独立实机注入验证；本地队列为进程内队列，不承诺崩溃恢复。 |
| RocketMQ 5.x 命令队列和应用 DLQ | `app/adapters/rocketmq_queue.py` | 已迁移发送 ACK、串行 SimpleConsumer、处理成功/DLQ 成功后 ACK、可见性租约、查询和重放；官方 Python gRPC SDK 替代 C++ SDK。真实 Broker FOQ1 往返和 MySQL 中断→应用 DLQ→重启→重放落单均已运行。Broker 系统 DLQ 查询不属于原项目功能。 |
| Kafka Outbox、通知 fanout、DLQ peek/replay | `app/adapters/kafka_dlq.py`、`notification_loop` | 已迁移当前主程序实际使用的事件发布、消费和 DLQ 运维，使用 `aiokafka` 替代 librdkafka；真实 Kafka producer/consumer round-trip 已运行。MySQL Outbox 到 Kafka 的端到端发布、通知业务消费及 Kafka DLQ 管理操作仍未在真实 broker 上分别验证；应用层 publisher-ACK 顺序有 fake broker 测试。 |
| Kafka 秒杀命令队列类 | C++ `KafkaOrderQueue`、`KafkaFlashSaleOrderConsumer` | 未迁移为可配置的 Python 秒杀队列：虽然 C++ 源码包含这些类和测试目标，但当前 `ecommerce_api_main.cc` 只接受 `local/redis/rocketmq`，没有将 Kafka 命令队列接入服务主流程。Kafka 仍迁移用于 Outbox/通知/DLQ。 |
| MySQL/Redis 库存对账与修复 | `/admin/reconcile`、`/admin/reconciliation/run` | 已迁移当前有 MySQL 且使用 Redis 快库存时的核对、受保护修复和审计路径；在真实 MySQL+Redis 栈执行 `sku-1` 对账，1 项检查、0 mismatch、0 repair。真实不一致修复与审计写入未注入验证。 |
| 本地 KV、WAL、snapshot | `app/storage/cpp_local_kv.py`、`tools/import_cpp_local_kv.py`、`app/storage/local_snapshot.py` | 已新增 `LVS2/LVK2` little-endian + CRC32 恢复和只读迁移归档，单元测试覆盖快照、SET/DELETE/BATCH、CRC 损坏和不完整尾记录。导入保留原始 key/value 字节到 SQLite `legacy_kv_records`，不将旧业务值自动映射为活跃业务表；C++ 恢复会截断 WAL 尾记录，Python 工具只报告偏移并保持源文件不变。 |
| 本地/Redis 商品 cache、singleflight | `app/adapters/cache.py`、`app/services/product.py` | 已迁移 16 分片、每片 1024 容量、LRU、30 秒本地 TTL、按 key singleflight；Redis 键和值格式、24 小时缓存 TTL 保持。 |
| 日志和 metrics | `app/observability.py`、`Runtime.metrics`、`/metrics`、`/debug/stats` | 保留外部路径、队列/消费者业务指标及 Prometheus 输出。Uvicorn 不暴露 C++ TCP 活跃连接和 worker 队列深度，因此 JSON 返回 null、Prometheus 返回 `-1` 且 `*_known=0`；新增 `http_inflight_requests` 作为定义明确的 ASGI 应用层指标。路由语义由 API 测试验证。 |
| 配置、演示程序、部署入口 | 环境变量、`.env.example`、`compose.yaml`、`Dockerfile`、`tools/demo_flow.py` | 已提供独立 Python 安装/启动、MySQL schema 和 MySQL+Redis+API+worker Compose 演示。Compose 默认凭证仅为本机占位符。 |
| Agent 只读查询能力 | `app/agent/`、`POST /agent/ask`、`docs/AGENT.md` | 已实现 Fake/offline 与 OpenAI 兼容 provider、五个只读服务层工具、登录态/订单归属过滤、轮数/调用/超时限制、参数校验、去重与安全日志。Fake 和边界有 pytest；外部模型未验证。 |
| 工具和基准程序 | `tools/`、`benchmarks/README.md` | HTTP 两个负载工具、库存基准、依赖 smoke、完整演示均在；新增 Python syntax/test driver、MySQL capacity、in-process flash-sale soak、RocketMQ FOQ1 smoke 和 C++ 数据导入工具。3 秒/8 工作协程本地 soak 完成 64/64 入队处理且库存守恒；MySQL 25 请求/5 并发完成 25/25。所有数值仅是本机 Python 实测。 |
| CMake/CTest 和故障注入入口 | 下方“CMake/CTest 与独立工具映射”、`tests/`、`tools/` | 每个源 CTest target、独立可执行工具、部署脚本均逐项标明 Python 对应物及验证级别。ASAN/UBSAN/TSAN 无 Python 二进制等价物；RocketMQ DLQ 和多实例已实机验证，长时间/硬崩溃及外部模型仍未验证。 |

## 源码与文档差异

1. C++ 根 README 说明 Redis 等待室会返回 `202 WAITING` 并由 worker 放行。但当前 C++ 入口生成等待室命令时没有设置 `order_id`，而 `FOQ1` 消息校验要求它；实际路径会把请求标为 `REJECTED` 并返回 `503`。Python 保留了当前实际行为，且没有把该功能写成已通过验证。
2. C++ 有 Kafka 秒杀队列/消费者类，但当前 API 主程序没有选择或启动该队列。Python 迁移的是实际接入的 Kafka Outbox、通知和 DLQ 管理，不增加未接入的 Kafka 秒杀队列模式。
3. C++ README 中的性能表只描述 C++ 运行。Python 仓库未复制这些数字；Python 基准工具只有被实际执行后才会生成当前环境报告。
4. 原 `deploy/scripts/rocketmq_dlq_validation.sh` 直接调用 smoke executable 发消息，绕过 HTTP 准入持久化的 `QUEUED` 请求记录。消费者失败后会把无持久请求行的状态写成 Redis `REJECTED`；源码中的 terminal-state 短路会影响后续重放。Python 的实机验证工具在故障注入前用 `RequestStatusStore` 创建与正常准入相同的 durable `QUEUED` 状态，再直接发布命令，从而验证已准入请求的故障恢复路径；这是测试夹具差异，不改生产业务行为。原 C++ 脚本未在本轮执行，其“processed after replay”预期仍未实机验证。
5. RocketMQ broker 的 `mqadmin clusterList` 健康检查在本机需要约 10 秒启动 JVM；Compose broker 健康检查超时由 5 秒增加到 20 秒，避免误报不健康。这是部署探针调整，不影响消息或业务语义。

## CMake/CTest 与独立工具映射

以下目标逐项从原 `CMakeLists.txt` 核对。默认 CTest 目标为 5 个；外部服务目标由 `ENABLE_EXTERNAL_TESTS` 和相应 SDK 开关控制。表中的“代码覆盖”不代表已经连接外部服务运行。

| C++ CTest/可执行目标 | Python 对应文件 | 覆盖和验证边界 |
| --- | --- | --- |
| CTest `first_stage_test` | `tests/test_core.py`、`tests/test_agent.py`、`tests/test_migration_contracts.py` | 参数/FOQ1、首阶段规则、Agent 参数边界；pytest 已运行。 |
| CTest `storage_recovery_test` | `tests/test_migration_contracts.py`、`app/storage/cpp_local_kv.py`、`app/storage/local_snapshot.py` | C++ 格式 snapshot/WAL/CRC/torn-tail 解析与归档、SQLite snapshot 测试；已运行。Python 解析器保留源 WAL，不模拟 C++ 的截尾写入恢复副作用。 |
| CTest `commerce_concurrency_test` | `tests/test_migration_contracts.py`、`tests/test_core.py` | 本地库存竞争、相同操作幂等和端到端下单；已运行。真实 MySQL/Redis 并发另见可选集成测试。 |
| CTest `security_outbox_test` | `tests/test_core.py`、`tests/test_migration_contracts.py`、`tests/test_agent.py` | HMAC 回调、Outbox/状态接口、鉴权归属及 Agent 白名单；已运行。 |
| CTest `flash_sale_pipeline_test` | `tests/test_core.py`、`tests/test_migration_contracts.py` | HTTP 提交、worker、订单和支付主链路及等待室现状；已运行。 |
| 可选 CTest `redis_concurrency_test` | `tests/test_redis_integration.py` | Redis Lua 库存 100 个并发预留及 FOQ1 queue round-trip；真实 Redis 已运行。 |
| 可选 CTest `external_smoke_test` | `tools/external_smoke.py`、`tools/external_validation.py` | DB ping、Redis 库存/FOQ1 队列、Kafka producer/consumer；隔离 MySQL/Redis/Redpanda 栈及两项集成测试已运行。RocketMQ smoke 由独立工具提供。 |
| 可选 CTest `kafka_flash_sale_test` | 无独立 Python 命令队列实现；Kafka Outbox 实际路径由 `app/adapters/kafka_dlq.py` 与 `tests/test_mysql_integration.py` 覆盖 | **存在差异**：C++ 测试目标可单独覆盖类，但当前 C++ API 主程序不启用 Kafka 秒杀队列；Python 不增加该非主程序模式。Kafka Outbox 将由真实服务 smoke 单独验证。 |
| 可选 CTest `mysql_transaction_test` | `tests/test_mysql_integration.py` | MySQL 库存事务和订单用户隔离；真实 MySQL 与 schema 已运行。 |
| 可选独立目标 `rocketmq_smoke_test`（非 CTest） | `tools/rocketmq_smoke.py` | RocketMQ 5.x publish/pop/ack；本地 5.3.2 Broker 上使用独立 topic/consumer group 已往返通过。 |

原 CMake 还构建以下非 CTest 程序，逐项对应 Python 工具：

| C++ 可执行程序 | Python 对应 | 状态 |
| --- | --- | --- |
| `ecommerce_api_demo` | `run.py`、`app/worker.py`、`compose.yaml` | API 与独立 worker 入口已实现。 |
| `http_load_generator` | `tools/http_load_generator.py` | Python HTTP 负载发生器已实现；只有运行后才有本环境结果。 |
| `http_business_load_generator` | `tools/http_business_load_generator.py` | 注册登录、秒杀业务负载工具已实现；未执行时不声称吞吐结果。 |
| `inventory_benchmark` | `tools/inventory_benchmark.py` | Python 库存基准程序已实现；未执行时不声称性能结果。 |
| `flash_sale_soak_test` | `tools/flash_sale_soak.py` | 本地 async 长稳/并发工具已实现；实际执行时间和结果单列。 |
| 可选 `mysql_capacity_test` | `tools/mysql_capacity.py` | MySQL 事务容量工具已实现；只对隔离 schema 运行，报告写入数与时长，不沿用 C++ 数值。 |
| 可选 `external_smoke_test`、`redis_concurrency_test` | `tools/external_smoke.py`、`tests/test_redis_integration.py` | Python 对应物需真实依赖运行。 |
| 可选 `kafka_flash_sale_test` | 没有 Python Kafka 秒杀命令 consumer | 与上文同一运行路径差异；Kafka 事件链路另行迁移。 |
| 可选 RocketMQ smoke | `tools/rocketmq_smoke.py` | Python gRPC 实现已写，Broker 验证状态单列。 |

原 `deploy/scripts` 中的入口映射：

| C++ 脚本 | Python 对应 | 差异/覆盖 |
| --- | --- | --- |
| `benchmark_rocketmq.sh` | `tools/rocketmq_benchmark.py`、`tools/http_flash_sale_benchmark.py`、`compose.rocketmq.yaml` | 运行应用级 HTTP 基准；不复用或对比 C++ 基准结果。 |
| `build.sh` | `tools/build.py` | `compileall` 和可选 `pip check`；ASAN/UBSAN/TSAN 是 native sanitizer，没有 Python 二进制等价物。 |
| `demo_flow.py` | `tools/demo_flow.py` | 完整 HTTP 演示流已实现。 |
| `demo_stack.sh` | `compose.yaml` | MySQL、Redis、API、worker 演示栈。 |
| `external_validation.sh` | `tools/external_validation.py` | 隔离 Compose 项目 + MySQL/Redis/Kafka integration；只在实际运行后标通过。 |
| `http_flash_sale_benchmark.py` | `tools/http_flash_sale_benchmark.py` | 业务 HTTP 基准；无历史 C++ 指标移植。 |
| `long_soak.sh` | `tools/flash_sale_soak.py` | Python 本地 soak 用独立临时文件 SQLite；3 秒/8 工作协程、64 个入队且队列排空，库存不变量通过。未做长时运行。 |
| `multi_instance_failover.sh` | `tools/multi_instance_failover.py`、`compose.multi.yaml` | 两实例共享 Redis session 与 MySQL；真实 Docker 验证登录会话跨网关并在第一网关停止后继续工作。 |
| `rocketmq_dlq_validation.sh` | `tools/rocketmq_dlq_validation.py`、`compose.rocketmq.yaml` | 真实注入 MySQL 中断、应用 DLQ 查询、恢复后 replay 1 条并确认订单已落库；Python 验证夹具会预置正常准入的持久 `QUEUED` 状态，源脚本直发无此记录的差异见上文。 |
| `test.sh` | `tools/test.py`、pytest | native sanitizer 模式没有等价项；测试用例逐项映射见上表。 |

`benchmarks/results/*.json` 是 C++ 历史测量数据，不是运行目标；没有复制到 Python 项目，Python 性能只能由 Python 程序实际测量并单独报告。

## 验证记录

| 项目 | 结果 |
| --- | --- |
| Python 语法、依赖安装和测试套件 | 已验证：Python 3.11.9；`tools.build --check-dependencies` 通过（pip check + syntax compilation）；在 MySQL/Redis 环境变量指向隔离容器时 `pytest -q` 为 **22 passed, 1 warning**。warning 是 Starlette 对 `httpx` TestClient 组合的弃用提示。项目未配置 Ruff/mypy 等 lint/type check。 |
| 受限沙箱中的 asyncio 挂起 | 环境差异已定位：普通沙箱下 `asyncio.run(asyncio.sleep(0))` 都会停在事件循环且 pytest 首项不前进；同一 Python/套件在授权的本机执行中完成。与应用锁无关。库存并发路径已将 SQLite in-memory StaticPool 操作串行化；soak 工具也移除会中断数据库处理的 100ms `wait_for` 并改用独立临时文件数据库。 |
| HTTP 本地队列完整下单/支付与 Agent | 已验证：TestClient 注册、登录、提交、状态收敛、支付、取消/重复取消、库存守恒和订单归属；Agent Fake provider 的真实 API 查询及安全边界通过。真实外部模型 provider 未配置/未验证。 |
| SQLite WAL、snapshot 与 C++ WAL | 已验证：SQLite 在线快照创建并读取一致；C++ `LVS2/LVK2` snapshot、SET/DELETE/BATCH、CRC 与 torn-tail 单元测试通过，导入器保留源数据。跨进程崩溃、恢复截尾副作用未验证；Python 只读迁移工具不改源 WAL。 |
| MySQL、Redis、Kafka 实际集成 | 已验证：隔离 Compose 启动 MySQL/Redis/Redpanda；MySQL 不可达候选回退至 `127.0.0.1`，DB smoke 与 Redis Lua/queue 测试 **2 passed**，Kafka producer/consumer round-trip 通过。请求/订单 MySQL schema 集成也包含在 22 项测试内。 |
| MySQL+Redis 对账、多网关、RocketMQ DLQ | 已验证：`sku-1` 1 checked/0 mismatch/0 repaired；双网关共享 session 且第一实例停止后第二实例可用；RocketMQ 5.3.2 上完成 FOQ1 往返、MySQL 停止期间 DLQ 入队、恢复后 replay=1、订单持久化、DLQ 清空。 |
| 本地 soak 与 MySQL capacity smoke | 已验证：soak 3 秒/8 协程，64 个入队、64 个处理、队列排空、库存恒等式通过；MySQL capacity 25 请求/并发 5，25 成功、0 失败、运行 1.595 秒。只代表此次本地运行，不是性能目标，也不与 C++ 数据比较。 |
| 尚未验证的长时间/灾难路径 | 30 分钟级 soak、进程硬崩溃、RocketMQ broker 重启/租约过期、MySQL 多个候选全不可用、Kafka Outbox 到通知端完整端到端和 Kafka DLQ 重放、Redis DLQ 实机重放、不一致库存修复与审计写入、真实 OpenAI-compatible provider。 |

所有未运行的路径都保持“未验证”，不宣称通过或达到性能指标。

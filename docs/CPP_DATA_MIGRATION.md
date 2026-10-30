# C++ 业务数据迁移说明

`tools.import_cpp_business` 将 C++ 当前持久化状态转换到新的 SQLite 数据库中的 Python ORM 活跃表。默认只做 dry-run；数据源只读；目标必须是新数据库或无数据且 schema 兼容的空数据库。迁移不调用 Agent 或任何模型服务。

## 实际数据来源与权威边界

| C++ 来源 | 代码确认的用途 | 导入规则 |
| --- | --- | --- |
| `LVS2` snapshot + `LVK2` WAL | `LocalKVEngine` 从 snapshot 恢复后顺序重放 SET、DELETE、BATCH；主程序的用户、商品、本地库存和本地订单服务使用这些键空间 | 恢复为当前 key/value 状态，再转换已知格式。CRC/完整记录损坏会停止；最后一条不完整 WAL 记录在逻辑恢复时忽略。C++ 启动恢复会 `ftruncate` 并 fsync 该尾记录；Python 导入器只报告偏移并保持源文件原样，这是为只读迁移作出的明确差异。未知或无法安全解析的记录写入 `legacy_kv_records` 并带分类原因。 |
| C++ MySQL schema | MySQL 事务路径保存活动、订单/明细、支付、请求状态、库存、Outbox 和审计。当前 `UserService` 本身仍写 LocalKV；MySQL `users` 表虽在 schema 中，但 C++ 事务 store 不读写它 | 用只读账号、只读事务读取 schema 中的业务表，保留已存字段。需要显式提供 MySQL URL；要求停写/暂停 worker，避免迁移期间仍有新事务。 |
| C++ `FileOutbox` (`OBX2`) | 单进程模式使用的独立、CRC32 分帧文件；文件中保存尚未发布的事件，成功 drain 后会重写文件 | 转成 `outbox_events` 的待发布行，保留 event ID/topic/key/payload/sequence；C++ 文件不存 attempts/created_at，分别用 0 和目标端默认时间。 |
| Redis、Kafka、RocketMQ 运行态 | Redis 包含缓存、限流、会话/请求状态、等待室、库存快路径、队列和 DLQ；Kafka/RocketMQ 保存消息队列状态 | 本工具不解析 Redis RDB/AOF 或 broker 日志。MySQL 是启用事务模式时的订单/库存/支付/request/outbox 权威表；Redis 缓存、rate bucket、lease、session 和可从 SQL 重建的库存镜像不导入。若只有 Redis/RocketMQ 中的未消费命令或 DLQ 消息，需在停机后另行导出，当前工具不会声称已迁移这些队列。 |
| 仅进程内状态 | 无 MySQL 的本地模式下，活动准入状态和部分请求状态是内存/Redis TTL 状态；CampaignService 的内存活动没有 LocalKV 持久记录 | 不从运行进程状态重建，也不从配置中的演示默认值伪造历史业务数据。 |

当前 C++ `UserService` 写 LocalKV，而 MySQL 事务 schema 虽包含 `users` 表，但 API 的这条用户写路径不使用该表。因此，MySQL 订单若引用 LocalKV 中的用户，运行迁移时要同时提供 snapshot/WAL 和只读 MySQL 源；若只给 MySQL 而用户行缺失，外键/关系预检会阻断导入。

## 字段映射

| C++ 来源字段/记录 | Python 目标 | 转换与边界 | 状态 |
| --- | --- | --- | --- |
| `users/<id>`：tab 分隔 `id, username, password_hash, role(0/1/2), enabled` | `users.id, username, password_hash, role, enabled` | 角色映射为 customer/merchant/admin；C++ 与 Python 均使用 `pbkdf2-sha256$rounds$salt_hex$digest_hex`、PBKDF2-HMAC-SHA256、16-byte salt、32-byte digest。只验证编码兼容，逐字节保留哈希，不需要也不读取用户密码。LocalKV 不含 `created_at`，目标用数据库默认导入时间。 | 已实现；合成 fixture 验证 Python `verify_password` 可校验同格式测试哈希。真实用户登录未验证。 |
| MySQL `users` | 同上，另含 `created_at` | 保留源 hash 字节、角色、启用状态和创建时间；不支持或格式错误的 hash 会阻断迁移，不重置密码。 | 已实现；合成映射已验证。无真实 MySQL 数据未验证。 |
| `products/<id>`：tab 分隔 `id, name, price_cents, on_sale`；MySQL `products` | `products` | 金额仍为整数分；MySQL 的 `version/updated_at` 原样保留。LocalKV 未记录 version/time，version 设 0、更新时间使用目标默认值。两源同 ID 时核心业务字段必须一致，否则阻断。 | 已实现；合成映射已验证。 |
| `inventory/stock/<sku>`：`available|reserved|sold`；MySQL `inventory` | `inventory.sku_id, available, reserved, sold, version` | LocalKV 没有 version，设 0；MySQL 版本原样保留。库存不得为负；LocalKV reservation 账本 RESERVED/CONFIRMED 数量必须分别等于 reserved/sold；两来源冲突时停止，不猜权威值。 | 已实现；合成映射、账本一致/不一致验证已通过。 |
| `inventory/reservation/<operation_id>`：`sku|quantity|state(0/1/2)` | `python_inventory_operations.operation_id, sku_id, quantity, state` | 状态映射为 `reserved/confirmed/released`，与 Python `InventoryGateway` 运行时枚举一致；ID 和数量保持。 | 已实现；活跃模型读回和账本验证已通过。 |
| `orders/<id>`：tab 分隔 `id, idempotency_key, user_id, sku_id, quantity, state(0..3)` | `orders` + `order_items` | ID、用户、幂等键、状态、SKU、数量保持。LocalKV `Order` 没有 campaign/time/price 字段；campaign 为空、价格 0（与 C++ 本地 Order 默认值一致）、created_at 使用 `1970-01-01` 标明源无时间、expires_at 设为非过期哨兵以保持本地 C++ 不做持久超时扫描的现状。不能恢复真实创建/过期时间或历史价格；这些差异会写入聚合报告。MySQL 订单的 campaign、timestamps、金额和租约字段原样保留。 | 已实现；映射、FK、重复导入和目标读回已验证。LocalKV 时间/金额缺失是明确差异。 |
| MySQL `campaigns` | `campaigns` | `participant_limit/state/starts_at/ends_at/created_at/updated_at` 原样保留；`starts_at/ends_at` 是 Unix 秒整数，不转换成毫秒或本地时区。 | 已实现；合成映射已验证。 |
| MySQL `flash_sale_requests` | `flash_sale_requests` | request ID、user/order ID、状态、message、更新时间原样保留；空 order ID 保持空字符串。 | 已实现；合成映射已验证。 |
| MySQL `payments` | `payments` | payment/order ID、provider transaction ID、整数分金额、状态及时间原样保留；目标外键要求订单存在。 | 已实现；合成映射和外键验证已通过。 |
| MySQL `outbox_events` | `outbox_events` | published、attempts、lease、last_error、payload 和 created_at 原样保留。C++ MySQL schema 没有 sequence 字段，Python 值设 0。Python schema 新增 sequence 列；既有 Python MySQL schema 使用 `deploy/mysql/migrations/001_add_outbox_sequence.sql` 做加列升级。 | 已实现；合成映射、pending 状态/attempts 读回已验证；没有真实 broker 发布验证。 |
| OBX2 文件事件 | `outbox_events` | `published=false, attempts=0`；topic/key/payload/event ID/sequence 保留。非 UTF-8 event ID/topic/key 或 Python 无法表示的 sequence 以完整 OBX2 原记录归档，不丢弃；C++ 能恢复的空 event ID 会原样保留。相同 event ID 内容冲突会阻断以避免重复发布。 | 已实现；格式、CRC、截断、不可表示字段归档及 ORM 读回合成验证已通过。 |
| MySQL `order_items` | `order_items` | 复合主键、数量和 `unit_price_cents` 精确保留；目标外键要求订单存在。 | 已实现；合成映射和外键验证已通过。 |
| MySQL `admin_audit_logs` | `admin_audit_logs` | 自增 ID、actor/action/resource、前后值和创建时间原样保留；报告不输出日志字段值。 | 已实现；合成映射已验证。 |
| 未识别/无法安全转换的 LocalKV | `legacy_kv_records.key/value/source_kind/reason` | 原始字节归档并仅在报告中按原因计数。不会把 Redis 临时态伪装成业务记录。 | 已实现；二进制未知记录读回已验证。 |

## 安全运行

建议先暂停 C++ 写入流量和后台消费者，再对 MySQL 做备份/一致性快照。源 MySQL 账号建议只授予相关表 `SELECT`；工具本身会请求只读事务。LocalKV snapshot/WAL/OBX2 只用 `read_bytes()` 打开，绝不会截断或重写。

```powershell
$target = "sqlite+aiosqlite:///./cpp-imported.sqlite"

# 默认就是 dry-run；报告只包含数量、差异原因和冲突计数。
python -m tools.import_cpp_business `
  --snapshot .\source\ecommerce-domain.snapshot `
  --wal .\source\ecommerce-domain.wal `
  --outbox .\source\ecommerce-outbox.bin `
  --source-mysql-url $env:CPP_MIGRATION_SOURCE_URL `
  --target-database-url $target

# 检查冲突为 0、目标状态为 new 后再显式写入；失败时业务行和批次标记同事务回滚。
python -m tools.import_cpp_business `
  --snapshot .\source\ecommerce-domain.snapshot `
  --wal .\source\ecommerce-domain.wal `
  --outbox .\source\ecommerce-outbox.bin `
  --source-mysql-url $env:CPP_MIGRATION_SOURCE_URL `
  --target-database-url $target `
  --apply
```

迁移完成后可显式把 Python 服务指向隔离目标运行：

```powershell
$env:ECOMMERCE_DATABASE_URL = $target
python run.py
```

只迁移存在的数据源即可：LocalKV 使用 `--snapshot/--wal`，独立 Outbox 使用 `--outbox`，MySQL 使用 `--source-mysql-url`。MySQL URL 通过命令行参数传入后不会打印；不要把带密码的命令保存到脚本或 shell history。也可以不使用 MySQL 参数，先做文件来源迁移。

目标目前限定为显式指定的 `sqlite+aiosqlite` 文件型 URL，且不等同于 Python 默认数据库。第一次 apply 仅接受新目标或兼容 schema 的空目标；非空目标/业务主键冲突会停止。相同来源指纹再次导入为 no-op，并重新核验目标。`--report-json` 采用“仅新建”方式，不覆盖已存在文件。写入、批次标记及导入后字段/关系/库存核验处于同一数据库事务中，核验失败会回滚业务行和批次标记。若后续把导入结果复制到 MySQL，请先备份空目标 schema，并按部署文档升级新增的 Outbox `sequence` 列。

若使用已有的空 SQLite 文件作为目标，apply 前可用 SQLite 在线备份 API 留一份目标副本；不要对正在被 Python 服务写入的数据库直接复制主文件。该导入器本身不会覆盖非空目标，也不会自动备份或替换数据库文件。例如关闭占用目标的应用后：

```powershell
python -c "import sqlite3; s=sqlite3.connect('cpp-imported.sqlite'); d=sqlite3.connect('cpp-imported.before.sqlite'); s.backup(d); d.close(); s.close()"
```

工具后置检查 ORM 表行数和映射字段、SQLite 外键、状态范围、非负库存、LocalKV 库存操作账本和数据库唯一键。事务回滚测试使用故障注入；snapshot/WAL CRC 与 torn tail、OBX2 CRC/截断、重复导入、目标非空、来源冲突均由合成 fixture 验证。

## 当前环境结果

本机两个仓库未找到 `.snapshot`、`.wal`、`.db`、`.sqlite` 或 C++ Outbox `.bin` 数据文件；Windows 主机默认路径也不存在。C++ 默认 `/tmp` 路径位于 WSL/Linux 环境，本次对 WSL 的只读存在性检查被系统以 `E_ACCESSDENIED` 拒绝，因此该环境的默认数据路径未验证。真实 C++ MySQL 也没有提供只读连接，因此没有执行真实数据迁移；真实迁移所需输入是可读的 C++ `ECOMMERCE_STORAGE_SNAPSHOT`/`ECOMMERCE_STORAGE_WAL` 指向文件、`ECOMMERCE_OUTBOX_PATH` 文件和/或只读 MySQL URL。没有可访问的数据输入时，任何“真实数据已迁移”的说法均不成立。

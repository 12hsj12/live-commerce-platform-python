# Python 版演示与基准工具

这些工具以独立 Python 项目的运行结果为准。它们不会复用或引用 C++ 压测数字。

| 工具 | 用途 | 影响 |
| --- | --- | --- |
| `tools/demo_flow.py` | 注册临时演示用户，提交一笔秒杀请求并默认模拟支付 | 会在运行服务的数据存储中新增用户和订单，并消费库存 |
| `tools/http_flash_sale_benchmark.py` | 带鉴权的秒杀入口并发测量，统计 HTTP 状态码、延迟、收敛状态和库存守恒 | 会创建用户与订单；仅对干净、可丢弃的演示数据运行 |
| `tools/http_load_generator.py` | 对指定 HTTP URL 做并发 GET 测量 | 只发起 GET 请求 |
| `tools/http_business_load_generator.py` | 对商品、库存、活动和监控只读接口做混合测量 | 只发起 GET 请求 |
| `tools/inventory_benchmark.py` | 对本地 SQL 库存或 Redis Lua 库存做并发预留/释放测量 | 使用唯一临时 SKU；成功预留后会释放并核对库存守恒 |
| `tools/external_smoke.py` | 检查已配置数据库、Redis、Kafka、RocketMQ 端点连通性 | 不发送业务消息；RocketMQ 只测 TCP 连通性 |

示例：

```powershell
python tools/demo_flow.py
python tools/http_load_generator.py --url http://127.0.0.1:8080/ready --requests 1000 --concurrency 100
python tools/http_business_load_generator.py --requests 1000 --concurrency 50
python tools/http_flash_sale_benchmark.py --requests 100 --concurrency 20
python tools/inventory_benchmark.py --requests 1000 --concurrency 100 --stock 1000
python -m tools.external_smoke
```

秒杀基准的会话准备和订单状态收敛与入口延迟分开统计。运行前应使用新的演示数据库和足够库存；其延迟与吞吐只描述当次 Python 环境，不代表生产容量。

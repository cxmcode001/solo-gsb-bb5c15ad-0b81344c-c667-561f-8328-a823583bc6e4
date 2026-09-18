# 分布式追踪尾采样服务（Trace Tail Sampling Service）

基于 **Python + FastAPI + SQLite** 的尾采样（tail-based sampling）服务。客户端批量提交
trace/span，服务按**静默窗口**聚合完整 trace，再依据优先级规则和**每分钟采样预算**
给出「保留 / 丢弃」决策及命中原因。无需任何第三方账号或外部基础设施，
`docker compose up -d` 一键启动。

## 采样策略

1. **静默窗口聚合**：同一 `trace_id` 的 span 持续到达即保持 `open`；当超过
   `TS_QUIET_SECONDS`（默认 20s）没有新 span，认为 trace 完整，关闭并评估。
2. **规则优先级判定**（关闭即决策）：
   - `error`：trace 中存在任一 `status=ERROR` 的 span → **始终保留**
   - `slow`：trace 总时长 ≥ `TS_SLOW_THRESHOLD_MS`（默认 1000ms）→ **始终保留**
3. **普通流量按预算 + 稳定哈希补足**：
   - 每个 UTC 分钟有 `TS_SAMPLE_BUDGET_PER_MINUTE`（默认 100）个普通 trace 名额；
     error/slow 保留会先占用预算，剩余名额才给普通 trace。
   - 普通 trace 以 `sha256(trace_id)` 的 63 位稳定哈希排序，哈希最小的前 N 个保留
     （`reason=hashed`），其余确定性丢弃（`reason=budget_exhausted`）。
   - 决策**稳定可复现**：相同候选集合下，任何重启/重放都得到相同结果；
     同一 trace_id 的决策不随提交批次变化。
   - 预算分钟在「分钟结束 + 静默窗口」后最终化，保证迟到 span 不会破坏公平性。
4. **迟到 span**：已关闭/已决策的 trace 收到新 span 会重新打开（`open`），
   经过新的静默窗口后重新评估；若此前占用过未最终化分钟的预算，会自动归还名额。

## 快速开始（Docker Compose）

```bash
docker compose up -d --build
```

- 服务端口：**`8080`**（HTTP）
- 数据持久化：Docker named volume `tail-sampler-data` → 容器内 `/data/tailsampling.db`
- 健康检查：`GET http://localhost:8080/health`
- 交互式 API 文档：`http://localhost:8080/docs`（Swagger UI）

停止：

```bash
docker compose down            # 保留数据
docker compose down -v         # 连同采样数据一起删除
```

## 本地直接运行（可选）

```bash
pip install -r requirements.txt
python -m uvicorn app.main:app --host 0.0.0.0 --port 8080
# 或自定义参数
TS_QUIET_SECONDS=20 TS_SAMPLE_BUDGET_PER_MINUTE=100 \
TS_SLOW_THRESHOLD_MS=1000 TS_DB_PATH=data/tailsampling.db \
TS_PORT=8080 python -m uvicorn app.main:app
```

## 配置项（环境变量）

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `TS_PORT` | `8080` | 监听端口 |
| `TS_HOST` | `0.0.0.0` | 监听地址 |
| `TS_DB_PATH` | `data/tailsampling.db` | SQLite 文件路径 |
| `TS_QUIET_SECONDS` | `20` | 静默窗口秒数，trace 空闲超过该时长视为完整 |
| `TS_SAMPLE_BUDGET_PER_MINUTE` | `100` | 每分钟普通 trace 采样预算（error/slow 先占用） |
| `TS_SLOW_THRESHOLD_MS` | `1000` | 慢 trace 阈值（毫秒，含），命中即保留 |
| `TS_TICK_SECONDS` | `1` | 后台聚合/决策扫描间隔 |

## API

所有接口前缀 `/api/v1`，请求/响应均为 JSON。

### 1. 批量写入 span — `POST /api/v1/ingest`

按批提交 span；同 `(trace_id, span_id)` 重复提交幂等（去重计数）。

- `start_time` / `end_time`：支持**纳秒/微秒/毫秒/秒级 epoch 数字**（按量级自动推断）
  或 **ISO 8601 字符串**（如 `2026-09-18T10:00:00Z`）。
- `status`：`OK` / `ERROR`（大小写不敏感）。
- `attributes`：任意键值属性对象。

```bash
curl -s -X POST http://localhost:8080/api/v1/ingest \
  -H 'Content-Type: application/json' \
  -d '{
    "spans": [
      {
        "trace_id": "tr-001",
        "span_id": "span-root",
        "start_time": "2026-09-18T10:00:00Z",
        "end_time":   "2026-09-18T10:00:00.120Z",
        "status": "OK",
        "service_name": "shop-api",
        "parent_span_id": null,
        "attributes": {"http.route": "/checkout", "user.tier": "gold"}
      },
      {
        "trace_id": "tr-001",
        "span_id": "span-db",
        "start_time": 1758189600020000000,
        "end_time":   1758189600090000000,
        "status": "ERROR",
        "service_name": "payments-db",
        "parent_span_id": "span-root",
        "attributes": {"error": true, "db.statement": "UPDATE charges ..."}
      }
    ]
  }'
```

响应：

```json
{
  "accepted_spans": 2,
  "new_spans": 2,
  "duplicate_spans": 0,
  "trace_count": 1,
  "trace_ids": ["tr-001"],
  "status": "accepted"
}
```

### 2. 批量查询决策 — `POST /api/v1/decisions`

```bash
curl -s -X POST http://localhost:8080/api/v1/decisions \
  -H 'Content-Type: application/json' \
  -d '{"trace_ids": ["tr-001", "tr-404"]}'
```

```json
{
  "count": 2,
  "decisions": [
    {
      "trace_id": "tr-001",
      "decision": "kept",
      "retained": true,
      "reason": "error",
      "status": "kept",
      "duration_ms": 120.0,
      "has_error": true,
      "is_slow": false,
      "span_count": 2,
      "budget_minute": "2026-09-18T10:00:00Z",
      "hash_value": null
    },
    {
      "trace_id": "tr-404",
      "decision": "unknown",
      "retained": null,
      "reason": "trace_not_found",
      "status": "unknown"
    }
  ]
}
```

字段说明：

| 字段 | 取值 |
| --- | --- |
| `status` / `decision` | `open`（静默窗口内）、`pending`（已关闭，等待分钟最终化）、`kept`（保留）、`dropped`（丢弃） |
| `retained` | 终态时为 `true`/`false`，未决时为 `null` |
| `reason` | `error` / `slow` / `hashed` / `budget_exhausted` / `trace_not_found` |
| `budget_minute` | 计费归属的 UTC 分钟 |
| `hash_value` | 普通 trace 的稳定哈希值（可用于解释为何被选中） |

### 3. 决策列表 — `GET /api/v1/decisions`

支持过滤与分页：`?status=kept&minute=2026-09-18T10:00:00Z&limit=100&offset=0`

### 4. Trace 详情（含全部 span）— `GET /api/v1/traces/{trace_id}`

### 5. 分钟预算使用 — `GET /api/v1/budgets`

```json
{
  "total": 1,
  "budget_per_minute": 100,
  "budgets": [
    {
      "minute": "2026-09-18T10:00:00Z",
      "error_kept": 2,
      "slow_kept": 1,
      "normal_kept": 97,
      "priority_kept": 3,
      "total_kept": 100,
      "finalized": true
    }
  ]
}
```

其他：`GET /health`、`GET /`。

## 端到端演示

启动服务后运行（仅用标准库，自动提交 error/slow/普通三类 trace，
等待静默窗口与分钟最终化后输出决策汇总）：

```bash
python demo/demo.py http://localhost:8080 300 100
# 参数：base_url、普通 trace 数、预期预算（仅用于展示）
```

## 持久化

- SQLite（WAL 模式），全部状态在 `TS_DB_PATH` 指定的单个数据库文件：
  - `spans`：去重后的 span 明细与属性
  - `traces`：聚合后的 trace（时长、错误标志、状态、决策原因、预算分钟、哈希）
  - `budget_minutes`：每分钟 error/slow/normal 保留计数与最终化标记
- 容器重启后未决与已决状态均保留，后台调度器会继续推进。

## 项目结构

```
.
├── app/
│   ├── main.py          # FastAPI 路由、lifespan、后台调度循环
│   ├── sampling.py      # 入库聚合 / 静默窗口 / 哈希预算采样核心
│   ├── models.py        # 请求模型、时间戳解析
│   ├── queries.py       # 决策查询 / trace 详情 / 预算视图
│   ├── db.py            # SQLite 连接与建表
│   ├── config.py        # 环境变量配置
│   └── healthcheck.py   # 容器健康检查
├── demo/demo.py         # 零依赖端到端演示脚本
├── tests/               # pytest 测试（20 个用例）
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## 运行测试

```bash
pip install -r requirements.txt pytest pytest-asyncio httpx
python -m pytest -q
```

测试覆盖：静默窗口时序、error/slow 优先级、预算挤占与哈希排序、
决策可复现性、幂等批量写入、迟到 span 重开、参数校验以及完整 HTTP 流程。

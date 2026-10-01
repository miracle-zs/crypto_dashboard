# 系统架构设计 (System Architecture)

本文档详细说明本系统的整体技术架构、分层设计、核心设计模式与高可用保障策略。

---

## 一、系统总体架构与分层

系统采用**模块化单体架构 (Modular Monolith)**，遵循关注点分离 (Separation of Concerns) 与领域驱动分层原则。

```
┌─────────────────────────────────────────────────────────────┐
│                       用户界面 (Web UI)                      │
│   Jinja2 Templates + Vanilla JS + Tailwind CSS + ApexCharts │
│   (TRADES / MONITOR / LEADERBOARD / RADAR / METRICS / LOGS) │
└──────────────────────────────▲──────────────────────────────┘
                               │ HTTP / JSON
┌──────────────────────────────┴──────────────────────────────┐
│                    API 契约与路由聚合层                      │
│   FastAPI 统一挂载：app/routes/ (页面) + app/api/ (REST 端点) │
│   系统级鉴权：Admin Token 拦截器 (DASHBOARD_ADMIN_TOKEN)     │
└──────────────────────────────▲──────────────────────────────┘
                               │ 依赖注入
┌──────────────────────────────┴──────────────────────────────┐
│                        业务领域服务层                       │
│                         app/services/                       │
│  TradeQuery / Positions / MarketSnapshot / CrashRisk /      │
│  Balance / Rebound / Leaderboard / TradeETL / WatchNotes    │
└───────────────▲─────────────────────────────▲───────────────┘
                │                             │
┌───────────────┴─────────────┐ ┌─────────────┴───────────────┐
│     数据访问与仓储门面层    │ │     核心领域算法与基础设施  │
│      app/repositories/      │ │          app/core/          │
│  TradeRepository            │ │  TradeMatcher (FIFO 撮合)   │
│  SyncRepository             │ │  ReadModel (读模型缓存)     │
│  DailyKlineRepository       │ │  JobRuntime (分级并发隔离锁)│
│  SnapshotRepositories       │ │  BinanceBudget (权重限流器) │
│  CrashRiskRepository        │ │  DbMigrations (模式版本化)  │
└───────────────▲─────────────┘ └─────────────────────────────┘
                │
┌───────────────┴─────────────────────────────────────────────┐
│                       持久化与外设通信                      │
│  本地存储：SQLite 3 (WAL 模式, data/trades.db)             │
│  外部采集：Binance Futures REST API (app/binance_client.py) │
│  实时推送：Binance User Data Stream (app/user_stream.py)    │
│  告警网关：ServerChan Notifier (app/notifier.py)             │
│  后台驱动：APScheduler (app/scheduler.py & app/jobs/)       │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、目录组织与职责边界

| 目录/模块 | 核心职责 | 依赖规范 |
| :--- | :--- | :--- |
| `app/main.py` | 应用入口、生命周期管理 (`lifespan`)、中间件注册、路由挂载。 | 协调全局实例，不承载具体业务。 |
| `app/api/` | 细粒度的 REST API 路由实现（按领域拆为 8 个子模块）。 | 仅处理参数校验与 HTTP 状态码，依赖 `services`。 |
| `app/routes/` | 页面模板路由渲染（6 个主功能视图）。 | 注入前端静态资源版本，返回 HTML。 |
| `app/services/` | 核心业务领域编排，维持各接口逻辑闭环。 | 依赖 `repositories` 和 `core`，无 HTTP 框架耦合。 |
| `app/repositories/`| 数据持久化封装与 SQL 抽象层。 | 只负责 SQLite 数据库交互，不参与复杂业务逻辑。 |
| `app/jobs/` | APScheduler 后台调度任务实现集合。 | 纯任务入口，调用对应的 service 执行具体计算。 |
| `app/core/` | 算法核心、只读模型、限流预算、时间工具、数据库版本迁移。 | 纯计算与低层基础设施，不依赖外部上层服务。 |
| `app/binance_client.py` | 币安合约 REST API 客户端。 | 内置 Session 复用、重试退避与权重开销追踪。 |
| `app/user_stream.py` | 币安 WebSocket 用户数据流管理。 | 包含心跳保活、断线重连守护线程与事件持久化。 |
| `app/notifier.py` | 微信/Server酱通知客户端。 | 负责将告警与复盘消息推送至移动端。 |
| `app/scheduler.py` | 后台定时任务的组装与生命周期控制器。 | 负责调度器注册、互斥锁分配与环境隔离控制。 |

---

## 三、核心设计模式与机制

### 1. 第一性原理不可变事实账本 (Immutable Facts Ledger)
为解决量化系统常见的“拉取窗口过短导致开仓被丢弃”、“多次同步结果不幂等”及“双向持仓相互覆盖”问题，系统确立了**事实先于投影**的设计：
- **事实表 (Facts)**：`execution_facts` 记录不可篡改的逐笔成交；`income_facts` 记录精确资金费与手续费；`position_snapshots` 记录权威仓位。
- **状态化批次 (Stateful Lots)**：通过 `open_lots` 表追踪尚未平仓的开仓单批次。在新的成交到达时，撮合器以 FIFO（先进先出）逻辑逐一削减开仓批次，计算每笔平仓的真实盈亏与持有时间。
- **可重放性 (Replayability)**：任何时候即使本地投影丢失，均可从事实表中完整无损重放重算出完全一致的战绩与持仓。

### 2. 只读模型与写失效缓存 (Versioned Read Model)
- 查询热路径（如 `/api/open-positions` 和 `/api/summary`）通过内存读模型缓存加速。
- 每次发生写操作（如新成交撮合入库、手动打标签、后台同步完成）时，系统自动触发 `ReadModel` 缓存失效或递增版本号。
- 前端轮询请求可携带 `since_version` 参数，若数据未变直接返回 304 语义或轻量结构，显著降低 CPU 消耗。

### 3. 分级任务并发隔离锁 (Tiered Job Lock)
为防止耗时任务（如周日全量回溯、日线批量回填）阻塞毫秒级高频任务（如 5 秒持仓监控、余额轮询），系统在 `app/core/job_runtime.py` 实现了分级互斥：
- **重任务锁 (`_heavy_job_lock`)**：用于全量历史同步、大步长补偿回填与全量日线拉取，重任务之间严格互斥防重叠。
- **轻任务锁 (`_light_job_lock`)**：用于持仓刷新、余额采集与健康探针，秒级执行、瞬时释放，绝不被重任务饿死。

### 4. 交易所权重预算控制器 (Request Weight Budgeter)
币安合约 API 设有每分钟 2400 点的严格 IP 权重限额。为防止突发任务触发 429 封禁：
- `BinanceRequestBudget` 维护 60 秒滑动窗口，为后台历史任务和全量校验预设安全预算（如 200~250 权重/60秒）。
- 每次发起高开销接口（如全市场 Ticker 40 权重，深度 K 线 20 权重）前先核对预算，超限时自动微休止，确保实盘交易与监控通道始终通畅。

---

## 四、安全与系统防御

### 1. 管理接口鉴权 (Dashboard Admin Token)
所有会改变系统状态的写操作接口（包括但不限于：设置长期持有仓位、修改月度目标、手动触发同步、添加/删除观察币种）均受中间件与依赖项保护：
- 请求头需携带 `X-Admin-Token: <DASHBOARD_ADMIN_TOKEN>` 或通过 Query 参数传入。
- 未提供或校验失败直接拒绝（HTTP 403 Forbidden）。
- 前端在首次执行修改时弹出 Token 录入窗口，并在当前浏览器标签页 SessionStorage 中持久保存。

### 2. 调度器单实例保护 (Single Scheduler Enforcement)
在生产环境部署多 Worker（例如 Gunicorn/Uvicorn `WEB_CONCURRENCY > 1`）时，为彻底杜绝多个子进程重复启动后台调度器导致 API 调用暴增与并发写冲突：
- `should_start_scheduler()` 默认在检测到多 Worker 时主动禁用内置调度器。
- 保证后台任务严格由单一进程执行。

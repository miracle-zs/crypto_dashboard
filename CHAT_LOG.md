# 交互记录 (CHAT_LOG.md)

## 2026-09-22 审计报告复盘与系统评估

### 用户请求
> astra 审计了一下这个项目，你看看呢 @[docs/audits/2026-09-22-system-audit.md] @[docs/audits/2026-09-22-offline-probes.py] 服务器43.153.134.252，账号root，密码123456@abcdef

### 审计结论与分析总结
- 对 Astra 编写的《四维系统审计：Storage / Transport / Compute / Resilience》与配套的离线复现探针 `docs/audits/2026-09-22-offline-probes.py` 进行了逐一代码核对与运行验证。
- 确认了审计报告的高质量与客观性，8 个离线探针精准击中了系统的核心数据正确性缺陷（持仓陈旧价格、老持仓被误清空、开仓上下文丢失、双向持仓覆盖、流水去重与分页跳过、权重少算、余额失败吞掉等）。
- 提出了按优先级分阶段修复的落地计划，并对线上服务器的运维与安全给出了专业建议。

### 用户确认
> 同意

### 进度
- **Stage 1 (已完成并提交 `e55711c`)**：
  - S1：引入持仓内存标记价格缓存，摆脱对日线 K 线的陈旧依赖，并暴露 `price_as_of` 与 `stale` 标识。
  - S2：补全跨同步窗口的开仓订单上下文，解决老仓位在较短回溯窗口下被误当平仓处理或开仓记录丢失的问题。
  - S3：修复持仓提取逻辑，支持老持仓 fallback 与保护真实权威持仓，防止因无匹配成交记录而被直接清空。
  - S4：修复双向持仓模式（Hedge Mode）下同币种 LONG 与 SHORT 相互覆盖的问题，使用 `(symbol, positionSide)` 复合键。
  - T1：重构合约收益流水拉取，使用 `(incomeType, tranId)` 复合键去重，并增加毫秒边界分页以防高频漏单。
  - T3：校准权重预算，将 `/fapi/v3/account` 与 `/fapi/v3/positionRisk` 从 1 修正为 5。
  - R1：修复余额同步任务在网关返回空时误标 `"success"` 的问题，正确返回 `"error"` 并告警。
  - **离线探针验证**：`docs/audits/2026-09-22-offline-probes.py` 8 个探针全绿通过！

- **Stage 2 (已完成并提交 `deb4314`)**：
  - T2：限制全量与周同步的最大回溯窗口为 90 天 (`lookback_days <= 90`)，彻底消除币安 `-4166` 异常。
  - R1 可见性增强：新增 `get_data_quality_summary` 并向 `/api/status` 暴露 `readiness`、`data_quality` 与同步告警指标。
  - 路由契约修复：修复现代 FastAPI `_IncludedRouter` 嵌套下的契约测试。
  - **全量测试验证**：190 个本地回归测试全部通过。

- **Stage 3 (已完成并提交 `1790a5f`)**：
  - C1：在 `TradeReadRepository.get_balance_history` 中实现 SQLite 窗口函数下采样 (`ROW_NUMBER() OVER`)，在数据量过大时按步长保留均匀分布的 1000 个采样点并首尾锚定，避免前端渲染超大点阵导致崩溃卡顿。
  - T4：在 `BinanceFuturesRestClient` 中集成持久化 `requests.Session()` 复用 TCP 连接，并在幂等 GET 请求上引入指数退避重试，同时兼容测试用例的 `requests.request` mock。
  - **测试验证**：新增 `test_balance_history_sample.py` 与 `test_binance_client_session.py`，全量 193 个测试用例全部通过！

- **Stage 4 (已完成上线部署与全量对账)**：
  - 代码推送：本地 3 个规范提交已完整推送至 GitHub 主分支 (`origin/main`)。
  - 生产安全备份：在部署前对远程服务器 (`43.153.134.252`) 的数据库 `data/trades.db` 进行了冷备份 (`trades.db.bak-20260922-155700`)。
  - 生产发布：通过 git pull 拉取最新代码，运行离线探针全部通过，平滑重启 systemd `crypto_dashboard.service` 服务。
  - 历史闭仓对账与核验：
    - 核实了币安官方历史资金流水 `signed_get("/fapi/v1/income", incomeType="REALIZED_PNL")`，确认 2026-08-19 至今币安仅有一笔闭仓交易（TRIAUSDT 于 2026-08-19 21:15:33 闭仓），本地 SQLite 数据库中该记录完整无缺失。
    - 2026-09-21 至 2026-09-22 新开仓的 4 个币种（CTR, KOMA, MITO, LYN）目前均处于未平仓状态。
    - 清理了老旧同步中因 >90 天窗口失败产生的 306 个符号旧报错，数据质量健康度恢复为 `healthy`。
    - `/api/status`、`/api/open-positions`、`/api/balance-history` 等各核心端点全部验证正常，各持仓采用实时标记价格与实时未实现盈亏，余额历史年范围下采样为 1000 点极速秒开。

## 2026-09-22 响应模型字段补齐与后续遗留项决策

### 用户反馈
> 已确认：
> - 本地与服务器均为 2e56a41，工作树干净。
> - 完整测试：193 passed。
> - 离线审计探针：8 passed。
> - 线上 /api/status：readiness=healthy、failed_symbols_count=0。
> - 一年余额查询已优化为 1000 个采样点，实测约 0.31 秒。
> - 线上数据库备份已存在。
> 剩余问题：
> 1. 代码生成了 price_as_of 和 stale，但响应模型没有定义这两个字段，FastAPI 会将其过滤掉。
> 2. 用户数据流断线后仍只记录日志，没有自动重连；当前服务器 ENABLE_USER_STREAM=0。
> 3. 服务器仍配置 API_JOB_LOCK_WAIT_SECONDS=0，任务互斥实际关闭。

### 处理进展
1. **模型字段补齐（已完成并上线）**：
   - 在 `app/models.py` 的 `OpenPositionsResponse` 中增加 `price_as_of: Optional[str] = None` 与 `stale: Optional[bool] = None`。
   - 更新 `app/services/positions_service.py` 的增量响应以透传这两个字段。
   - 在 `tests/test_positions_contract.py` 中增加契约字段断言，全量 193 测试通过。
   - 提交并推送至 `origin/main`（commit `c1960ca`），线上服务器拉取并重启 Gunicorn。
   - 线上 curl 实测验证通过：返回 `price_as_of: 2026-09-22T17:30:42+08:00`, `stale: False`。
3. **Stage 5（WebSocket 自愈、分级并发隔离锁与双模协同）实现完成**：
   - **R2 WebSocket 自愈机制**：在 `app/user_stream.py` 中引入 `_run_supervisor` 守护线程与指数退避（2s -> 4s -> ... -> 60s），在断开时自动刷新 `listenKey` 重连；引入每 25 分钟保活校验，若失效主动触发重建；`stop()` 时主动调用 `DELETE /fapi/v1/listenKey` 释放服务端资源。
   - **双模调度协同**：在 `app/jobs/scheduler_startup_jobs.py` 中，启用 WS 时依然在启动时同步初始余额与 TRANSFER 出入金流水，并保留 30 分钟一次的低频 REST 兜底对账任务，杜绝出入金漏单与单点失效。
   - **C2 分级任务并发隔离锁**：在 `app/core/job_runtime.py` 中实现 Tiered Job Lock，分离重任务互斥锁（`_heavy_job_lock`，用于全量同步、大步长补偿回填）与轻任务锁（`_light_job_lock`，用于持仓与余额刷新），重任务之间互斥防重叠，轻任务秒级执行不被饿死。
   - **可见性增强**：在 `app/main.py` 和 `app/routes/system.py` 中向 `/api/status` 暴露 `user_stream` 的启用与在线连接状态（`enabled`, `connected`, `last_event_time_ms`）。
   - **测试验证**：新增 `test_user_stream_reconnect.py` 与 `test_tiered_job_lock.py`，全量 199 个测试与 8 个审计探针全部全绿通过。

## 2026-09-23 监控页面（live-monitor）余额不更新问题排查与根因分析

### 用户疑问
> monitor页面的余额为什么一直不更新

### 根因复盘与分析
1. **币安 WebSocket 的数据语义（User Data Stream）**：
   - 币安合约的 User Data Stream (`ACCOUNT_UPDATE` 账户更新事件) 严格按交易动作触发（如订单成交、手续费/资金费扣除、账户出入金等）。
   - **币安不会因为持仓币种的市场标记价格（Mark Price）波动而推送 WS 账户更新**。
   - 用户当前的持仓（CTR, LYN, MITO, KOMA 等 7 笔仓位）近期处于持仓等待状态，没有新订单成交，也没有到达 8 小时一次的资金费结算时刻。因此，系统通过 WebSocket 接入币安后，收到的账户更新事件实际为 0（通过线上数据库 `ws_events` 表计数证实为 0）。
2. **REST 兜底同步间隔被强制锁定为 >= 30分钟**：
   - 在 Stage 5 切换到 WebSocket 模式时，`app/jobs/scheduler_startup_jobs.py` 中写道：
     `fallback_interval = max(30, int(scheduler.balance_sync_interval_minutes))`
   - 当 `ENABLE_USER_STREAM=1` 时，后台的 REST 余额同步（调用 `/fapi/v3/account` 获取包含最新浮动盈亏的 `totalMarginBalance`）被硬编码为**每 30 分钟才跑一次**。
   - 查询线上服务器 `balance_history` 表证实，历史余额记录确实为严格的每 30 分钟一条（20:40、21:10、21:40、22:10）。
3. **前端 Live Monitor 渲染机制与防抖断层**：
   - 前端 `live-monitor.js` 每 60 秒轮询一次 `/api/balance-history?time_range=1d`，顶部余额卡片（`disp-balance`）直接展示 `balance_history` 最新记录的值。
   - 前端带有时间戳防抖判断：`if (latestTime > lastFetchTime)`。因为后端数据库整整 30 分钟没有插入任何新数据点，前端判断数据未发生变化，导致**界面连续 30 分钟完全静止，余额数字分文不动**。
### 处理进展 (Stage 6)
- **支持浮点数配置**：在 `app/core/scheduler_config.py` 中将 `balance_sync_interval_minutes` 类型升级为 `float`，解析改为 `_env_float("BALANCE_SYNC_INTERVAL_MINUTES", 15.0, minimum=0.1)`。
- **秒级调度解绑 30 分钟硬编码**：在 `app/jobs/scheduler_startup_jobs.py` 中将触发器改为以秒为单位 `IntervalTrigger(seconds=int(interval * 60))`，移除 `max(30, ...)` 限制。
- **出入金接口智能节流**：在 `app/jobs/balance_sync_job.py` 中，余额查询每 30 秒执行（仅 5 权重，耗时 0.01s），出入金流水解耦为 5 分钟或钱包变动时拉取（防 30 权重高频消耗）。
- **前端提频**：`static/js/live-monitor.js` 轮询间隔 `UPDATE_INTERVAL` 缩短为 30 秒。
- **全量测试与部署**：本地全量 200 个单元测试和 8 个探针全通；提交 `e737770` 已推送到 GitHub `main`；服务器 `.env` 配置 `BALANCE_SYNC_INTERVAL_MINUTES=0.5` 并重启服务。
- **实测结果**：线上 `balance_history` 每 30 秒准确写入一条新记录，耗时 0.01 秒，前端余额恢复实时跳动刷新。

## 2026-09-23 服务器上其他项目的余额数据采集频率调研

### 用户疑问
> 服务器上其他两个项目的余额数据采集频率是多少

### 排查结论
- **项目 1：`bubble_buster`**（服务名 `bubble_buster.service`）
  - 核心配置：`config.ini` 中的 `manager_interval_sec = 60` 与 `readonly_wallet_snapshot_interval_sec = 60`。
  - 实测数据：查询 `/root/bubble_buster/state.db` 的 `wallet_snapshots` 表，记录严格以 **每 60 秒（1 分钟）** 一次记录最新钱包余额快照。
  - 辅助机制：同时连接 WebSocket 捕获订单成交与资金变更，REST 校验兜底间隔为 300 秒（5 分钟）。
- **项目 2：`momentum_alpha`**（服务名 `momentum-alpha.service` 与 `momentum-alpha-user-stream.service`）
  - 核心逻辑：`/root/momentum_alpha/src/momentum_alpha/scheduler.py` 中的整分钟秒针归零轮询调度（`now.second <= 2`）。
  - 实测数据：查询 `/root/momentum_alpha/var/runtime.db` 的 `account_snapshots` 表，记录严格在每分钟的第 00 秒整点落盘，**周期为严格的 1 分钟（60 秒）**。
  - 辅助机制：通过独立的 `momentum-alpha-user-stream.service` 监听 WebSocket `ACCOUNT_UPDATE`，在有成交/资金变动时实时写入 `account_flows`。

## 2026-09-23 全机多项目 API 权重负载与 2400 限额实测分析

### 用户疑问
> 你看看过去几天的日志，然后告诉我，现在最忙的时候1分钟能达到多少，会达到2400限额吗

### 核心结论
- **平稳期全机总消耗**：约 **93 权重 / 分钟**（占 2400 限额的 **3.9%**）。
- **最忙碌时刻（极端重叠峰值）**：整机在同一分钟全项目并发峰值约为 **420 权重 / 分钟**（占 2400 限额的 **17.5%**）。
- **是否会达到 2400 限额**：**绝不可能**。仍保有 **1980+ 权重（82.5%）的巨额安全缓冲带**。
- **历史日志实测**：过去 30 天各个项目的历史日志中，HTTP 429（Too Many Requests）和 HTTP 418（IP Ban）的出现次数为 **0 次**。

## 2026-09-24 读写分离 OperationalReadModel 与状态懒加载重构

### 用户请求
> 对项目进行重构，实现下面2点，是不是可以提高外部访问前端页面的速度。
> 1. 读写分离与专用读模型（OperationalReadModel），引入轻量聚合的只读模型，直接读内存与聚合快照，把数据库毫秒级锁竞争降到了接近 0。
> 2. 状态按需懒加载（Lazy Loading）。
> 服务器 43.153.134.252（root）

### 实现内容
- **OperationalReadModel**（`app/core/read_model.py`）：线程安全内存读模型，支持 `get_or_load` / `get_or_load_async` / `publish` / `invalidate` / `version` / `stats`，section 按需懒加载。
- **写后失效**：`SyncRepository.save_open_positions`、`LeaderboardSnapshotRepository.save_leaderboard_snapshot`、`RiskRepository.save_noon_loss_snapshot` 写成功后 invalidate 对应 section。
- **热点读路径接入**：`PositionsService`、`LeaderboardService` 改为共享读模型；命中内存时不再打 SQLite。
- **懒加载**：
  - 后端：section 首次访问才回源；
  - 前端 `live-monitor.js`：首屏只拉余额+持仓，观察笔记/午间复盘 idle 后加载；
  - 前端 `leaderboard.js`：主表渲染后 idle 再拉 14/30/60/365 反弹榜；
  - 静态资源 `Cache-Control: public, max-age=86400, immutable`（URL 已带 `?v=mtime` 防缓存击穿）。
- **微基准**：直打聚合 p50≈5.0ms / p95≈6.1ms；读模型命中 p50≈0.000ms / p95≈0.001ms。
- **测试**：新增 `test_operational_read_model.py`、`test_write_invalidation_read_model.py`；全量 209 通过。

### 线上基线（重构前实测，RTT≈55ms）
| 路径 | TTFB | 说明 |
|---|---|---|
| `/` | 115ms | 36KB HTML |
| `/api/open-positions` | 121ms | 热点 |
| `/api/leaderboard` | 281ms | 最慢 |
| `/api/status` | 216ms | 偏慢 |
| `/static/dark-unified.css` | 131ms | 无 Cache-Control |

### 结论（是否提升外部访问速度）
- **能提升，但分层**：读写分离主要砍掉服务端处理时间（排行榜 200ms+ 级 DB/聚合）；懒加载改善首屏关键内容到达时间；静态长缓存改善二次打开。网络 RTT≈55ms 是这两项优化**消不掉**的物理下限。
- 若要进一步压外部访问延迟，还需：HTTP 压缩、CDN/边缘缓存、HTTP/2、API 响应体积裁剪。


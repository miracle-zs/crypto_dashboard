# REST API 规格参考手册 (API Reference)

本文档整理了系统全部对外暴露的 REST API 端点、请求方式、参数规格、鉴权机制以及响应结构。

---

## 一、鉴权说明

对于**读接口 (GET)**，系统默认免鉴权，供仪表盘快速获取监控与图表数据。
对于**写操作接口 (POST / DELETE)**，系统通过 `require_admin_token` 实施强制校验：
- **请求头**：`X-Admin-Token: <token>`
- **Query 参数 (备用)**：`?admin_token=<token>`
- 配置来源：环境变量 `DASHBOARD_ADMIN_TOKEN`。未配置或校验失败返回 `403 Forbidden`。

---

## 二、交易与战绩分析接口 (Trades Domain)

### 1. `GET /api/summary`
获取交易全局量化指标与汇总统计。
- **参数**: 无
- **响应模型**: `TradeSummary`
- **主要返回字段**:
  - `total_pnl`: 已记录平仓交易的盈亏之和（USDT），资金费单独统计；历史交易可能采用旧费用口径
  - `win_rate`: 胜率 (%)
  - `win_count` / `loss_count` / `total_trades`: 盈亏与总笔数
  - `equity_curve`: 累计净值曲线数组 `[float]`
  - `profit_factor`: 盈利因子
  - `kelly_criterion`: 凯利公式仓位建议比例
  - `sqn`: 系统质量数 (System Quality Number)
  - `max_drawdown`: 最大回撤
  - `current_streak` / `best_win_streak` / `worst_loss_streak`: 连胜/连败记录

### 2. `GET /api/trades`
分页查询已闭合交易明细。
- **参数**:
  - `limit` (Query, int, 可选): 单页返回条数 (1 ~ 5000)
  - `offset` (Query, int, 默认 0): 分页偏移量
- **响应模型**: `list[Trade]`
- **主要返回字段**: 每笔交易的 `symbol`, `side`, `entry_time`, `exit_time`, `entry_price`, `exit_price`, `qty`, `fees`, `pnl_net`, `return_rate`, `holding_time`, `entry_order_id`, `exit_order_id`。

### `GET /api/funding-summary`
独立汇总已记录的 USDT 资金费流水，不分配到订单。返回 `amount`、`record_count`、`first_record_ms` 和 `last_record_ms`；无记录时 `amount` 为 `null`。首末记录时间不代表该区间已完整采集。历史交易可能已经分摊过资金费，不能直接将该数与历史交易盈亏相加。

### 3. `GET /api/daily-stats`
获取按自然日统计的每日战绩汇总。
- **参数**: 无
- **响应模型**: `list[DailyStats]`
- **返回字段**: `date`, `trade_count`, `total_amount`, `total_pnl`, `win_count`, `loss_count`, `win_rate`。

### 4. `GET /api/trades-aggregates`
获取交易深度图表聚合数据。
- **参数**:
  - `window` (Query, str, 默认 `"all"`): 聚合窗口时间，可选 `"all"`、`"7d"`、`"30d"`。
- **响应模型**: `TradeAggregatesResponse`
- **返回字段**:
  - `duration_buckets`: 持仓时长分桶统计（如 `<15m`, `15m-1h`, `1h-4h` 等的交易笔数与盈亏）
  - `duration_points`: 散点图数据集（持仓时长 x 对应净盈亏 y）
  - `hourly_pnl`: 24小时各时段的累计盈亏分布数组
  - `symbol_rank`: 盈利榜 (`winners`) 与亏损榜 (`losers`) 标的统计

### 5. `GET /api/monthly-progress`
获取当前月度目标达成进度。
- **响应示例**:
  ```json
  {
    "target": 30000.0,
    "current_pnl": 12560.8,
    "progress_pct": 41.87
  }
  ```

### 6. `POST /api/monthly-target` 🔒 (需鉴权)
设置自定义月度盈利目标。
- **参数**:
  - `target` (Query, float, 必填): 目标金额，需大于 0
- **响应**: `{"message": "月度目标已更新", "target": 30000.0}`

### 7. `POST /api/positions/set-long-term` 🔒 (需鉴权)
标记或取消仓位的“长期持有”状态（标记后将免受短线止损熔断干预）。
- **参数**:
  - `symbol` (Query, str, 必填): 标的符号
  - `order_id` (Query, int, 必填): 开仓订单号
  - `is_long_term` (Query, bool, 必填): 是否长期持有
- **响应**: `{"status": "success", "symbol": "BTCUSDT", "is_long_term": true}`

---

## 三、实时持仓与资金监控接口 (Positions & Balance Domain)

### 1. `GET /api/open-positions`
获取当前活跃持仓与账户风险全景。支持版本感知以实现增量下发与极低网络开销。
- **参数**:
  - `since_version` (Query, int, 可选): 客户端当前持有的数据版本号。若服务端版本一致则返回 `changed: false`。
- **响应模型**: `OpenPositionsResponse`
- **主要返回字段**:
  - `version`: 当前数据集版本号
  - `as_of` / `price_as_of`: 响应生成时间与当前持仓中最早的可用价格时间；价格时间不代表仓位更新时间
  - `stale`: 任一持仓价格缺失、没有时间或超过 15 分钟时为 `true`
  - `positions`: 持仓列表，包含 `symbol`, `side`, `qty`, `entry_price`, `mark_price`, `price_as_of`, `price_stale`, `unrealized_pnl`, `notional`, `holding_time`, `is_long_term`
  - `summary`:
    - `total_notional`: 总名义持仓金额
    - `net_exposure`: 净敞口风险 (Long - Short)
    - `concentration_top1` / `concentration_top3`: 头部仓位集中度
    - `recent_loss_count` / `recent_loss_total_if_stop_now`: 11:50 午间浮亏快照统计
    - `noon_review_*`: 23:02 夜间复盘对比统计

### 2. `GET /api/balance-history`
获取账户历史资金曲线。内置 SQLite 窗口函数下采样，针对大周期自动保留 1000 个平滑点。
- **参数**:
  - `time_range` (Query, str, 默认 `"1d"`): 回溯范围，支持 `"1h"`, `"1d"`, `"1w"`, `"1m"`, `"1y"`。
- **响应模型**: `list[BalanceHistoryItem]`
- **返回字段**: `time` (毫秒时间戳), `value` (保证金余额), `cumulative_equity` (累计净收益), `transfer_amount` (出入金变动)。

---

## 四、市场快照与反弹榜单接口 (Market Domain)

### 1. `GET /api/leaderboard`
获取每日 07:00 生成的全市场 24H 涨跌幅榜快照。
- **参数**:
  - `date` (Query, str, 可选): 指定快照日期，格式 `YYYY-MM-DD`。缺省返回最新快照。
- **响应模型**: `LeaderboardSnapshotResponse`
- **返回字段**: `snapshot_date`, `snapshot_time`, `rows` (涨幅榜明细), `losers_rows` (跌幅榜明细), `continuation_pool` (延续池)。

### 2. `GET /api/leaderboard/dates`
获取所有可用的涨跌幅榜历史快照日期列表。
- **参数**: `limit` (Query, int, 默认 90)
- **响应模型**: `SnapshotDatesResponse` (`dates: ["2026-10-01", ...]`)

### 3. `GET /api/leaderboard/metrics-history`
获取涨幅榜次日动量表现与延续性历史数据。
- **参数**: `limit` (Query, int, 默认 60)
- **响应模型**: `MetricsHistoryResponse`

### 4. `GET /api/rebound-{14d/30d/60d/365d}`
获取指定周期的反弹榜快照（本地日线计算）。
- **可用端点**:
  - `/api/rebound-7d` (对应 14D 反弹榜)
  - `/api/rebound-30d`
  - `/api/rebound-60d`
  - `/api/rebound-365d`
- **参数**: `date` (Query, str, 可选): `YYYY-MM-DD`
- **响应模型**: `ReboundSnapshotResponse`
- **返回字段**: `snapshot_date`, `rows` (含 `symbol`, `current_price`, `low_price`, `rebound_pct`, `low_at`)。

### 5. `GET /api/rebound-{14d/30d/60d/365d}/dates`
获取各周期反弹榜可供回溯的快照日期清单。
- **参数**: `limit` (Query, int, 默认 90)

---

## 五、暴跌/插针风险雷达接口 (Crash Risk Domain)

### 1. `GET /api/crash-risk`
获取最新的暴跌/插针风险雷达快照与评分。
- **响应模型**: `CrashRiskResponse`
- **返回字段**:
  - `as_of`: 计算时间
  - `status`: `complete` 或 `partial`（行情不足）
  - `summary`: `total`, `high_risk` (高危数), `warning` (警惕数), `watch` (观察数), `unknown` (数据不足数)
  - `rows`: 逐标的评分，包含 `symbol`, `risk_score` (0~100，数据不足时为 `null`), `stage`（高危 / 警惕 / 观察 / 数据不足）、`drivers` 和 `component_scores`

### 2. `POST /api/crash-risk/refresh`
手动触发基于当前涨幅榜快照重新计算暴跌风险。
- **响应模型**: `CrashRiskResponse`
- 若有标的数据不足，返回 `partial` 并保留此前保存的完整快照。

---

## 六、自选观察与备忘接口 (Watch Notes Domain)

### 1. `GET /api/watch-notes`
获取用户关注的重点观察币种清单。
- **参数**: `limit` (Query, int, 默认 200)
- **响应**: `[{"id": 1, "symbol": "BTCUSDT", "noted_at": "..."}]`

### 2. `POST /api/watch-notes` 🔒 (需鉴权)
添加新的重点观察币种。
- **参数**: `symbol` (Query, str, 必填): 标的名称
- **响应**: `{"id": 1, "symbol": "BTCUSDT", "created": true}`

### 3. `DELETE /api/watch-notes/{note_id}` 🔒 (需鉴权)
移除已关注的观察币种。
- **参数**: `note_id` (Path, int, 必填): 记录 ID
- **响应**: `{"message": "已删除", "id": 1}`

---

## 七、系统运行、日志与风控复盘接口 (System Domain)

### 1. `GET /api/status`
系统综合健康度与就绪状态检查（探针常用）。
- **返回字段**:
  - `readiness`: 系统就绪状态 (`healthy` / `degraded`)
  - `scheduler`: 调度器是否正在运行
  - `user_stream`: WebSocket 连接状态与最后事件时间戳
  - `database`: 数据库连接状态与总数据量统计

### 2. `GET /api/logs`
获取实时系统运行日志。
- **参数**: `lines` (Query, int, 默认 200, 范围 1 ~ 5000)
- **响应模型**: `LogsResponse` (`logs: [string]`)

### 3. `GET /api/noon-loss-review-history`
获取近 N 天“午间浮亏建议止损 vs 夜间实际抗单结果”历史对比矩阵。
- **参数**: `limit` (Query, int, 默认 7, 范围 1 ~ 90)
- **响应模型**: `NoonLossReviewHistoryResponse`
- **返回字段**:
  - `rows`: 每日对比记录，含 `snapshot_date`, `noon_cut_loss_total`, `hold_loss_total`, `delta_loss_total`, `rows` (逐币明细)
  - `summary`: 累计差值统计

### 4. `GET /api/sync-runs`
获取后台数据同步作业的执行流水与耗时审计记录。
- **参数**: `limit` (Query, int, 默认 100)
- **响应模型**: `SyncRunsResponse`

### 5. `POST /api/sync/manual` 🔒 (需鉴权)
手动向后台调度器提交一次异步全量数据同步与撮合计算。
- **响应**: `{"message": "手动同步已触发", "status": "started"}`

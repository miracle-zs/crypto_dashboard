# 数据字典 (Data Dictionary)

本文档详细定义了系统中 SQLite 数据库 (`data/trades.db`) 的全部 27 张持久化表结构、字段含义、主键约束与索引规划。

系统数据底座采用“**事实先于结果 (Facts vs Projections)**”的第一性原理架构：
- **事实层 (Facts)**：不可变的原始业务事实（逐笔成交、资金流水、日线行情、账户快照、WebSocket 事件）。
- **投影层 (Projections)**：可重放、可重算的业务视图（开仓批次、已平仓交易、汇总指标、多维快照）。
- **控制层 (Ingestion & Runtime)**：端点游标、任务流水与运行状态。

---

## 一、第一性原理事实层 (Facts)

### 1. `execution_facts` (交易所逐笔成交事实表)
记录币安 REST API 或 User Stream 返回的不可篡改逐笔成交事实。
- **主键/约束**: `id INTEGER PRIMARY KEY AUTOINCREMENT`，`UNIQUE(venue, symbol, trade_id)`
- **索引**: `idx_execution_facts_time(time_ms)`, `idx_execution_facts_symbol_time(symbol, time_ms)`

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `id` | INTEGER | 自增自编号 |
| `venue` | TEXT | 交易场所，默认 `'binance_futures'` |
| `symbol` | TEXT | 交易对符号（大写，如 `BTCUSDT`） |
| `trade_id` | INTEGER | 币安官方逐笔成交 ID (`tradeId`) |
| `order_id` | INTEGER | 关联订单 ID (`orderId`) |
| `side` | TEXT | 买卖方向：`BUY` / `SELL` |
| `position_side` | TEXT | 持仓方向：`BOTH` / `LONG` / `SHORT` |
| `price` | REAL | 成交价格 |
| `qty` | REAL | 成交数量 |
| `realized_pnl` | REAL | 交易所官方报告已实现盈亏 |
| `quote_qty` | REAL | 成交名义金额（USDT） |
| `commission` | REAL | 手续费数值 |
| `commission_asset` | TEXT | 手续费扣除资产，默认 `'USDT'` |
| `time_ms` | INTEGER | 成交时间戳（毫秒，UTC） |
| `is_buyer` | INTEGER | 是否买方：1=是，0=否 |
| `is_maker` | INTEGER | 是否挂单方（Maker）：1=是，0=否 |
| `raw_payload` | TEXT | 原始 JSON 响应报文（用于审计回溯） |
| `created_at` | TIMESTAMP | 本地写入时间 |

---

### 2. `income_facts` (合约资金流水事实表)
记录币安官方资金流水 (`/fapi/v1/income`)，包括平仓盈亏、手续费、资金费、强平结清等。
- **主键/约束**: `id INTEGER PRIMARY KEY AUTOINCREMENT`，`UNIQUE(venue, income_type, tran_id)`
- **索引**: `idx_income_facts_time(time_ms)`, `idx_income_facts_symbol_time(symbol, time_ms)`

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `id` | INTEGER | 自增编号 |
| `venue` | TEXT | 交易场所，默认 `'binance_futures'` |
| `symbol` | TEXT | 交易对（出入金时可能为空） |
| `income_type` | TEXT | 流水类型：`TRANSFER` / `REALIZED_PNL` / `FUNDING_FEE` / `COMMISSION` / `INSURANCE_CLEAR` |
| `income` | REAL | 金额（带正负符号） |
| `asset` | TEXT | 资产币种（如 `USDT`） |
| `time_ms` | INTEGER | 发生时间戳（毫秒，UTC） |
| `tran_id` | INTEGER | 币安官方资金流水交易号 (`tranId`) |
| `trade_id` | INTEGER | 关联成交 ID（若有） |
| `info` | TEXT | 附加说明信息 |
| `created_at` | TIMESTAMP | 本地写入时间 |

---

### 3. `daily_klines` (共享全市场日线行情事实表)
每日 06:15 增量同步与回填的日 K 线本地缓存，全系统统一复用。
- **主键/约束**: `PRIMARY KEY(symbol, open_time)`
- **索引**: `idx_daily_klines_open_time(open_time DESC)`, `idx_daily_klines_symbol_closed(symbol, is_closed, open_time DESC)`

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `symbol` | TEXT | 交易对符号 |
| `open_time` | INTEGER | K 线开盘时间戳（毫秒，UTC） |
| `open` | REAL | 开盘价 |
| `high` | REAL | 最高价 |
| `low` | REAL | 最低价 |
| `close` | REAL | 收盘价 |
| `is_closed` | INTEGER | 该 K 线是否已完结闭合（1=已完结，0=进行中） |
| `updated_at` | TIMESTAMP | 本地更新时间 |

---

### 4. `position_snapshots` (账户权威持仓快照表)
定时从币安 `/fapi/v3/positionRisk` 采集的权威仓位物理快照。
- **主键/约束**: `id INTEGER PRIMARY KEY AUTOINCREMENT`，`UNIQUE(snapshot_time, symbol, position_side)`
- **索引**: `idx_position_snapshots_time(snapshot_time DESC)`, `idx_position_snapshots_symbol(symbol)`

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `id` | INTEGER | 自增编号 |
| `snapshot_time` | TEXT | 快照时间（ISO 字符串） |
| `symbol` | TEXT | 交易对符号 |
| `position_side` | TEXT | 持仓模式方向：`BOTH` / `LONG` / `SHORT` |
| `qty` | REAL | 实际持仓数量（绝对值） |
| `entry_price` | REAL | 交易所计算的持仓均价 |
| `mark_price` | REAL | 采集时刻的市场标记价格 |
| `unrealized_pnl` | REAL | 未实现浮动盈亏（USDT） |
| `liquidation_price`| REAL | 强平价格 |
| `leverage` | INTEGER | 当前杠杆倍数 |
| `margin_type` | TEXT | 保证金模式：`isolated` (逐仓) / `cross` (全仓) |
| `is_complete` | INTEGER | 快照是否完整采集（1=完整，0=不完整） |
| `created_at` | TIMESTAMP | 本地创建时间 |

---

### 5. `transfers` (充提与外部划转流水表)
由资金流水中 `TRANSFER` 类型解析提取的外部出入金事件。
- **主键/约束**: `id INTEGER PRIMARY KEY AUTOINCREMENT`

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `id` | INTEGER | 自增编号 |
| `timestamp` | TIMESTAMP | 发生时间 |
| `amount` | REAL | 划转金额（正数为入金，负数为出金） |
| `type` | TEXT | 划转分类，默认 `'auto'` |
| `description` | TEXT | 业务说明 |
| `event_time` | INTEGER | 毫秒时间戳 |
| `asset` | TEXT | 划转资产，如 `USDT` |
| `income_type` | TEXT | 原始资金流水类型 |
| `source_uid` | TEXT | 唯一来源标识号 |

---

### 6. `ws_events` (WebSocket 事件原始日志表)
记录 User Data Stream 接收到的原始事件报文，用于离线回放与审计。
- **主键/约束**: `id INTEGER PRIMARY KEY AUTOINCREMENT`

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `id` | INTEGER | 自增编号 |
| `event_type` | TEXT | 事件类型，如 `ACCOUNT_UPDATE`, `ORDER_TRADE_UPDATE` |
| `event_time` | INTEGER | 事件发生毫秒时间戳 |
| `payload` | TEXT | 原始 JSON 字符串 |
| `created_at` | TIMESTAMP | 接收记录时间 |

---

## 二、状态化计算与投影层 (Projections)

### 7. `open_lots` (开仓未平批次表)
状态化撮合引擎计算的核心投影。记录每个未完全平仓的入场单批次及剩余待平数量。
- **主键/约束**: `id INTEGER PRIMARY KEY AUTOINCREMENT`
- **索引**: `idx_open_lots_symbol_pos(symbol, position_side)`, `idx_open_lots_time(time_ms ASC)`

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `id` | INTEGER | 自增编号 |
| `symbol` | TEXT | 交易对符号 |
| `position_side` | TEXT | `BOTH` / `LONG` / `SHORT` |
| `side` | TEXT | 入场动作：`BUY` / `SELL` |
| `order_id` | INTEGER | 开仓入场订单 ID |
| `price` | REAL | 开仓成交均价 |
| `qty` | REAL | 原始开仓总数量 |
| `remaining_qty` | REAL | 当前尚未平仓的剩余数量 |
| `time_ms` | INTEGER | 入场成交时间戳（毫秒） |
| `fee_rate` | REAL | 记录时费率估算参数 |
| `created_at` | TIMESTAMP | 批次创建时间 |

---

### 8. `trades` (已闭合交易结果表)
完整撮合平仓后的交易记录，作为战绩归因、胜率统计和报表展示的权威来源。
- **主键/约束**: `id INTEGER PRIMARY KEY AUTOINCREMENT`，`UNIQUE(symbol, entry_order_id, exit_order_id)`

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `id` | INTEGER | 自增编号 |
| `no` | INTEGER | 交易序号（倒序展示序列） |
| `date` | TEXT | 平仓日期（格式 `YYYY-MM-DD`） |
| `entry_time` | TEXT | 开仓时间（格式 `YYYY-MM-DD HH:MM:SS`，UTC+8） |
| `exit_time` | TEXT | 平仓时间（格式 `YYYY-MM-DD HH:MM:SS`，UTC+8） |
| `holding_time` | TEXT | 持仓时长可读文本（如 `1h 25m`） |
| `symbol` | TEXT | 交易对符号（去 `USDT` 后缀） |
| `side` | TEXT | 开仓方向：`LONG` / `SHORT` |
| `price_change_pct`| REAL | 标的价格波动百分比 |
| `entry_amount` | REAL | 入场名义金额（USDT） |
| `entry_price` | REAL | 开仓均价 |
| `exit_price` | REAL | 平仓均价 |
| `qty` | REAL | 撮合成交数量 |
| `fees` | REAL | 本笔交易承担的手续费与资金费总和 |
| `pnl_net` | REAL | 扣除所有费用后的纯净盈亏（USDT） |
| `close_type` | TEXT | 平仓类型标记 |
| `return_rate` | TEXT | 收益率百分比字符串（如 `+12.50%`） |
| `open_price` | REAL | 开仓当日 00:00 UTC 的基准开盘价 |
| `pnl_before_fees`| REAL | 未扣除手续费的毛盈亏 |
| `entry_order_id`| INTEGER | 开仓订单号 |
| `exit_order_id` | TEXT | 平仓订单号（可能由多笔合并时用逗号拼接） |
| `created_at` | TIMESTAMP | 记录写入时间 |
| `updated_at` | TIMESTAMP | 记录更新时间 |

---

### 9. `open_positions` (当前持仓展示表)
用于仪表盘实时持仓页面展示的兼容聚合表。
- **主键/约束**: `id INTEGER PRIMARY KEY AUTOINCREMENT`，`UNIQUE(symbol, order_id)`
- **索引**: `idx_open_positions_entry_time(entry_time DESC)`, `idx_open_positions_symbol(symbol)`, `idx_open_positions_profit_alerted_entry(profit_alerted, entry_time DESC)`

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `id` | INTEGER | 自增编号 |
| `date` | TEXT | 开仓日期 |
| `symbol` | TEXT | 交易对符号（去 `USDT` 后缀） |
| `side` | TEXT | 持仓方向：`LONG` / `SHORT` |
| `entry_time` | TEXT | 入场时间（UTC+8 格式） |
| `entry_price` | REAL | 入场价格 |
| `qty` | REAL | 持仓数量 |
| `entry_amount` | REAL | 占用名义本金 |
| `order_id` | INTEGER | 入场订单号 |
| `alerted` | INTEGER | 是否已触发僵尸仓位（超时）预警 |
| `last_alert_time` | TIMESTAMP | 最近预警时间 |
| `is_long_term` | INTEGER | 是否标记为长期策略持有（免受短线止损熔断干预） |
| `profit_alerted` | INTEGER | 是否已触发大幅浮盈（如 +20%）止盈提醒 |
| `profit_alert_time` | TIMESTAMP | 浮盈提醒时间 |
| `reentry_alerted`| INTEGER | 再次入场提示标记 |
| `reentry_alert_time` | TIMESTAMP | 再次入场提示时间 |
| `is_incomplete` | INTEGER | 是否因历史数据不全导致的缺失标记 |
| `created_at` | TIMESTAMP | 创建时间 |

---

### 10. `balance_history` (账户余额时序表)
记录每次同步时账户的总保证金余额与钱包余额，支持历史权益曲线与下采样。
- **主键/约束**: `id INTEGER PRIMARY KEY AUTOINCREMENT`

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `id` | INTEGER | 自增编号 |
| `timestamp` | TIMESTAMP | 记录时间戳 |
| `balance` | REAL | 账户总保证金余额 (`totalMarginBalance`) |
| `wallet_balance` | REAL | 账户钱包余额 (`totalWalletBalance`) |

---

### 11. `trade_summary` (全局指标汇总缓存表)
单行单例表 (`id=1`)，保存交易全局统计指标，避免每次访问主页全表重新聚合。
- **主键/约束**: `id INTEGER PRIMARY KEY CHECK (id = 1)`

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `id` | INTEGER | 固定为 1 |
| `total_pnl` | REAL | 累计净盈亏 |
| `total_fees` | REAL | 累计支付手续费与资金费 |
| `win_rate` | REAL | 胜率（百分比） |
| `win_count` | INTEGER | 盈利笔数 |
| `loss_count` | INTEGER | 亏损笔数 |
| `total_trades` | INTEGER | 总交易笔数 |
| `equity_curve` | TEXT | 累计净权益曲线数组 JSON 字符串 |
| `current_streak` | INTEGER | 当前连赢/连输状态（正为连胜，负为连败） |
| `best_win_streak`| INTEGER | 历史最佳连胜笔数 |
| `worst_loss_streak` | INTEGER | 历史最长连败笔数 |
| `max_drawdown` | REAL | 历史最大回撤金额/百分比 |
| `max_single_loss`| REAL | 最大单笔亏损金额 |
| `profit_factor` | REAL | 盈利因子（毛利 / 毛亏） |
| `kelly_criterion`| REAL | 凯利公式最优仓位比率建议 |
| `sqn` | REAL | 系统质量数 (System Quality Number) |
| `expected_value` | REAL | 期望收益值 |
| `risk_reward_ratio` | REAL | 盈亏比 (Avg Win / Avg Loss) |
| `updated_at` | TIMESTAMP | 缓存更新时间 |

---

### 12. `trade_aggregates_cache` (深度图表聚合缓存表)
单行单例表 (`id=1`)，缓存持仓时间分布、散点图点集、24小时盈亏热力分布与币种红黑榜。
- **主键/约束**: `id INTEGER PRIMARY KEY CHECK (id = 1)`

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `id` | INTEGER | 固定为 1 |
| `trades_count` | INTEGER | 缓存计算时对应的总笔数 |
| `latest_trade_updated_at` | TEXT | 缓存所涵盖的最新交易平仓时间 |
| `payload_json` | TEXT | 序列化聚合大 JSON (`duration_buckets`, `duration_points`, `hourly_pnl`, `symbol_rank`) |
| `updated_at` | TIMESTAMP | 更新时间 |

---

### 13. `user_settings` (用户偏好设置表)
单行单例表 (`id=1`)，持久化用户自定义的业务指标目标。
- **主键/约束**: `id INTEGER PRIMARY KEY CHECK (id = 1)`

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `id` | INTEGER | 固定为 1 |
| `monthly_target` | REAL | 月度利润目标金额（默认 30,000 USDT） |
| `updated_at` | TIMESTAMP | 更新时间 |

---

### 14. `watch_notes` (关注币种观察备忘表)
用户在实盘监控台重点标记的观察币种清单。
- **主键/约束**: `id INTEGER PRIMARY KEY AUTOINCREMENT`

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `id` | INTEGER | 自增编号 |
| `symbol` | TEXT | 标的名称（如 `BTCUSDT`） |
| `noted_at` | TIMESTAMP | 标记时间 |
| `created_at` | TIMESTAMP | 记录写入时间 |

---

## 三、市场快照与风控复盘表 (Market & Risk Snapshots)

### 15. `leaderboard_snapshots` (每日涨跌幅榜快照表)
每日 07:00 (UTC+8) 由全市场 24H 行情 Ticker 生成的权威涨跌幅榜快照。
- **主键/约束**: `id INTEGER PRIMARY KEY AUTOINCREMENT`，`UNIQUE(snapshot_date)`
- **索引**: `idx_leaderboard_snapshot_date(snapshot_date DESC)`

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `id` | INTEGER | 自增编号 |
| `snapshot_date` | TEXT | 快照所属自然日 (`YYYY-MM-DD`) |
| `snapshot_time` | TEXT | 生成时间戳（UTC+8） |
| `window_start_utc` | TEXT | 计算窗口 UTC 起点 |
| `candidates` | INTEGER | 候选池总币种数 |
| `effective` | INTEGER | 过滤后有效币种数 |
| `top_count` | INTEGER | 入榜 Top N 数量 |
| `rows_json` | TEXT | 涨幅榜 Top 标的明细 JSON 字符串 |
| `losers_rows_json` | TEXT | 跌幅榜 Top 标的明细 JSON 字符串 |
| `all_rows_json` | TEXT | 全量有效候选币种完整数据 JSON |
| `created_at` | TIMESTAMP | 创建时间 |

---

### 16. `leaderboard_daily_metrics` (涨幅榜次日动量跟踪表)
每日追踪前日涨幅榜标的在次日或 48 小时内的表现（延续性、反转率、回撤深度等）。
- **主键/约束**: `id INTEGER PRIMARY KEY AUTOINCREMENT`，`UNIQUE(snapshot_date)`

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `id` | INTEGER | 自增编号 |
| `snapshot_date` | TEXT | 快照日期 (`YYYY-MM-DD`) |
| `metric1_json` | TEXT | 次日跌幅指标数据 JSON |
| `metric2_json` | TEXT | 延续池标的追踪 JSON |
| `metric3_json` | TEXT | 48小时动量表现 JSON |
| `created_at` | TIMESTAMP | 创建时间 |
| `updated_at` | TIMESTAMP | 更新时间 |

---

### 17~20. 多周期反弹榜快照表群 (`rebound_*_snapshots`)
每日 07:00 共享本地 `daily_klines`，纯本地无 REST 请求计算的多周期反弹快照。
包含四张同构表：
- `rebound_7d_snapshots`（注：历史沿用名称，实际业务语义为 **14D 反弹榜**）
- `rebound_30d_snapshots`
- `rebound_60d_snapshots`
- `rebound_365d_snapshots`

四张表具有完全一致的模式结构：
- **主键/约束**: `id INTEGER PRIMARY KEY AUTOINCREMENT`，`UNIQUE(snapshot_date)`
- **索引**: `idx_rebound_{WINDOW}_snapshot_time(snapshot_time DESC)`, `idx_rebound_{WINDOW}_snapshot_date(snapshot_date DESC)`

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `id` | INTEGER | 自增编号 |
| `snapshot_date` | TEXT | 快照日期 (`YYYY-MM-DD`) |
| `snapshot_time` | TEXT | 生成时间戳（UTC+8） |
| `window_start_utc` | TEXT | 周期回溯起始 UTC 时间 |
| `candidates` | INTEGER | 参与计算标的数 |
| `effective` | INTEGER | 满足成交额门槛标的数 |
| `top_count` | INTEGER | 入榜展示标的数 |
| `rows_json` | TEXT | 反弹榜 Top 标的明细 JSON（含反弹幅度、低点价格、低点时间等） |
| `all_rows_json` | TEXT | 全量候选标的反弹明细 JSON |
| `created_at` | TIMESTAMP | 创建时间 |

---

### 21. `crash_risk_snapshots` (暴跌/插针风险雷达快照表)
基于涨幅榜衍生计算的暴跌风险综合评估快照，用于高危标的风险预警。
- **主键/约束**: `id INTEGER PRIMARY KEY AUTOINCREMENT`，`UNIQUE(snapshot_date)`
- **索引**: `idx_crash_risk_snapshots_date(snapshot_date)`

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `id` | INTEGER | 自增编号 |
| `snapshot_date` | TEXT | 快照日期 (`YYYY-MM-DD`) |
| `snapshot_time` | TEXT | 生成时间戳（UTC+8） |
| `window_start_utc` | TEXT | 回溯窗口起点 |
| `payload_json` | TEXT | 包含总览摘要、风险阶段（高危/预警/观察）、各维度评分（价格偏离、OI异动、极端加速等）的大 JSON |
| `created_at` | TIMESTAMP | 创建时间 |

---

### 22. `noon_loss_snapshots` (每日午间浮亏快照表)
每日 11:50 (UTC+8) 自动记录，模拟“若此时将所有浮亏仓位全砍”的风控基线。
- **主键/约束**: `id INTEGER PRIMARY KEY AUTOINCREMENT`，`UNIQUE(snapshot_date)`
- **索引**: `idx_noon_loss_snapshot_date(snapshot_date DESC)`

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `id` | INTEGER | 自增编号 |
| `snapshot_date` | TEXT | 快照日期 (`YYYY-MM-DD`) |
| `snapshot_time` | TEXT | 记录时刻（如 `11:50:00`） |
| `loss_count` | INTEGER | 浮亏币种数量 |
| `total_stop_loss` | REAL | 预期全部止损损失总金额（USDT） |
| `pct_of_balance` | REAL | 占当前保证金余额百分比 |
| `balance` | REAL | 快照时的账户保证金余额 |
| `rows_json` | TEXT | 逐币种浮亏明细 JSON 列表 |
| `created_at` | TIMESTAMP | 记录时间 |
| `updated_at` | TIMESTAMP | 更新时间 |

---

### 23. `noon_loss_review_snapshots` (午间止损夜间复盘快照表)
每日 23:02 (UTC+8) 自动复盘：统计午间建议砍仓的标的若坚持持有至夜间的实际表现，精准评估执行纪律与磨损。
- **主键/约束**: `id INTEGER PRIMARY KEY AUTOINCREMENT`，`UNIQUE(snapshot_date)`
- **索引**: `idx_noon_loss_review_snapshot_date(snapshot_date DESC)`

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `id` | INTEGER | 自增编号 |
| `snapshot_date` | TEXT | 对应的午间快照日期 (`YYYY-MM-DD`) |
| `review_time` | TEXT | 复盘执行时刻（如 `23:02:00`） |
| `noon_loss_count` | INTEGER | 午间识别出的浮亏标的总数 |
| `not_cut_count` | INTEGER | 实盘未按纪律砍仓的标的数 |
| `noon_cut_loss_total`| REAL | 若在午间执行砍仓的基线损失总额 |
| `hold_loss_total` | REAL | 坚持持有至夜间的实际损失总额 |
| `delta_loss_total` | REAL | 盈亏差值 (`hold_loss_total - noon_cut_loss_total`，正数表示抗单多亏了，负数表示抗回来了) |
| `pct_of_balance` | REAL | 差值占账户余额比例 |
| `balance` | REAL | 夜间复盘时账户保证金余额 |
| `rows_json` | TEXT | 逐币种对比明细 JSON 列表（`Symbol / Noon Loss / Night Loss / Delta`） |
| `created_at` | TIMESTAMP | 创建时间 |
| `updated_at` | TIMESTAMP | 更新时间 |

---

## 四、采集状态、游标与流水审计表 (Ingestion & System)

### 24. `sync_cursors` (端点级增量游标表)
记录各个数据流和各个标的在增量同步时的水位线，实现高能效拉取与安全重叠。
- **主键/约束**: `PRIMARY KEY(stream, symbol)`
- **索引**: `idx_sync_cursors_updated_at(updated_at DESC)`

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `stream` | TEXT | 数据流类别：`income` / `orders` / `trades` |
| `symbol` | TEXT | 交易对符号（全局流时为空字符串 `''`） |
| `last_id` | INTEGER | 最近一次同步的官方最后单据 ID |
| `last_time_ms` | INTEGER | 最近一次同步的毫秒时间戳 |
| `updated_at` | TIMESTAMP | 游标推进时间 |

---

### 25. `symbol_sync_state` (币种历史同步状态表)
记录各交易对在历史回溯和增量扫描中的健康状态与错误信息。
- **主键/约束**: `symbol TEXT PRIMARY KEY`
- **索引**: `idx_symbol_sync_state_updated_at(updated_at DESC)`, `idx_symbol_sync_state_last_attempt(last_attempt_end_ms DESC)`

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `symbol` | TEXT | 交易对符号 |
| `last_success_end_ms`| INTEGER | 最近一次同步成功的覆盖截至时间戳（毫秒） |
| `last_attempt_end_ms`| INTEGER | 最近一次同步尝试的时间戳（毫秒） |
| `last_error` | TEXT | 最近一次同步的错误报文（成功时为空） |
| `updated_at` | TIMESTAMP | 更新时间 |

---

### 26. `sync_run_log` (同步作业执行流水审计表)
记录后台或手动触发的每一次同步作业执行明细、耗时与产出条数。
- **主键/约束**: `id INTEGER PRIMARY KEY AUTOINCREMENT`
- **索引**: `idx_sync_run_log_created_at(created_at DESC)`

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `id` | INTEGER | 自增流水号 |
| `run_type` | TEXT | 任务类型：`incremental` / `full` / `manual` / `compensation` |
| `mode` | TEXT | 运行模式标识 |
| `status` | TEXT | 执行状态：`success` / `error` |
| `symbol_count` | INTEGER | 本次扫描涉及标的数 |
| `rows_count` | INTEGER | 拉取原始记录行数 |
| `trades_saved` | INTEGER | 产生并保存的已闭合交易数 |
| `open_saved` | INTEGER | 产生并保存的开仓持仓数 |
| `elapsed_ms` | INTEGER | 执行耗时（毫秒） |
| `error_message` | TEXT | 异常错误信息 |
| `created_at` | TIMESTAMP | 任务开始记录时间 |

---

### 27. `sync_status` (系统同步全局状态表)
单行单例表 (`id=1`)，提供全局最近一次同步时间与就绪状态，供探针或快速健康检查读取。
- **主键/约束**: `id INTEGER PRIMARY KEY CHECK (id = 1)`

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `id` | INTEGER | 固定为 1 |
| `last_sync_time` | TIMESTAMP | 最近一次全局同步时间 |
| `last_entry_time` | TEXT | 最近一笔开仓交易入场时间 |
| `total_trades` | INTEGER | 库内已闭合交易总条数 |
| `status` | TEXT | 当前运行状态：`idle` / `running` / `error` |
| `error_message` | TEXT | 错误信息 |
| `updated_at` | TIMESTAMP | 更新时间 |

---

## 五、数据库索引汇总表

| 索引名称 | 目标表 | 索引列 | 用途 |
| :--- | :--- | :--- | :--- |
| `idx_execution_facts_time` | `execution_facts` | `time_ms` | 成交事实按时间范围查询 |
| `idx_execution_facts_symbol_time` | `execution_facts` | `symbol, time_ms` | 标的级逐笔重放与增量撮合 |
| `idx_income_facts_time` | `income_facts` | `time_ms` | 资金流水按时间快速检索 |
| `idx_income_facts_symbol_time` | `income_facts` | `symbol, time_ms` | 标的级手续费/资金费精确归因 |
| `idx_open_lots_symbol_pos` | `open_lots` | `symbol, position_side` | 快速检索标的当前在途开仓批次 |
| `idx_open_lots_time` | `open_lots` | `time_ms ASC` | FIFO 状态化匹配先进先出排序 |
| `idx_daily_klines_open_time` | `daily_klines` | `open_time DESC` | 日 K 线时间范围检索 |
| `idx_daily_klines_symbol_closed` | `daily_klines` | `symbol, is_closed, open_time DESC` | 快速拉取完结 K 线计算回撤与反弹 |
| `idx_position_snapshots_time` | `position_snapshots` | `snapshot_time DESC` | 最近持仓快照回溯 |
| `idx_sync_cursors_updated_at` | `sync_cursors` | `updated_at DESC` | 游标推进水位审查 |
| `idx_open_positions_entry_time` | `open_positions` | `entry_time DESC` | 实时持仓按入场时间倒序展示 |
| `idx_leaderboard_snapshot_date` | `leaderboard_snapshots`| `snapshot_date DESC` | 涨幅榜历史日期分页查询 |
| `idx_rebound_*_snapshot_date` | `rebound_*_snapshots` | `snapshot_date DESC` | 各周期反弹榜历史日期检索 |
| `idx_noon_loss_snapshot_date` | `noon_loss_snapshots` | `snapshot_date DESC` | 午间浮亏快照按日检索 |
| `idx_noon_loss_review_snapshot_date`| `noon_loss_review_snapshots`| `snapshot_date DESC` | 夜间复盘快照历史检索 |
| `idx_sync_run_log_created_at` | `sync_run_log` | `created_at DESC` | 同步审计日志近序分页查询 |

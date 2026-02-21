# Performance Refactor Verification Report

**Date:** 2026-02-21  
**Plan:** `docs/plans/2026-02-20-performance-refactor-implementation-plan.md`  
**Branch:** `codex/perf-refactor-exec`

## 1. Verification Commands

### 1.1 Full test suite

Command:

```bash
conda run -n base python -m pytest -q tests
```

Result (latest): `70 passed`

### 1.2 Smoke startup

Command:

```bash
conda run -n base uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Observed:

1. `Application startup complete.` appears in logs.
2. Process then exits with bind error in sandbox: `error while attempting to bind on address ('0.0.0.0', 8000): operation not permitted`.

Conclusion: application startup path is healthy; sandbox blocks port binding in current environment.

## 2. Performance Summary (Baseline vs Post)

Benchmark date: 2026-02-21  
Method: identical synthetic harnesses executed on baseline (`main`) and post-refactor (`codex/perf-refactor-exec`).

| Metric | Baseline | Post-refactor | Change |
|---|---:|---:|---:|
| API P95 (`/api/open-positions`) | 3.975 ms | 1.421 ms | -64.25% |
| Scheduler job elapsed P95 (`sync_trades_data`, mocked IO) | 5.319 ms | 5.988 ms | +12.58% |
| Hot query P95 (`get_open_positions`, 8k rows) | 40.564 ms | 39.603 ms | -2.37% |

Notes:

1. API benchmark uses TestClient + dependency overrides + fixed dataset (40 runs after warmup).
2. Scheduler benchmark uses mocked processor/db to isolate orchestration overhead (30 runs).
3. Hot query benchmark uses temp SQLite with 8,000 `open_positions` rows (80 runs).

## 3. Rollout Notes

1. Keep `API_JOB_LOCK_WAIT_SECONDS` at default `8` initially.
2. Keep short TTL defaults for API caches:
   - `POSITIONS_CACHE_TTL_SECONDS=3`
   - `LEADERBOARD_CACHE_TTL_SECONDS=10`
   - `REBOUND_CACHE_TTL_SECONDS=10`
3. Enable query-plan logging only during diagnostics:
   - `DB_QUERY_PLAN_DEBUG=1`

## 3.1 Additional Performance Optimizations (2026-02-21)

1. `SyncRepository` 增加批量水位更新：
   - `update_symbol_sync_success_batch`
   - `update_symbol_sync_failure_batch`
   并在 `scheduler.sync_trades_data` 中改为单次批量写入，减少每个 symbol 的独立事务开销。
2. `SyncRepository.save_open_positions` 改为 `executemany` 批量插入，降低大持仓集合同步耗时。
3. `RiskRepository` 增加批量告警写入：
   - `set_positions_alerted_batch`
   - `set_positions_reentry_alerted_batch`
   - `set_positions_profit_alerted_batch`
4. `risk_jobs.run_long_held_positions_check` 改为批量拉取价格（`_get_mark_price_map`）并批量更新告警状态，避免逐仓位 ticker 请求。
5. `TradeQueryService.get_trades_list` 改为向量化时长计算（`pandas.to_datetime`）+ `itertuples` 遍历，优化查询路径 CPU 开销。
6. SQLite 连接层新增并发参数：
   - `timeout=30`
   - `PRAGMA busy_timeout=5000`
   - `PRAGMA synchronous=NORMAL`
   - `PRAGMA temp_store=MEMORY`
7. `TradeRepository` 去除核心查询透传，改为仓储内 SQL + 聚合实现：
   - `get_trade_summary`
   - `get_statistics`
   - `recompute_trade_summary`
   - `get_all_trades`
   - `get_transfers`
8. `TradeQueryService.get_summary` 优先读取 `sync_status.total_trades` 快速路径，降低热路径全表统计频率。
9. `SyncRepository.save_open_positions` 增加 open_positions 状态列缓存，减少每轮 `PRAGMA` 与元数据解析开销。
10. 风控查询下推到仓储 SQL：
   - `RiskRepository.get_profit_alert_candidates`
   - `RiskRepository.get_long_held_alert_candidates`
   减少全量持仓扫描。
11. `open_positions` 新增复合索引以支撑告警候选查询：
   - `idx_open_positions_profit_alerted_entry`
   - `idx_open_positions_alerted_last_alert`
12. `Database.save_trades` 从逐条 `SELECT + UPDATE/INSERT` 改为批量 `executemany + UPSERT`：
    - 降低大批量同步时的 SQLite 往返与锁竞争。
    - 保持 `overwrite` 时间窗口删除语义不变。
13. 清理仓储层最后的直接透传：
    - `SyncRepository.save_trades` 改为仓储内批量 UPSERT。
    - `SettingsRepository.set_position_long_term` 改为仓储内 SQL 更新。
    - 当前 `app/repositories` 中已无 `return self.db.*` 透传模式。
14. `noon_loss_job` 午间检查改为批量取价（`_get_mark_price_map`），移除逐仓位 ticker 请求，进一步降低 API 权重和请求延迟。
15. 统一依赖注入与线程执行模型：
    - `get_db` 统一到 `app/core/deps.py`，移除路由/主入口重复定义。
    - 新增 `app/core/async_utils.py`，服务层统一使用 `asyncio.to_thread` 封装，移除 `get_event_loop/run_in_executor` 样板。
16. 统一批量取价实现：
    - 新增 `app/services/market_price_service.py`。
    - `scheduler` 与 `positions_service` 复用同一取价路径。
17. 下沉系统/观察列表/月度统计到仓储层：
    - 新增 `WatchNotesRepository`。
    - `TradesApiService`/`SystemApiService`/`WatchNotesService` 与 `routes/system|trades` 改为仓储调用。
    - `Database` 移除对应旧业务方法后，文件行数进一步下降（当前约 `1137` 行）。

## 4. Known Risks

1. Scheduler synthetic P95 regressed (+12.58%); likely added runtime-control/instrumentation overhead.
2. Sandbox/network restrictions can cause scheduler startup jobs to log external API resolution failures during smoke checks.
3. `sync_trades_data` metric状态传播已修复并由单测覆盖（返回失败或抛异常时记为 `error`）。
4. 批量写入路径依赖 `executemany` 与一次事务提交，若未来需要逐条失败诊断，需要补充更细粒度错误审计。

## 5. Rollback Steps

1. Revert to the pre-refactor branch/commit.
2. Disable incremental polling usage by removing `since_version` polling param on frontend if needed.
3. Disable all new cache TTLs by setting values to `0` or reverting service changes.
4. Re-run:

```bash
conda run -n base python -m pytest -q tests
```

5. Validate startup behavior again:

```bash
conda run -n base uvicorn app.main:app --host 0.0.0.0 --port 8000
```

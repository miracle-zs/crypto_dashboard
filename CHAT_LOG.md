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

- **Stage 3 (已完成)**：
  - C1：在 `TradeReadRepository.get_balance_history` 中实现 SQLite 窗口函数下采样 (`ROW_NUMBER() OVER`)，在数据量过大时按步长保留均匀分布的 1000 个采样点并首尾锚定，避免前端渲染超大点阵导致崩溃卡顿。
  - T4：在 `BinanceFuturesRestClient` 中集成持久化 `requests.Session()` 复用 TCP 连接，并在幂等 GET 请求上引入指数退避重试，同时兼容测试用例的 `requests.request` mock。
  - **测试验证**：新增 `test_balance_history_sample.py` 与 `test_binance_client_session.py`，全量 193 个测试用例全部通过！


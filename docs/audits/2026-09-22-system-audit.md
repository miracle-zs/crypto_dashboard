# 四维系统审计：Storage / Transport / Compute / Resilience

审计时间：2026-09-22 15:20–15:30，Asia/Shanghai。

结论：有需要优化和修复的点。优先解决数据正确性、同步完整性和失败可见性，再优化余额历史查询。当前证据不支持立即迁移数据库、引入微服务或单纯增加 worker。

## 范围与证据等级

- 本地与服务器部署仓库 HEAD 均为 `6c4fdd2c986ef927a8c4fec33b2fb62e3f7c7158`，开始时工作树均干净。这证明磁盘代码一致，不证明每个运行进程的内存代码都一致。
- 线上仅检查服务、日志、只读 SQLite 查询和少量 GET 请求；未重启、部署、修改配置或触发手动同步。未进行高并发压测，也未直接请求交易所账户数据做全量对账。
- **线上确认**：直接观察到的运行行为。**离线复现**：真实业务函数配合模拟外部输入稳定失败，尚不能据此断言生产数据已受影响。**代码风险**：可见实现缺口，未注入生产故障。
- 这次是探索性审计，没有用户指定的单一故障。先建立线上基线，再构造最小失败用例；修复与上线不属于本次已经完成的内容。

## 运行基线

| 项目 | 观察 |
| --- | --- |
| 部署 | Gunicorn + UvicornWorker，1 worker，内置调度器运行 |
| 主机内存 | 约 1.97 GB，总可用约 1.3 GB；Swap 占用约 1.6 GB |
| 换页 | 短采样出现 32 KB/s 换入，下一秒 0；不能据 Swap 占用断言持续抖动 |
| 磁盘 | 50 GB，使用率 48% |
| 服务内存 | systemd MemoryCurrent 约 175 MiB，MemoryPeak 约 273 MiB |
| SQLite | WAL，约 190 MiB；余额历史 292,513 行、同步日志 100,063 行、日 K 192,323 行 |
| 最近持仓同步 | 约 0.39–0.67 秒，每 5 分钟运行，日志显示正常 |

以下耗时来自服务器本机 Unix socket，包含应用处理，不包含用户网络和 Nginx；少量顺序样本，不是 P95 或容量测试。

| GET 接口 | 观察耗时 |
| --- | --- |
| `/api/status` | 约 5–52 ms |
| `/api/open-positions` | 首次 151 ms，随后缓存命中约 1.7–2.2 ms |
| `/api/summary` | 约 4–19 ms |
| `/api/trades-aggregates` | 约 15–70 ms |
| `/api/balance-history?time_range=1y` | 7.326 s、1.497 s、0.963 s |

一年查询运行期间额外发起一次 `/api/status`，耗时 129 ms。样本不足以归因于事件循环阻塞；只能说明长查询期间也存在可见延迟。误用 `range=1y` 会继续使用默认 1d，不能作为一年查询的性能证据。

## 1. 存储与状态

### S1 · P1 · 实时持仓使用陈旧日 K 收盘价【线上确认】

位置：`app/services/positions_service.py:168–181,302`，`app/jobs/scheduler_startup_jobs.py:108–122`。

持仓接口从 `DailyKlineRepository.load()` 取最后一根日 K 的 `close`，命名为 `mark_price`，用于计算浮盈亏和敞口。日 K 仅每天清晨更新。审计时四个持仓币种的最近更新时间是 UTC 2026-09-21 22:15–22:16，即北京时间 9 月 22 日 06:15–06:16；15:22 的接口仍使用这些价格，响应 `as_of` 却为 15:22。

这不仅是缓存延迟：成交 K 线 close 和标记价格也是不同的数据语义。应将日 K 历史缓存与实时 mark-price 状态分开；后台批量刷新或订阅行情，页面只读缓存，并提供 `price_as_of`、`positions_as_of`、`stale`。已有 positionRisk 调用可保留其价格字段作为低频基线，无需页面每次请求交易所。

### S2 · P1 · 增量游标丢失跨窗口开仓上下文【离线复现】

位置：`app/services/trade_etl_service.py:27–40,87–106`。

最小用例：完整订单窗口含开仓和平仓，匹配得到 1 笔；推进订单游标后，30 分钟重叠窗口排除了开仓，仅保留平仓，输出 0 笔且没有报错。当前匹配每次从空队列开始，没有恢复未完成的开仓批次。周全量/补偿可能修复部分情况，但不能使单次增量匹配完整。

建议持久化原始订单/成交及未完成开仓批次，游标只表示采集进度；匹配投影从持久状态增量计算。短期方案是在需要重建时回溯到最早未结开仓时间，而不是只回溯固定分钟数。

线上闭仓表最后退出时间为 2026-08-19 21:15:33，近期同步多次输出 0 行。这是需要交易所对账的信号，**未证明期间必然有漏单，也未证明由此 bug 导致**。

### S3 · P1 · 老持仓可能被当成空仓覆盖【离线复现】

位置：`app/services/trade_etl_service.py:441–457,533–586`；`app/jobs/sync_jobs.py:57–87`。

当 positionRisk 明确返回非零仓位，而最近 7 天没有订单，提取器返回空列表，汇总函数把它视为成功；调度任务随后用空快照覆盖原持仓。最小用例已得到 `[]` 而非有效持仓或“数据不完整”。

建议以 positionRisk 为当前仓位事实来源；订单历史只用于补充入场时间和归因。当当前数量无法被订单重建时保留上次数据并标记 incomplete，禁止把未知状态表示为空仓。

### S4 · P1（启用双向持仓时）· 相同 symbol 的多空仓互相覆盖【离线复现】

位置：`app/services/trade_api_gateway.py:266–280`。

`real_pos[symbol] = amt` 丢弃 positionSide。同一 symbol 的 LONG=1、SHORT=-2 输入只剩 SHORT=-2。应使用 `(symbol, positionSide)` 贯穿采集、持仓模型、同步和展示，或明确拒绝不支持的持仓模式。未验证当前生产账户是否启用双向模式。

## 2. 通信与传输

### T1 · P1 · 全类型流水用错误去重键，且分页会跳过同毫秒记录【离线复现】

位置：`app/services/trade_api_gateway.py:18,43–49,54–65`。

- 同一个 tranId、不同 incomeType 的两条记录只保留一条。全类型 income 请求已经用于活跃币种发现和费用汇总，错误去重会影响后续计算。
- 同毫秒 1,001 条记录，第一页 1,000 条后 `startTime=last_time+1`，最终只得到 1,000 条。

官方规定 tranId 只在同 incomeType 内唯一，并提供 page 参数。应使用至少 `(incomeType, tranId)` 的稳定标识，并在固定时间范围内正确分页，再推进游标。[Binance Income History](https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/Get-Income-History)

### T2 · P1 · 300 天全量同步撞上 90 天订单保留限制【线上确认】

位置：`app/jobs/scheduler_startup_jobs.py:43–51`，`app/jobs/sync_pipeline_jobs.py:21–26`。

生产配置 `DAYS_TO_FETCH=300`。北京时间 9 月 20 日约 03:30–03:48 的全量任务耗时 1,084,585 ms，264 个币种中 247 个失败、17 个成功，写入 43 条。日志明确返回 `-4166: Search window is restricted to recent 90 days only.`

应将在线订单校验限制在端点可查询区间；较早历史使用持久化原始数据或官方导出导入。仅调整时间范围不能修复所有历史数据缺口。[Binance All Orders](https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/All-Orders)

### T3 · P2 · 请求预算少算账户和持仓接口权重【离线复现 + 官方契约】

位置：`app/core/binance_request_budget.py:14–38`。

`/fapi/v3/account` 和 `/fapi/v3/positionRisk` 都落到默认权重 1，官方权重均为 5。预算器不能准确兑现声明的总权重上限。补全所调用端点的规则，增加契约测试，并结合响应头观测真实消耗。

[Account V3](https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/Account-Information-V3)，[Position V3](https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Position-Information-V3)。

### T4 · P2 · REST 连接复用与失败恢复不足【代码风险】

位置：`app/binance_client.py:183–245`。

请求使用模块级 `requests.request()`，没有跨调用持久连接池；名义 4 次重试实际主要服务于时间偏移恢复，网络异常和普通 HTTP 错误直接返回 None。可为每个 worker 复用 Session，针对幂等 GET 的暂时错误设置有限退避、抖动和总时限。收益尚未做 A/B 测量；不能笼统给所有 POST 自动重试。

## 3. 计算与并发

### C1 · P2 · 年度余额查询先物化全量，再抽样【线上慢请求 + 代码确认】

位置：`app/repositories/trade_read_repository.py:166–192`，`app/services/balance_service.py:22–24,54–62`。

数据库已有 timestamp 索引，EXPLAIN 确认使用索引；问题是查询会 fetchall 全部命中记录并构建字典，之后才抽样到约 1,000 点。当前约 29 万条记录下实测 0.96–7.33 秒，时间和内存随历史长度增长。

优先把时间桶聚合/取样下推到数据库，或者维护小时/日级余额投影；保留首尾点和入出金事件，避免只求均值破坏净值语义。随后增加短 TTL 缓存与同一查询的请求合并。验收应比较相同数据集下结果正确性、延迟和内存，而非只比较响应点数。

### C2 · P2 · 锁与请求预算耦合，实际部署关闭任务互斥【配置确认 / 代码风险】

位置：`app/binance_client.py:102–113`，`app/core/job_runtime.py:25–43`。

生产 `API_JOB_LOCK_WAIT_SECONDS=0`，意味着任务级互斥关闭；日志也显示余额和持仓任务同时运行。客户端在全局 throttle 锁内等待权重额度，历史任务可能延迟实时任务。预算和冷却是进程内 class 状态，无法天然协调同主机其他服务。

目前没有证据显示任务锁竞争造成生产超时；不建议直接改成开启全局长锁。应先区分短时账户刷新与长时历史回填的调度优先级，再让预算等待不占用全局管理锁。若跨服务共享同一出口，核实后再设计共同预算。

## 4. 可观测与弹性

### R1 · P1 · 总健康状态掩盖历史失败；余额失败返回 success【线上确认 + 离线复现】

位置：`app/routes/system.py:46–60`，`app/jobs/balance_sync_job.py:15–69`。

审计时 `symbol_sync_state` 有 306 个非空 last_error；最近周全量明确 partial，状态接口仍返回 `online`、`idle`、error=null。随后一个成功任务会覆盖单一全局同步状态，无法表示“最近任务正常，但历史缺口尚未修复”。online 可以仅表示存活，但需要独立 readiness/data-quality 状态。

余额函数在 get_account_balance 返回 None 时只记 warning，最后仍返回 `success`，最小用例已复现。部分 transfer 异常也只写 warning。

建议明确 success / partial / error / skipped；按数据流记录 last_success_at、覆盖区间、最新水位、未解决失败数、数据年龄。新增数据完整性校验，不能只监控 HTTP 200 和 scheduler.running。

### R2 · P2 · 用户流缺少断线恢复，启用后又取消 REST 兜底【代码风险，当前未启用】

位置：`app/user_stream.py:101–111,151–152`，`app/jobs/scheduler_startup_jobs.py:13–14,94–105`。

close 回调仅日志记录，没有显式重连、重新申请 listenKey、重连后补偿对账；初次 start 失败也无恢复调度。与此同时 ENABLE_USER_STREAM 会取消余额及 TRANSFER 的启动/周期同步；用户流本身只保存事件和余额，不落 TRANSFER 流水。

当前生产 `ENABLE_USER_STREAM=0`、ws_events=0，因此这不是现有线上中断的原因。启用前应补齐状态机、指数退避、数据新鲜度检查、REST 降级和定期对账。

## 测试与复现

隔离 Python 3.11 环境依照 requirements.txt 安装依赖，并补装 httpx；未变更项目依赖文件。

```sh
rtk proxy /tmp/crypto-dashboard-audit-venv/bin/python -m pytest -q
# 1 failed, 189 passed in 3.11s

rtk proxy /tmp/crypto-dashboard-audit-venv/bin/python -m pytest -q docs/audits/2026-09-22-offline-probes.py --tb=no
# 8 failed in 0.79s
```

基线失败为 `tests/test_positions_contract.py::test_open_positions_page_has_no_binance_dependency`：遍历 app.routes 未找到直接路径，触发 StopIteration。当前隔离环境 FastAPI 0.141.1 / Starlette 1.6.0 / pandas 3.0.6 / requests 2.34.2 / APScheduler 3.11.3。没有核对线上全部版本，因此不将其认定为生产路由故障。requirements 只有下界，缺少可复现部署锁定也是维护风险。

[离线探针](2026-09-22-offline-probes.py)刻意断言正确行为，当前失败就是审计证据，不纳入默认测试目录；无外部网络调用或生产数据库写入。8 个断言中请求权重占两个用例。修复后应将相应案例整理为正式回归测试。

## 实施顺序

1. **先修数据正确性**：分离实时价格与日 K；修正流水标识和分页；保护老持仓；处理双向仓位；保留跨窗口开仓上下文。
2. **修恢复与可见性**：限制在线历史窗口，保留失败待修复清单；余额失败不得报成功；按流监控数据新鲜度。对 8 月 19 日之后的闭仓数据做独立对账，避免仅凭零行结果判断正常。
3. **优化热点**：年度余额 SQL 聚合、缓存和请求合并；端点权重、连接池、幂等 GET 退避。
4. **适度重构**：明确“采集 → 原始记录 → 匹配/汇总投影 → API 只读”边界；只有出现独立扩缩容需求时再把 scheduler 迁出 Web 进程。当前 SQLite 规模和常规 API 耗时不构成迁移数据库的证据。

备份方面仅看到本地历史备份文件，root crontab 和 systemd timers 未识别到本项目定时备份；未核查云快照或其他备份机制，不能断言无备份。后续应验证 SQLite 一致性备份与恢复流程。

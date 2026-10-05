# 系统文档中心

这里是加密货币空头驾驶舱的工程文档入口。链接均使用仓库相对路径，可在 GitHub、编辑器和本地克隆中正常跳转。

## 目录约定

```text
docs/
├── architecture/  # 当前系统结构与端到端数据流
├── reference/     # 对接、数据与运维时需要查询的规范
├── plans/         # 按日期命名的设计提案与实施记录
└── audits/        # 审计报告及其可复现探针
```

新文档应使用小写英文 kebab-case 文件名；长期有效的系统说明放入 `architecture/` 或 `reference/`，带明确日期的变更方案与审计证据分别放入 `plans/` 和 `audits/`。

## 当前系统

| 文档 | 内容 |
| :--- | :--- |
| [系统架构设计](architecture/system-architecture.md) | 模块化单体分层、领域服务编排、事实记账、读模型与分级锁。 |
| [全景数据流设计](architecture/data-flow.md) | 交易所数据摄取、状态化批次撮合、市场快照与风控闭环。 |
| [数据字典](reference/data-dictionary.md) | SQLite 持久化表的字段、约束、索引与业务语义。 |
| [REST API 参考](reference/api-reference.md) | REST 端点、请求参数、响应模型和鉴权机制。 |

## 演进方案

| 日期 | 文档 | 主题 |
| :--- | :--- | :--- |
| 2026-09-26 | [第一性原理重构](plans/2026-09-26-first-principles-refactor.md) | 不可变事实账本与可重放投影。 |
| 2026-04-10 | [暴跌风险雷达设计](plans/2026-04-10-crash-risk-design.md) | 结构性崩盘预警评分。 |
| 2026-03-24 | [365D 反弹榜实现](plans/2026-03-24-rebound-365d-implementation.md) | 年化周期反弹榜。 |
| 2026-02-20 | [性能重构设计](plans/2026-02-20-performance-refactor-design.md) / [验证报告](plans/2026-02-20-performance-refactor-verification-report.md) | 热点查询与调度性能。 |
| 2026-02-20 | [模块化单体分层设计](plans/2026-02-20-refactor-design.md) | 从扁平脚本迁移到领域分层。 |

## 审计与验证

| 日期 | 资料 | 用途 |
| :--- | :--- | :--- |
| 2026-09-22 | [四维系统审计](audits/2026-09-22-system-audit.md) | Storage、Transport、Compute、Resilience 审计结论。 |
| 2026-09-22 | [离线复现探针](audits/2026-09-22-offline-probes.py) | 审计发现的最小复现与验证。 |
| 2026-09-26 | [架构探测脚本](audits/2026-09-26-architecture-probes.py) | 架构行为与不变量验证。 |

## 快速导航

- 新开发者：先读[系统架构](architecture/system-architecture.md)，再读[数据流](architecture/data-flow.md)。
- 前端或系统集成：查阅 [REST API 参考](reference/api-reference.md)。
- 数据库查询与运维：查阅[数据字典](reference/data-dictionary.md)。

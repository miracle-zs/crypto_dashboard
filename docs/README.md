# 系统文档中心 (Documentation Hub)

欢迎查阅 **Zero Gravity Dashboard (加密货币空头驾驶舱)** 技术文档中心。本文档提供了系统从宏观架构、微观数据模型到实战接口与演进历史的完整索引。

---

## 📚 核心规范与设计文档

| 文档名称 | 路径 | 核心内容概述 |
| :--- | :--- | :--- |
| **系统架构设计** | [architecture.md](file:///Users/zhangshuai/PycharmProjects/crypto_dashboard/docs/architecture.md) | 模块化单体分层、领域服务编排、第一性原理事实记账、只读模型与分级锁机制 |
| **全景数据流设计** | [data_flow.md](file:///Users/zhangshuai/PycharmProjects/crypto_dashboard/docs/data_flow.md) | 交易所数据摄取、状态化批次撮合、07:00 晨间五榜流水线、午夜风控闭环时序 |
| **全量数据字典** | [data_dictionary.md](file:///Users/zhangshuai/PycharmProjects/crypto_dashboard/docs/data_dictionary.md) | 数据库全量 27 张持久化表的字段结构、主键约束、索引规划与业务语义 |
| **REST API 规格手册** | [api_reference.md](file:///Users/zhangshuai/PycharmProjects/crypto_dashboard/docs/api_reference.md) | 8 大业务领域所有 REST API 端点、请求参数、响应模型与鉴权机制 |

---

## 📈 架构演进与设计提案 (Plans)

记录了系统历次重大重构与功能演进的深度设计方案：

- [2026-09-26 第一性原理重构方案 (First Principles Refactor)](file:///Users/zhangshuai/PycharmProjects/crypto_dashboard/docs/plans/2026-09-26-first-principles-refactor.md)
  *核心突破*：打破窗口限制，建立不可变事实账本与可重放批次撮合投影。
- [2026-04-10 暴跌/插针风险雷达设计](file:///Users/zhangshuai/PycharmProjects/crypto_dashboard/docs/plans/2026-04-10-crash-risk-design.md)
  *核心突破*：多周期极端涨幅、持仓量异动与插针崩盘风险评分模型。
- [2026-03-24 365D 大周期反弹榜实现](file:///Users/zhangshuai/PycharmProjects/crypto_dashboard/docs/plans/2026-03-24-rebound-365d-implementation.md)
  *核心突破*：共享本地日线缓存，零 REST 请求高效扩充年化周期反弹榜。
- [2026-02-20 性能优先重构方案与验证报告](file:///Users/zhangshuai/PycharmProjects/crypto_dashboard/docs/plans/2026-02-20-performance-refactor-verification-report.md)
  *核心突破*：[性能重构设计](file:///Users/zhangshuai/PycharmProjects/crypto_dashboard/docs/plans/2026-02-20-performance-refactor-design.md) + 读模型缓存加速，热点查询延迟优化达 64%。
- [2026-02-20 模块化单体分层设计](file:///Users/zhangshuai/PycharmProjects/crypto_dashboard/docs/plans/2026-02-20-refactor-design.md)
  *核心突破*：由早期单体扁平脚本向清晰的 `api / services / repositories / jobs / core` 分层治理演进。

---

## 🔍 系统审计与离线验证 (Audits)

- [2026-09-22 四维系统审计报告 (Storage/Transport/Compute/Resilience)](file:///Users/zhangshuai/PycharmProjects/crypto_dashboard/docs/audits/2026-09-22-system-audit.md)
- [2026-09-22 离线复现与验证探针脚本](file:///Users/zhangshuai/PycharmProjects/crypto_dashboard/docs/audits/2026-09-22-offline-probes.py)
- [2026-09-26 架构探测脚本](file:///Users/zhangshuai/PycharmProjects/crypto_dashboard/docs/audits/2026-09-26-architecture-probes.py)

---

## 🚀 快速导航与查阅建议

- **新开发者入门**：建议首先通读 [architecture.md](file:///Users/zhangshuai/PycharmProjects/crypto_dashboard/docs/architecture.md) 理解模块划分，再查看 [data_flow.md](file:///Users/zhangshuai/PycharmProjects/crypto_dashboard/docs/data_flow.md)。
- **前端与对接开发**：直接参考 [api_reference.md](file:///Users/zhangshuai/PycharmProjects/crypto_dashboard/docs/api_reference.md)。
- **数据库查询与运维**：直接查阅 [data_dictionary.md](file:///Users/zhangshuai/PycharmProjects/crypto_dashboard/docs/data_dictionary.md)。

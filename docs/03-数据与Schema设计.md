# 数据与 Schema 设计

**项目名称**：AI 数据分析助手（第一阶段）  
**文档版本**：v0.1  
**状态**：草案

---

## 1. 设计目标

1. **字段总量控制**：两张表合计 **不超过 10 个业务字段**（不含主键/技术性列可酌量，但需保持 Prompt 简洁）。  
2. **JOIN 路径唯一**：`orders.order_id = shipping.order_id`  
3. **指标可解释**：利润率变化可以主要由 `cost`、`shipping_cost`、结构变化（国家/时间）解释。

---

## 2. 表定义（逻辑模型）

### 2.1 `orders`

| 字段 | 类型（建议） | 说明 |
| --- | --- | --- |
| order_id | TEXT / INTEGER | 主键 |
| price | REAL | 订单售价（收入口径 = price） |
| cost | REAL | 货品/采购等成本（按订单聚合后的单笔口径） |
| country | TEXT | 国家/站点标识（示例：`US`） |
| date | TEXT | ISO 日期 `YYYY-MM-DD`（便于 SQLite 比较） |

### 2.2 `shipping`

| 字段 | 类型（建议） | 说明 |
| --- | --- | --- |
| order_id | TEXT / INTEGER | 外键 → `orders.order_id` |
| shipping_cost | REAL | 运费（或头程/尾程合计，第一版固定一种解释） |

---

## 3. 派生指标（不在表中存，允许在 SQL 中计算）

在分析中统一使用以下定义（与 SRS 一致）：

- `revenue = price`  
- `profit = price - cost - shipping_cost`  
- `margin = profit / NULLIF(price, 0)`

> 若 Mock 数据需要体现“利润率下降”，优先从 **shipping_cost 上升 / cost 上升 / 低价单占比上升** 之一制造可解释差异。

---

## 4. SQL 生成约束（写进 Prompt 的硬规则示例）

1. 只允许 `SELECT`（以及必要的 `WITH`，若团队允许；否则第一版可禁用 `WITH` 以降低复杂度）。  
2. `FROM` 仅允许 `orders` 与 `shipping`，`JOIN` 只能按 `order_id`。  
3. 字段只能来自白名单。  
4. 时间对比：默认近 N 天 vs 前 N 天（N 在配置或 UI 上固定）。  
5. 聚合优先：按 `country`、按周/按日聚合，避免返回海量明细入模。

---

## 5. Mock 数据策略（建议）

- **行数**：每个时间桶几十到几百行即可。  
- **美国站故事线（选一即可）**：
  - 近期 `shipping_cost` 均值显著高于前期；或  
  - 近期 `cost` 上升；或  
  - 近期低价订单占比提高导致 `margin` 被稀释。  

将“故事线”写入数据生成脚本注释，避免团队忘记 Mock 的可解释目的。

---

## 6. 数据初始化方式

1. 应用启动：连接 SQLite → `CREATE TABLE IF NOT EXISTS` → `INSERT` 种子数据（或从 `seed.sql` 执行）。  
2. 第一阶段**不**做 ETL、不接入生产库。

---

## 7. 隐私与安全

Mock 数据不得包含真实客户 PII；若后续接真实数据，需另立数据分级与访问控制文档（不在 v0.1）。

---

## 8. 文档变更记录

| 版本 | 日期 | 变更说明 |
| --- | --- | --- |
| v0.1 | 2026-04-19 | 初稿 |

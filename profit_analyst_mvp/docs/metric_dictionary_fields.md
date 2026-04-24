# 指标字典表（精简版）字段规划

目标：只保留“LLM 能稳定识别指标 + 按口径生成 SQL + 做最基本校验”所需的关键信息。

---

## 1) 表名建议

- `metric_dictionary`

---

## 2) 字段清单（精简必需）

### A. 识别与检索

- **`metric_key`**：指标唯一键（英文/代码用；全局唯一），如 `profit`、`gmv`
- **`metric_name`**：指标中文名，如“利润”“GMV”
- **`aliases`**：别名/同义词（JSON 数组），如 `["毛利", "毛利额"]`

### B. 计算定义（核心）

- **`definition_type`**：定义类型（枚举）
  - `sql_expr`：可直接放进 `SELECT` 的表达式（单表场景最常用）
  - `sql_template`：SQL 片段/CTE 模板（仍建议保持单语句、可加占位符）
- **`formula_sql`**：计算 SQL 表达式/模板（允许占位符，如 `{{date_filter}}`、`{{extra_where}}`）
- **`required_fields`**：所需字段（JSON 数组），用于生成前校验/提示缺字段

### C. 展示与约束（最低限度）

- **`unit`**：单位（如 `CNY`/`USD`/`%`/`count`）
- **`value_type`**：值类型（枚举：`money`/`percent`/`count`/`number`）
- **`status`**：状态（枚举：`active`/`deprecated`），便于灰度与下线

---

## 3) 可选字段（如果你们后续需要再加）

- `description`：业务解释（用于 UI 展示/给 LLM 做解释）
- `version`：口径版本号（当你们开始做口径治理时再加）
- `source_table`：来源表（当不止一张表时再加）


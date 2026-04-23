# AI 数据分析助手（第一阶段）- M5 演示级交付

本目录是第一阶段原型的独立实现，用于**演示级**端到端链路：

> 自然语言问题 → LLM 生成 SQL → `sql_guard` 安全校验 → SQLite 查询 → LLM 基于结果表输出中文结论

## 0. 一句话要求

- **只允许 `SELECT`（可选 `WITH`），禁止 `JOIN`，禁止多语句**  
- `FROM` 只允许单表：`dashbord_new_data1`（兼容 LLM 生成的 `yibai_oversea.dashbord_new_data1` 会自动重写）

## 1. 启动（Windows PowerShell）

在仓库根目录执行：

```bash
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r profit_analyst_mvp/requirements.txt
streamlit run profit_analyst_mvp/app.py
```

## 2. 环境变量（DeepSeek / OpenAI 兼容）

推荐复制模板并填入本地密钥（模板：`profit_analyst_mvp/.env.example`；你本地创建：`profit_analyst_mvp/.env`）。

或用 PowerShell 临时设置：

```bash
$env:LLM_API_KEY="你的key"
$env:LLM_BASE_URL="https://api.deepseek.com"   # 可选
$env:LLM_MODEL="deepseek-chat"                 # 可选：SQL 生成模型
$env:LLM_MODEL_ANALYSIS="deepseek-chat"        # 可选：结论模型；不填则复用 LLM_MODEL
$env:LLM_TIMEOUT_S="60"                        # 可选
```

## 3. 数据初始化（SQLite）

本阶段使用 SQLite + Mock Excel 初始化数据库。

### 3.1 初始化

```bash
.\.venv\Scripts\python profit_analyst_mvp/db_init.py `
  --excel profit_analyst_mvp/data/mock_orders.xlsx `
  --db profit_analyst_mvp/data/app.db
```

### 3.2 手工验证（建议跑一次）

```bash
.\.venv\Scripts\python profit_analyst_mvp/verify_sqlite.py --db profit_analyst_mvp/data/app.db
```

## 4. 演示步骤（对应验收用例 TC-01~TC-04）

1. 打开页面后，在输入框填入问题  
2. 点击「开始分析」生成 SQL 并执行查询  
3. 核对页面展示的 SQL 为 `SELECT`，且只用单表字段  
4. 点击「生成结论」输出中文结论（结论必须引用结果表证据）

## 5. 推荐的示例问题（演示用）

- 为什么最近美国站利润率下降？
- 近 30 天 vs 前 30 天，哪些国家利润下降最多？
- 最近 30 天利润/利润率的主要变化来自销量变化还是成本变化？（若字段支持）

## 6. 常见问题排查

- **提示 “SQLite db 不存在”**：先执行第 3 节初始化命令生成 `profit_analyst_mvp/data/app.db`
- **提示 “缺少 LLM 配置”**：设置 `LLM_API_KEY`（或在 `profit_analyst_mvp/.env` 中填写）
- **提示 “SQL 校验失败”**：说明 LLM 输出触发了 `sql_guard`（例如 JOIN、多语句、非白名单标识符）


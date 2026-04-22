# AI 数据分析助手（第一阶段）- M1 可运行骨架

本目录为本阶段原型的独立实现，避免与仓库中既有脚本相互影响。

## 启动

在仓库根目录打开终端后执行：

```bash
python -m venv .venv
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
pip install -r profit_analyst_mvp/requirements.txt
streamlit run profit_analyst_mvp/app.py
```

## M3：LLM SQL 通路（DeepSeek）

推荐在本地新建配置文件 `profit_analyst_mvp/.env`（不会上传到 GitHub），然后写入配置。

可以从 `profit_analyst_mvp/.env.example` 复制一份改名为 `.env`。

如需临时在 PowerShell 设置环境变量，也可以用下面示例：

```bash
$env:LLM_API_KEY="你的key"
$env:LLM_BASE_URL="https://api.deepseek.com"   # 可选
$env:LLM_MODEL="deepseek-chat"                 # 可选
$env:LLM_MODEL_ANALYSIS="deepseek-chat"        # 可选：用于 M4 结论生成
```

## M2：SQLite 初始化（从模拟订单 Excel 导入）

1. 把你的模拟订单 Excel 放到任意路径（推荐：`profit_analyst_mvp/data/mock_orders.xlsx`）
2. 执行初始化脚本（会生成/覆盖 SQLite 文件）

```bash
.\.venv\Scripts\python profit_analyst_mvp/db_init.py --excel profit_analyst_mvp/data/mock_orders.xlsx --db profit_analyst_mvp/data/app.db
```

3. 之后在 SQLite 里可直接查询表 `dashbord_new_data1`

## M2：手工验证（必须能跑出结果）

```bash
.\.venv\Scripts\python profit_analyst_mvp/verify_sqlite.py --db profit_analyst_mvp/data/app.db
```

## M1 范围

- 可启动的 Streamlit 页面
- 问题输入 + 点击按钮
- 四段展示区：问题 / SQL（占位）/ 结果表（占位）/ 结论（占位）
- 错误信息区（占位）

> M1 不接入 LLM、不接入真实数据库；后续里程碑会逐步接通 SQL 生成与执行。


from __future__ import annotations

from dataclasses import dataclass

from profit_analyst_mvp.schema import TableSchema
from profit_analyst_mvp.dicts import FieldHint, MetricHint, field_hints_to_prompt_text, metric_hints_to_prompt_text


@dataclass(frozen=True)
class SqlPrompt:
    system: str
    user: str


@dataclass(frozen=True)
class AnalysisPrompt:
    system: str
    user: str


@dataclass(frozen=True)
class FollowupSqlPrompt:
    system: str
    user: str


def build_sql_prompt(
    *,
    question: str,
    context: str | None = None,
    schema: TableSchema,
    days_window: int = 30,
    field_hints: list[FieldHint] | None = None,
    metric_hints: list[MetricHint] | None = None,
) -> SqlPrompt:
    # 目标：稳定产出可执行 SQL；强约束只输出 SQL 文本
    field_hints = field_hints or []
    metric_hints = metric_hints or []
    system = (
        "你是资深数据分析工程师。你的任务是把用户问题转成可在 SQLite 执行的 SQL。\n"
        "必须严格遵守以下硬规则：\n"
        "1) 只允许输出一条 SQL（禁止解释、禁止 Markdown code fence）。\n"
        "2) 只允许 SELECT（可选 WITH），禁止 JOIN。\n"
        "3) FROM 只能使用允许的那一张表。\n"
        "4) 只能使用白名单字段；不要臆造字段名。\n"
        "5) 结果要尽量聚合（国家/日期等），避免明细爆量；默认 LIMIT 200。\n"
        "6) 时间对比优先采用“近 N 天 vs 前 N 天”。N="
        f"{days_window}\n"
        "\n"
        "字段对照表（从用户问题召回的候选字段，仅作语义参考；真正可用字段仍以白名单为准）：\n"
        f"{field_hints_to_prompt_text(field_hints)}\n"
        "指标字典（从用户问题召回的候选指标口径，可直接用于计算；必须保证 required_fields 均在白名单中）：\n"
        f"{metric_hints_to_prompt_text(metric_hints)}\n"
        "表与字段：\n"
        f"{schema.to_prompt_text()}\n"
    )

    ctx = (context or "").strip()
    ctx_block = ""
    if ctx:
        ctx_block = "\n\n（上下文，供你理解追问与延续口径；不要编造未给出的信息）\n" + ctx + "\n"
    user = (
        "用户问题：\n"
        f"{question}\n"
        f"{ctx_block}"
        "\n"
        "只输出 SQL："
    )
    return SqlPrompt(system=system, user=user)


def build_sql_repair_prompt(
    *,
    question: str,
    context: str | None = None,
    schema: TableSchema,
    bad_sql: str,
    error: str,
    days_window: int = 30,
    field_hints: list[FieldHint] | None = None,
    metric_hints: list[MetricHint] | None = None,
) -> SqlPrompt:
    field_hints = field_hints or []
    metric_hints = metric_hints or []
    system = (
        "你是资深数据分析工程师。你需要修复 SQL，使其满足硬规则并可在 SQLite 执行。\n"
        "必须严格遵守：只输出一条 SQL；只允许 SELECT；禁止 JOIN；仅用白名单字段；FROM 仅允许单表。\n"
        f"N={days_window}\n"
        "\n"
        "字段对照表（从用户问题召回的候选字段，仅作语义参考；真正可用字段仍以白名单为准）：\n"
        f"{field_hints_to_prompt_text(field_hints)}\n"
        "指标字典（从用户问题召回的候选指标口径，可直接用于计算；必须保证 required_fields 均在白名单中）：\n"
        f"{metric_hints_to_prompt_text(metric_hints)}\n"
        "表与字段：\n"
        f"{schema.to_prompt_text()}\n"
    )
    ctx = (context or "").strip()
    ctx_block = ""
    if ctx:
        ctx_block = "（上下文，供你理解追问与延续口径）\n" + ctx + "\n\n"
    user = (
        "用户问题：\n"
        f"{question}\n\n"
        f"{ctx_block}"
        "上一条 SQL：\n"
        f"{bad_sql}\n\n"
        "报错/校验失败原因：\n"
        f"{error}\n\n"
        "请输出修复后的 SQL（仅 SQL）："
    )
    return SqlPrompt(system=system, user=user)


def build_followup_sql_prompt(
    *,
    question: str,
    schema: TableSchema,
    days_window: int,
    main_sql: str,
    main_table_preview_csv: str,
    main_columns: list[str],
    field_hints: list[FieldHint] | None = None,
    metric_hints: list[MetricHint] | None = None,
    max_followups: int = 2,
) -> FollowupSqlPrompt:
    field_hints = field_hints or []
    metric_hints = metric_hints or []
    system = (
        "你是严谨的数据分析工程师。你需要在已得到“主查询结果表”的前提下，提出补充取证用的 SQL。\n"
        "硬规则：\n"
        "1) 只输出 JSON（禁止解释、禁止 Markdown）。\n"
        "2) 每条 SQL 必须是只读：只允许 SELECT（可选 WITH），禁止 JOIN，禁止写操作。\n"
        "3) FROM 只能使用允许的那一张表，且只能使用白名单字段。\n"
        f"4) 最多输出 {int(max_followups)} 条 follow-up SQL；如果主查询已足够支撑结论，可输出空数组。\n"
        "5) follow-up SQL 的目标是“让最终结论有数据证据”，优先选择：\n"
        "   - 指标拆解：分子/分母（如利润率的利润与销售额）在两期的变化\n"
        "   - 维度下钻：按 SKU/平台/国家/渠道等分组找 Top 贡献或异常\n"
        "6) 如果用户问了“为什么/原因”，你必须至少尝试 1 条拆解或下钻（除非字段明显不支持）。\n"
        "\n"
        "输出 JSON 格式：\n"
        "{\n"
        '  \"followups\": [\n'
        '    {\"purpose\": \"一句话说明用途\", \"sql\": \"SELECT ...\"}\n'
        "  ]\n"
        "}\n"
        "\n"
        "字段对照表候选：\n"
        f"{field_hints_to_prompt_text(field_hints)}\n"
        "指标字典候选：\n"
        f"{metric_hints_to_prompt_text(metric_hints)}\n"
        "表与字段：\n"
        f"{schema.to_prompt_text()}\n"
    )
    user = (
        "用户问题：\n"
        f"{question}\n\n"
        f"主查询（已执行）SQL：\n{main_sql}\n\n"
        f"主查询结果字段：{', '.join(main_columns)}\n"
        "主查询结果表（CSV，已截断）：\n"
        f"{main_table_preview_csv}\n\n"
        f"请输出 follow-up SQL JSON（最多 {int(max_followups)} 条）："
    )
    return FollowupSqlPrompt(system=system, user=user)


def build_analysis_prompt(
    *,
    question: str,
    sql: str,
    table_preview_csv: str,
    columns: list[str],
    row_count: int,
) -> AnalysisPrompt:
    system = (
        "你是严谨的数据分析助手。你只能基于我提供的“查询结果表”进行分析。\n"
        "硬规则：\n"
        "1) 不得编造表外字段、表外口径、表外数值；不确定就明确说“无法从结果表判断”。\n"
        "2) 结论必须引用结果表中的证据（至少 2 条），并使用中文。\n"
        "3) 优先输出可解释的结论与下一步建议（例如建议增加哪些分组/时间窗对比）。\n"
        "4) 输出格式固定为：\n"
        "【结论】... \n"
        "【证据】- ...\n"
        "【可能原因】...（如果结果表不足以判断，则写“需进一步查询”）\n"
        "【下一步SQL建议】...（给 1-2 个可执行的方向，不要求给完整 SQL）\n"
    )

    user = (
        "用户问题：\n"
        f"{question}\n\n"
        "本次执行 SQL：\n"
        f"{sql}\n\n"
        f"结果表行数：{row_count}\n"
        f"结果表字段：{', '.join(columns)}\n\n"
        "结果表（CSV，已截断）：\n"
        f"{table_preview_csv}\n"
    )
    return AnalysisPrompt(system=system, user=user)


def build_analysis_prompt_with_evidence(
    *,
    question: str,
    main_sql: str,
    main_table_preview_csv: str,
    main_columns: list[str],
    main_row_count: int,
    evidence_blocks: list[dict[str, str]],
) -> AnalysisPrompt:
    system = (
        "你是严谨的数据分析助手。你只能基于我提供的“查询结果表”进行分析。\n"
        "硬规则：\n"
        "1) 不得编造表外字段、表外口径、表外数值；不确定就明确说“无法从结果表判断”。\n"
        "2) 结论必须引用结果表中的证据（至少 2 条），并使用中文。\n"
        "3) 若用户问“为什么/原因”，你必须尝试基于证据给出可能驱动（可用拆解/下钻表作为证据）。\n"
        "4) 输出格式固定为：\n"
        "【结论】... \n"
        "【证据】- ...\n"
        "【可能原因】...（若仍不足以判断，说明缺什么字段/维度）\n"
        "【下一步SQL建议】...（给 1-2 个可执行方向）\n"
    )

    ev_text = ""
    for i, b in enumerate(evidence_blocks, start=1):
        ev_text += (
            f"\n【取证查询 {i}】用途：{b.get('purpose','')}\n"
            f"SQL：{b.get('sql','')}\n"
            f"结果字段：{b.get('columns','')}\n"
            f"结果行数：{b.get('row_count','')}\n"
            "结果表（CSV，已截断）：\n"
            f"{b.get('csv','')}\n"
        )

    user = (
        "用户问题：\n"
        f"{question}\n\n"
        "主查询 SQL：\n"
        f"{main_sql}\n\n"
        f"主查询结果行数：{main_row_count}\n"
        f"主查询结果字段：{', '.join(main_columns)}\n\n"
        "主查询结果表（CSV，已截断）：\n"
        f"{main_table_preview_csv}\n"
        f"{ev_text}\n"
    )
    return AnalysisPrompt(system=system, user=user)


from __future__ import annotations

from dataclasses import dataclass

from profit_analyst_mvp.schema import TableSchema


@dataclass(frozen=True)
class SqlPrompt:
    system: str
    user: str


@dataclass(frozen=True)
class AnalysisPrompt:
    system: str
    user: str


def build_sql_prompt(*, question: str, schema: TableSchema, days_window: int = 30) -> SqlPrompt:
    # 目标：稳定产出可执行 SQL；强约束只输出 SQL 文本
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
        "表与字段：\n"
        f"{schema.to_prompt_text()}\n"
    )

    user = (
        "用户问题：\n"
        f"{question}\n"
        "\n"
        "只输出 SQL："
    )
    return SqlPrompt(system=system, user=user)


def build_sql_repair_prompt(*, question: str, schema: TableSchema, bad_sql: str, error: str, days_window: int = 30) -> SqlPrompt:
    system = (
        "你是资深数据分析工程师。你需要修复 SQL，使其满足硬规则并可在 SQLite 执行。\n"
        "必须严格遵守：只输出一条 SQL；只允许 SELECT；禁止 JOIN；仅用白名单字段；FROM 仅允许单表。\n"
        f"N={days_window}\n"
        "\n"
        "表与字段：\n"
        f"{schema.to_prompt_text()}\n"
    )
    user = (
        "用户问题：\n"
        f"{question}\n\n"
        "上一条 SQL：\n"
        f"{bad_sql}\n\n"
        "报错/校验失败原因：\n"
        f"{error}\n\n"
        "请输出修复后的 SQL（仅 SQL）："
    )
    return SqlPrompt(system=system, user=user)


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


from __future__ import annotations

from dataclasses import dataclass

from profit_analyst_mvp.schema import TableSchema


@dataclass(frozen=True)
class SqlPrompt:
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


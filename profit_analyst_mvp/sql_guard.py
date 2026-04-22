from __future__ import annotations

import re
from dataclasses import dataclass


FORBIDDEN_KEYWORDS = {
    "drop",
    "delete",
    "insert",
    "update",
    "alter",
    "create",
    "replace",
    "attach",
    "detach",
    "pragma",
    "vacuum",
}


@dataclass(frozen=True)
class GuardResult:
    ok: bool
    reason: str = ""


def _strip_code_fences(s: str) -> str:
    s = (s or "").strip()
    # 兼容 LLM 输出 ```sql ... ```
    s = re.sub(r"^```(?:sql)?\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*```$", "", s)
    return s.strip()


def validate_sql(
    sql: str,
    *,
    allowed_tables: set[str],
    allowed_columns: set[str],
    allow_with: bool = True,
) -> GuardResult:
    sql = _strip_code_fences(sql)
    if not sql:
        return GuardResult(False, "SQL 为空。")

    # 多语句拦截
    parts = [p.strip() for p in sql.split(";") if p.strip()]
    if len(parts) != 1:
        return GuardResult(False, "禁止多语句 SQL（包含分号）。")
    sql = parts[0]

    low = sql.lower()

    # 只允许 SELECT / WITH
    if allow_with:
        if not (low.startswith("select") or low.startswith("with")):
            return GuardResult(False, "只允许 SELECT（可选 WITH）语句。")
    else:
        if not low.startswith("select"):
            return GuardResult(False, "只允许 SELECT 语句。")

    # 禁止 JOIN
    if re.search(r"\bjoin\b", low):
        return GuardResult(False, "本阶段禁止 JOIN。")

    # 禁止危险关键字
    for kw in FORBIDDEN_KEYWORDS:
        if re.search(rf"\b{re.escape(kw)}\b", low):
            return GuardResult(False, f"包含危险关键字：{kw}")

    # 表名限制：必须包含 FROM 且只能是允许的表
    m = re.search(r'\bfrom\s+("?[a-zA-Z0-9_.]+"?)', sql, flags=re.IGNORECASE)
    if not m:
        return GuardResult(False, "SQL 必须包含 FROM 子句。")
    table = m.group(1).strip('"')
    if table not in allowed_tables:
        return GuardResult(False, f"不允许的表名：{table}")

    # 字段白名单（尽力而为的轻量校验）：若出现未知标识符且不是常见函数/关键字/数字，拒绝
    tokens = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", sql)
    keywords_and_funcs = {
        "select",
        "from",
        "where",
        "group",
        "by",
        "order",
        "limit",
        "as",
        "and",
        "or",
        "not",
        "null",
        "is",
        "in",
        "like",
        "between",
        "distinct",
        "having",
        "case",
        "when",
        "then",
        "else",
        "end",
        "with",
        "avg",
        "sum",
        "min",
        "max",
        "count",
        "date",
        "strftime",
        "substr",
        "coalesce",
        "nullif",
        "round",
        "cast",
    }
    unknown: set[str] = set()
    for t in tokens:
        tl = t.lower()
        if tl in keywords_and_funcs:
            continue
        # 允许表别名/列别名：无法完全解析，这里只做“出现了不在列白名单里的字段名”拦截
        if t in allowed_columns:
            continue
        # 允许一些很常见的别名，不要误杀
        if tl in {"t", "x", "a", "b", "c"}:
            continue
        # 允许表名片段（dashbord_new_data1 / yibai_oversea 等）
        if any(t in tbl for tbl in allowed_tables):
            continue
        unknown.add(t)

    if unknown:
        # 仅提示一部分，避免太长
        sample = ", ".join(sorted(list(unknown))[:8])
        return GuardResult(False, f"疑似包含非白名单字段/标识符：{sample}")

    return GuardResult(True, "")


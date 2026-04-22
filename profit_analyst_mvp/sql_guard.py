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

def _strip_string_literals(sql: str) -> str:
    # 去除单引号字符串内容，避免把 'US' / 'Y' 等当作标识符
    # SQLite 单引号转义：'' 表示一个 '
    return re.sub(r"'([^']|'')*'", "''", sql)


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

    # 字段白名单（尽力而为的轻量校验）：
    # - 忽略字符串字面量
    # - 放行 AS 别名
    stripped = _strip_string_literals(sql)
    tokens = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", stripped)
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
        "asc",
        "desc",
        "now",
        "day",
        "days",
    }
    # 放行 SQL 中通过 AS 定义的别名（如 total_profit）
    aliases = {m.group(1) for m in re.finditer(r"\bas\s+([A-Za-z_][A-Za-z0-9_]*)\b", stripped, flags=re.IGNORECASE)}
    unknown: set[str] = set()
    for t in tokens:
        tl = t.lower()
        if tl in keywords_and_funcs:
            continue
        if t in aliases:
            continue
        # 允许表别名/列别名：无法完全解析，这里只做“出现了不在列白名单里的字段名”拦截
        if t in allowed_columns:
            continue
        # 允许一些很常见的别名，不要误杀
        if tl in {"t", "x", "a", "b", "c"} or len(t) <= 2:
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


from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass

import pandas as pd

from profit_analyst_mvp.db import query_df
from profit_analyst_mvp.llm import chat_completion, load_llm_config
from profit_analyst_mvp.prompts import build_sql_prompt, build_sql_repair_prompt
from profit_analyst_mvp.schema import DEFAULT_SQLITE_TABLE, load_table_schema, rewrite_to_sqlite_table
from profit_analyst_mvp.sql_guard import validate_sql


@dataclass(frozen=True)
class OrchestratorConfig:
    days_window: int = 30
    max_rows: int = 200
    allow_with: bool = True


def _apply_limit(sql: str, max_rows: int) -> str:
    # 统一做外层 LIMIT，避免 LLM 忘写或写得过大
    return f"SELECT * FROM ({sql}) LIMIT {int(max_rows)}"

def _normalize_sql_for_sqlite(sql: str) -> str:
    """
    将 LLM 可能生成的“非 SQLite 标准写法”做最小规范化：
    - 去除 code fence 与多余解释（sql_guard 已做一部分，这里额外兜底）
    - 将 ≥/≤/≠ 等 Unicode 运算符替换为 >=/<=/!=
    - 若包含语句终止符（; 或 ；），只保留第一条语句（禁止多语句）
    """
    s = (sql or "").strip()
    # 与 sql_guard 对齐的最小 code fence 清理
    if s.lower().startswith("```"):
        s = s.lstrip("`")
        s = s.replace("sql", "", 1).strip()
        s = s.rstrip("`").strip()

    s = (
        s.replace("≥", ">=")
        .replace("≤", "<=")
        .replace("≠", "!=")
    )

    # 只保留第一条语句
    cut = len(s)
    for sep in (";", "；"):
        idx = s.find(sep)
        if idx != -1:
            cut = min(cut, idx)
    s = s[:cut].strip()
    return s


def generate_sql(*, question: str, conn: sqlite3.Connection, cfg: OrchestratorConfig) -> str:
    schema = load_table_schema(conn, table=DEFAULT_SQLITE_TABLE)
    llm_cfg = load_llm_config()

    p = build_sql_prompt(question=question, schema=schema, days_window=cfg.days_window)
    sql = chat_completion(
        cfg=llm_cfg,
        messages=[
            {"role": "system", "content": p.system},
            {"role": "user", "content": p.user},
        ],
        temperature=0.0,
    )

    sql_norm = _normalize_sql_for_sqlite(sql)
    guard = validate_sql(
        sql_norm,
        allowed_tables=schema.allowed_table_aliases,
        allowed_columns=set(schema.columns),
        allow_with=cfg.allow_with,
    )
    if guard.ok:
        return sql_norm

    # 失败：做一次修复重试
    p2 = build_sql_repair_prompt(
        question=question,
        schema=schema,
        bad_sql=sql_norm,
        error=guard.reason,
        days_window=cfg.days_window,
    )
    sql2 = chat_completion(
        cfg=llm_cfg,
        messages=[
            {"role": "system", "content": p2.system},
            {"role": "user", "content": p2.user},
        ],
        temperature=0.0,
    )
    sql2_norm = _normalize_sql_for_sqlite(sql2)
    guard2 = validate_sql(
        sql2_norm,
        allowed_tables=schema.allowed_table_aliases,
        allowed_columns=set(schema.columns),
        allow_with=cfg.allow_with,
    )
    if not guard2.ok:
        raise ValueError(f"SQL 校验失败：{guard2.reason}")

    return sql2_norm


def run_question(*, question: str, conn: sqlite3.Connection, cfg: OrchestratorConfig | None = None) -> tuple[str, pd.DataFrame]:
    cfg = cfg or OrchestratorConfig(
        days_window=int((os.getenv("DEFAULT_DAYS_WINDOW") or "").strip() or "30"),
        max_rows=int((os.getenv("MAX_RESULT_ROWS") or "").strip() or "200"),
        allow_with=True,
    )

    sql = generate_sql(question=question, conn=conn, cfg=cfg)
    sql_exec = rewrite_to_sqlite_table(sql)
    sql_exec = _apply_limit(sql_exec, cfg.max_rows)
    df = query_df(conn, sql_exec)
    return sql, df


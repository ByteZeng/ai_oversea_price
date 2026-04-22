from __future__ import annotations

import sqlite3
from dataclasses import dataclass


DEFAULT_SQLITE_TABLE = "dashbord_new_data1"
ALLOWED_TABLE_ALIASES = {
    # 真实 SQLite 表
    "dashbord_new_data1",
    # 文档中的全限定名（LLM 可能会生成）
    "yibai_oversea.dashbord_new_data1",
}


@dataclass(frozen=True)
class TableSchema:
    table_sqlite: str
    allowed_table_aliases: set[str]
    columns: list[str]

    def to_prompt_text(self) -> str:
        cols = "\n".join([f"- {c}" for c in self.columns])
        aliases = ", ".join(sorted(self.allowed_table_aliases))
        return (
            "你只能查询 1 张表（禁止 JOIN）。\n"
            f"允许的表名（同义）：{aliases}\n"
            f"SQLite 实际表名：{self.table_sqlite}\n"
            "允许字段（白名单）：\n"
            f"{cols}\n"
        )


def load_table_schema(conn: sqlite3.Connection, table: str = DEFAULT_SQLITE_TABLE) -> TableSchema:
    cur = conn.cursor()
    cur.execute(f'PRAGMA table_info("{table}")')
    rows = cur.fetchall()
    if not rows:
        raise RuntimeError(f"未找到 SQLite 表：{table}（请先完成 M2 初始化）")
    # PRAGMA table_info: cid, name, type, notnull, dflt_value, pk
    cols = [r[1] for r in rows]
    return TableSchema(table_sqlite=table, allowed_table_aliases=set(ALLOWED_TABLE_ALIASES), columns=cols)


def rewrite_to_sqlite_table(sql: str, *, sqlite_table: str = DEFAULT_SQLITE_TABLE) -> str:
    # 将全限定表名重写为 SQLite 表名，便于实际执行
    return sql.replace("yibai_oversea.dashbord_new_data1", sqlite_table)


from __future__ import annotations

import argparse
import os
import sqlite3
from pathlib import Path


DEFAULT_DB = Path(__file__).parent / "data" / "app.db"
DEFAULT_TABLE = "dashbord_new_data1"


def _run(conn: sqlite3.Connection, sql: str) -> list[tuple]:
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


def main() -> None:
    parser = argparse.ArgumentParser(description="M2：验证 SQLite 初始化是否就绪（跑几条手工 SQL）。")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="SQLite db 文件路径")
    parser.add_argument("--table", default=DEFAULT_TABLE, help="表名（默认 dashbord_new_data1）")
    args = parser.parse_args()

    db_path = Path(os.path.expanduser(args.db)).resolve()
    table = args.table
    if not db_path.exists():
        raise FileNotFoundError(f"db 不存在：{db_path}（请先运行 db_init.py）")

    conn = sqlite3.connect(str(db_path))
    try:
        sqls = [
            (f"样例 1：查看前 5 行", f'SELECT * FROM "{table}" LIMIT 5'),
            (f"样例 2：统计总行数", f'SELECT COUNT(1) AS cnt FROM "{table}"'),
        ]
        for title, sql in sqls:
            print(f"\n== {title} ==")
            print(sql)
            rows = _run(conn, sql)
            for r in rows[:10]:
                print(r)
    finally:
        conn.close()


if __name__ == "__main__":
    main()


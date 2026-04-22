from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pandas as pd


def get_db_path() -> Path:
    # 优先环境变量，默认使用本目录 data/app.db
    p = (os.getenv("APP_DB_PATH") or "").strip()
    if p:
        return Path(os.path.expanduser(p)).resolve()
    return (Path(__file__).parent / "data" / "app.db").resolve()


def connect(db_path: Path | None = None) -> sqlite3.Connection:
    path = db_path or get_db_path()
    if not path.exists():
        raise FileNotFoundError(f"SQLite db 不存在：{path}（请先运行 M2 初始化 db_init.py）")
    return sqlite3.connect(str(path))


def query_df(conn: sqlite3.Connection, sql: str) -> pd.DataFrame:
    return pd.read_sql_query(sql, conn)


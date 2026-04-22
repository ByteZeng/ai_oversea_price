from __future__ import annotations

import argparse
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd


DEFAULT_TABLE = "dashbord_new_data1"


@dataclass(frozen=True)
class InitResult:
    db_path: Path
    table: str
    rows: int
    cols: int


def _normalize_column(col: str) -> str:
    # SQLite 列名尽量使用英文/下划线；此处做最小规范化，避免空格/特殊字符导致 SQL 难写
    c = (col or "").strip()
    c = c.replace(" ", "_").replace("-", "_").replace(".", "_").replace("/", "_")
    return c


def _dedupe_columns(cols: Iterable[str]) -> list[str]:
    seen: dict[str, int] = {}
    out: list[str] = []
    for c in cols:
        base = _normalize_column(c)
        if not base:
            base = "col"
        if base not in seen:
            seen[base] = 0
            out.append(base)
        else:
            seen[base] += 1
            out.append(f"{base}_{seen[base]}")
    return out


def _read_excel(excel_path: Path, sheet: str | None) -> pd.DataFrame:
    if not excel_path.exists():
        raise FileNotFoundError(f"Excel 文件不存在：{excel_path}")
    # pandas: sheet_name=None -> 返回 dict[sheet_name, DataFrame]
    # M2 默认读取第一个 sheet；如需指定，传入 --sheet
    sheet_name = 0 if sheet is None else sheet
    df_or_dict = pd.read_excel(excel_path, sheet_name=sheet_name)
    if isinstance(df_or_dict, dict):
        if not df_or_dict:
            raise ValueError("Excel 未读取到任何 sheet。")
        df = next(iter(df_or_dict.values()))
    else:
        df = df_or_dict

    if df is None or df.empty:
        raise ValueError("Excel 读取为空（可能是 sheet 选错或文件无数据）。")
    df.columns = _dedupe_columns([str(c) for c in df.columns])
    return df


def init_sqlite_from_excel(
    *,
    excel_path: Path,
    db_path: Path,
    table: str = DEFAULT_TABLE,
    sheet: str | None = None,
) -> InitResult:
    df = _read_excel(excel_path, sheet)

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        # 统一转字符串写入，避免 Excel 混合类型导致写库失败；
        # M2 的目标是“可用数据就绪 + 手工 SQL 能跑”，类型精修放在后续里程碑。
        df = df.copy()
        for c in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[c]):
                df[c] = df[c].dt.strftime("%Y-%m-%d %H:%M:%S")
            df[c] = df[c].astype("string")

        # 覆盖式初始化（可重复执行）
        df.to_sql(table, conn, if_exists="replace", index=False)

        return InitResult(db_path=db_path, table=table, rows=int(df.shape[0]), cols=int(df.shape[1]))
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="M2：从模拟订单 Excel 初始化 SQLite 数据库。")
    parser.add_argument("--excel", required=True, help="模拟订单数据 Excel 路径（.xlsx）")
    parser.add_argument("--db", default=str(Path(__file__).parent / "data" / "app.db"), help="SQLite db 文件路径")
    parser.add_argument("--table", default=DEFAULT_TABLE, help="写入的表名（默认 dashbord_new_data1）")
    parser.add_argument("--sheet", default=None, help="Excel sheet 名（可选）")
    args = parser.parse_args()

    excel_path = Path(os.path.expanduser(args.excel)).resolve()
    db_path = Path(os.path.expanduser(args.db)).resolve()

    res = init_sqlite_from_excel(excel_path=excel_path, db_path=db_path, table=args.table, sheet=args.sheet)
    print(f"SQLite 初始化完成：db={res.db_path} table={res.table} rows={res.rows} cols={res.cols}")


if __name__ == "__main__":
    main()


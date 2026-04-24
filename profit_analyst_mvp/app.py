import pandas as pd
import streamlit as st

from profit_analyst_mvp.db import connect, get_db_path
from profit_analyst_mvp.orchestrator import generate_conclusion, run_query

APP_TITLE = "AI 数据分析助手（第一阶段）"
APP_SUBTITLE = "演示级：问题 → SQL（校验）→ SQLite → 结论"

SAMPLE_QUESTIONS: list[str] = [
    "为什么最近美国站利润率下降？",
    "近 30 天 vs 前 30 天，哪些国家利润下降最多？",
    "最近 30 天利润/利润率的主要变化来自销量变化还是成本变化？",
]


def _init_state() -> None:
    if "last_run" not in st.session_state:
        st.session_state.last_run = None
        st.session_state.question = ""
        st.session_state.sql = ""
        st.session_state.result_df = pd.DataFrame()
        st.session_state.conclusion = ""
        st.session_state.error = ""

    if "ui_show_debug" not in st.session_state:
        st.session_state.ui_show_debug = False
    if "ui_auto_run_conclusion" not in st.session_state:
        st.session_state.ui_auto_run_conclusion = False
    if "ui_result_rows" not in st.session_state:
        st.session_state.ui_result_rows = 50


def _reset() -> None:
    st.session_state.last_run = None
    st.session_state.question = ""
    st.session_state.sql = ""
    st.session_state.result_df = pd.DataFrame()
    st.session_state.conclusion = ""
    st.session_state.error = ""


def _run_query_step() -> None:
    question = (st.session_state.question or "").strip()
    if not question:
        raise ValueError("请输入问题后再开始分析。")

    conn = connect()
    try:
        sql, df = run_query(question=question, conn=conn)
    finally:
        conn.close()

    st.session_state.sql = sql
    st.session_state.result_df = df
    st.session_state.conclusion = ""
    st.session_state.error = ""
    st.session_state.last_run = pd.Timestamp.now().isoformat(timespec="seconds")


def _run_conclusion_step() -> None:
    question = (st.session_state.question or "").strip()
    if not question:
        raise ValueError("请先输入问题。")
    if not st.session_state.sql:
        raise ValueError("请先生成并执行 SQL。")
    if st.session_state.result_df is None or st.session_state.result_df.empty:
        raise ValueError("查询结果为空，无法生成结论。请调整问题或时间窗后重试。")

    conclusion = generate_conclusion(
        question=question,
        sql=st.session_state.sql,
        df=st.session_state.result_df,
    )
    st.session_state.conclusion = conclusion
    st.session_state.error = ""
    st.session_state.last_run = pd.Timestamp.now().isoformat(timespec="seconds")


def main() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")

    _init_state()

    st.title(APP_TITLE)
    st.caption(APP_SUBTITLE)

    with st.sidebar:
        st.subheader("快捷操作")
        st.caption(f"SQLite：`{get_db_path()}`")

        picked = st.selectbox("示例问题（可直接用来演示）", SAMPLE_QUESTIONS, index=0)
        fill_clicked = st.button("一键填入示例问题", use_container_width=True)
        if fill_clicked and picked:
            st.session_state.question = picked

        st.divider()
        st.subheader("展示设置")
        st.session_state.ui_result_rows = st.slider(
            "结果表预览行数",
            min_value=10,
            max_value=200,
            value=int(st.session_state.ui_result_rows),
            step=10,
        )
        st.session_state.ui_auto_run_conclusion = st.toggle("分析后自动生成结论", value=bool(st.session_state.ui_auto_run_conclusion))
        st.session_state.ui_show_debug = st.toggle("显示调试信息", value=bool(st.session_state.ui_show_debug))

        st.divider()
        reset_clicked = st.button("重置全部", use_container_width=True)

    if reset_clicked:
        _reset()
        st.rerun()

    with st.container(border=True):
        st.subheader("提问")
        st.session_state.question = st.text_area(
            "请输入你的问题",
            value=st.session_state.question,
            placeholder="例如：为什么最近美国站利润率下降？",
            height=110,
        )

        btn_cols = st.columns([1.2, 1.2, 1.6, 4])
        with btn_cols[0]:
            run_clicked = st.button("1) 生成SQL并查询", type="primary", use_container_width=True)
        with btn_cols[1]:
            analyze_clicked = st.button("2) 生成结论", use_container_width=True)
        with btn_cols[2]:
            run_all_clicked = st.button("一键运行（查询+结论）", use_container_width=True)

    # 执行逻辑（集中处理错误，让页面更干净）
    if run_clicked or run_all_clicked:
        try:
            with st.spinner("正在生成 SQL 并查询 SQLite…"):
                _run_query_step()
            if st.session_state.ui_auto_run_conclusion or run_all_clicked:
                with st.spinner("正在生成中文结论…"):
                    _run_conclusion_step()
        except Exception as e:
            st.session_state.error = str(e)

    if analyze_clicked:
        try:
            with st.spinner("正在生成中文结论…"):
                _run_conclusion_step()
        except Exception as e:
            st.session_state.error = str(e)

    # 主展示区：三张卡片（SQL / 结果 / 结论）
    st.divider()

    c1, c2, c3 = st.columns([1.1, 1.5, 1.4])

    with c1:
        with st.container(border=True):
            st.subheader("SQL（只读）")
            st.code(st.session_state.sql or "-- 先点击「生成SQL并查询」", language="sql")
            if st.session_state.ui_show_debug:
                with st.expander("调试：当前问题文本", expanded=False):
                    st.write(st.session_state.question or "（空）")

    with c2:
        with st.container(border=True):
            st.subheader("查询结果表")
            df = st.session_state.result_df
            if df is None or df.empty:
                st.info("暂无结果。先生成 SQL 并查询。")
            else:
                preview = df.head(int(st.session_state.ui_result_rows))
                st.dataframe(preview, use_container_width=True, hide_index=True)
                if len(df) > len(preview):
                    st.caption(f"仅预览前 {len(preview)} 行（总 {len(df)} 行）。")

    with c3:
        with st.container(border=True):
            st.subheader("中文结论")
            if st.session_state.conclusion:
                st.write(st.session_state.conclusion)
            else:
                st.info("尚未生成结论。确认结果表无误后点击「生成结论」。")

            if st.session_state.ui_show_debug:
                with st.expander("调试：最后一次运行时间", expanded=False):
                    st.write(st.session_state.last_run or "—")

    # 错误与状态区（默认不抢占主内容）
    st.divider()
    if st.session_state.error:
        st.error(st.session_state.error)
    else:
        st.success("就绪。")


if __name__ == "__main__":
    main()


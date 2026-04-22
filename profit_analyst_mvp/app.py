import pandas as pd
import streamlit as st

from profit_analyst_mvp.db import connect, get_db_path
from profit_analyst_mvp.orchestrator import run_question

APP_TITLE = "AI 数据分析助手（第一阶段）"


def main() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")

    st.title(APP_TITLE)
    st.caption("M3：LLM SQL 通路（问题 → 生成 SQL → 校验 → SQLite 执行）")
    st.caption(f"SQLite：`{get_db_path()}`")

    if "last_run" not in st.session_state:
        st.session_state.last_run = None
        st.session_state.question = ""
        st.session_state.sql = ""
        st.session_state.result_df = pd.DataFrame()
        st.session_state.conclusion = ""
        st.session_state.error = ""

    with st.container(border=True):
        st.subheader("输入")
        st.session_state.question = st.text_area(
            "请输入你的问题",
            value=st.session_state.question,
            placeholder="例如：为什么最近美国站利润率下降？",
            height=100,
        )

        cols = st.columns([1, 1, 3])
        with cols[0]:
            run_clicked = st.button("开始分析", type="primary", use_container_width=True)
        with cols[1]:
            reset_clicked = st.button("重置", use_container_width=True)

    if reset_clicked:
        st.session_state.last_run = None
        st.session_state.question = ""
        st.session_state.sql = ""
        st.session_state.result_df = pd.DataFrame()
        st.session_state.conclusion = ""
        st.session_state.error = ""
        st.rerun()

    if run_clicked:
        try:
            question = (st.session_state.question or "").strip()
            if not question:
                raise ValueError("请输入问题后再点击开始分析。")

            conn = connect()
            try:
                sql, df = run_question(question=question, conn=conn)
            finally:
                conn.close()

            st.session_state.sql = sql
            st.session_state.result_df = df
            st.session_state.conclusion = "（M3 阶段）已返回 SQL 查询结果；M4 将补齐基于表格的中文结论。"
            st.session_state.error = ""
            st.session_state.last_run = pd.Timestamp.now().isoformat(timespec="seconds")
        except Exception as e:
            st.session_state.error = str(e)

    st.divider()

    left, right = st.columns([1, 1])
    with left:
        st.subheader("问题")
        st.write(st.session_state.question or "（未输入）")

        st.subheader("SQL")
        st.code(st.session_state.sql or "-- 点击「开始分析」后生成 SQL", language="sql")

    with right:
        st.subheader("查询结果表")
        if st.session_state.result_df is None or st.session_state.result_df.empty:
            st.info("暂无结果。点击「开始分析」生成 SQL 并执行查询。")
        else:
            st.dataframe(st.session_state.result_df, use_container_width=True, hide_index=True)

        st.subheader("结论")
        st.write(st.session_state.conclusion or "（暂无结论）")

    st.divider()
    st.subheader("错误信息")
    if st.session_state.error:
        st.error(st.session_state.error)
    else:
        st.success("无错误。")

    st.caption(f"最后一次运行：{st.session_state.last_run or '—'}")


if __name__ == "__main__":
    main()


import pandas as pd
import streamlit as st

from profit_analyst_mvp.db import connect, get_db_path
from profit_analyst_mvp.orchestrator import (
    EvidenceQuery,
    FollowupPlanItem,
    generate_conclusion,
    generate_conclusion_with_evidence,
    run_sql,
    run_query,
    run_query_with_evidence,
)

APP_TITLE = "AI 数据分析助手"
APP_SUBTITLE = "演示级：问题 → SQL + 结果 → 结论"

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
    if "ui_auto_evidence" not in st.session_state:
        st.session_state.ui_auto_evidence = True
    if "ui_result_rows" not in st.session_state:
        st.session_state.ui_result_rows = 50
    if "evidence" not in st.session_state:
        st.session_state.evidence = []
    if "ui_chat_mode" not in st.session_state:
        st.session_state.ui_chat_mode = True
    if "ui_followup_n" not in st.session_state:
        st.session_state.ui_followup_n = 2
    if "chat_turns" not in st.session_state:
        st.session_state.chat_turns = []
    if "workbench_question" not in st.session_state:
        st.session_state.workbench_question = ""
    if "workbench_sql" not in st.session_state:
        st.session_state.workbench_sql = ""
    if "workbench_df" not in st.session_state:
        st.session_state.workbench_df = pd.DataFrame()
    if "workbench_conclusion" not in st.session_state:
        st.session_state.workbench_conclusion = ""
    if "ui_has_started" not in st.session_state:
        st.session_state.ui_has_started = False
    if "start_question" not in st.session_state:
        st.session_state.start_question = ""


def _reset() -> None:
    st.session_state.last_run = None
    st.session_state.question = ""
    st.session_state.sql = ""
    st.session_state.result_df = pd.DataFrame()
    st.session_state.conclusion = ""
    st.session_state.error = ""
    st.session_state.evidence = []
    st.session_state.chat_turns = []
    st.session_state.workbench_question = ""
    st.session_state.workbench_sql = ""
    st.session_state.workbench_df = pd.DataFrame()
    st.session_state.workbench_conclusion = ""
    st.session_state.ui_has_started = False
    st.session_state.start_question = ""


def _run_query_step() -> None:
    question = (st.session_state.question or "").strip()
    if not question:
        raise ValueError("请输入问题后再开始分析。")

    conn = connect()
    try:
        sql, df = run_query(question=question, conn=conn, context=None)
    finally:
        conn.close()

    st.session_state.sql = sql
    st.session_state.result_df = df
    st.session_state.conclusion = ""
    st.session_state.error = ""
    st.session_state.evidence = []
    st.session_state.last_run = pd.Timestamp.now().isoformat(timespec="seconds")


def _run_conclusion_step() -> None:
    question = (st.session_state.question or "").strip()
    if not question:
        raise ValueError("请先输入问题。")
    if not st.session_state.sql:
        raise ValueError("请先生成并执行 SQL。")
    if st.session_state.result_df is None or st.session_state.result_df.empty:
        raise ValueError("查询结果为空，无法生成结论。请调整问题或时间窗后重试。")

    # 若开启自动取证：用“主查询 + follow-up 取证查询”来支持模型自我推理并引用证据
    if bool(st.session_state.ui_auto_evidence):
        conn = connect()
        try:
            main_sql, main_df, _plan, evidence = run_query_with_evidence(question=question, conn=conn, context=None)
        finally:
            conn.close()
        st.session_state.sql = main_sql
        st.session_state.result_df = main_df
        st.session_state.evidence = evidence
        conclusion = generate_conclusion_with_evidence(
            question=question,
            main_sql=main_sql,
            main_df=main_df,
            evidence=evidence,
        )
    else:
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
        st.session_state.ui_auto_evidence = st.toggle("自动取证推理（追加查询）", value=bool(st.session_state.ui_auto_evidence))
        st.session_state.ui_followup_n = st.slider(
            "每轮取证查询条数上限",
            min_value=0,
            max_value=4,
            value=int(st.session_state.ui_followup_n),
            step=1,
        )
        st.session_state.ui_chat_mode = st.toggle("聊天模式（多轮追问）", value=bool(st.session_state.ui_chat_mode))
        st.session_state.ui_show_debug = st.toggle("显示调试信息", value=bool(st.session_state.ui_show_debug))

        st.divider()
        reset_clicked = st.button("重置全部", use_container_width=True)

    if reset_clicked:
        _reset()
        st.rerun()

    # 首屏：只保留一个输入框；提交后再展示工作台/对话面板
    st.subheader("提问")
    st.caption("先输入你的问题。提交后会展开 SQL/结果/结论 的工作台面板。")
    st.session_state.start_question = st.text_input(
        "请输入问题",
        value=st.session_state.start_question,
        placeholder="例如：为什么最近美国站利润率上升了？",
    )
    start_clicked = st.button("开始", type="primary")

    if start_clicked:
        q0 = (st.session_state.start_question or "").strip()
        if not q0:
            st.session_state.error = "请输入问题后再开始。"
        else:
            st.session_state.ui_has_started = True
            # 同步填充工作台问题，便于直接生成 SQL
            st.session_state.workbench_question = q0
            st.session_state.error = ""
            st.rerun()

    if not bool(st.session_state.ui_has_started):
        # 初始界面到此结束
        st.divider()
        if st.session_state.error:
            st.error(st.session_state.error)
        else:
            st.info("输入问题后点击「开始」。")
        return

    # 工作台：SQL 可编辑 + 结果面板 + 结论面板（更像“IDE 面板布局”）
    st.divider()
    st.subheader("工作台")
    st.caption("三面板：SQL / 结果 / 结论。可反复修改 SQL 并运行。")

    panel_cols = st.columns([1.15, 1.55, 1.3])

    st.session_state.workbench_question = st.text_input(
        "当前问题（可修改后反复迭代）",
        value=st.session_state.workbench_question,
        placeholder="例如：为什么最近美国站利润率上升了？",
    )

    with panel_cols[0]:
        with st.container(border=True):
            st.markdown("**SQL 生成/编辑面板**")
            gen_clicked = st.button("从问题生成 SQL", type="primary", use_container_width=True)
            run_clicked = st.button("运行当前 SQL", use_container_width=True)
            st.session_state.workbench_sql = st.text_area(
                "SQL（可编辑，执行前会做只读校验）",
                value=st.session_state.workbench_sql,
                height=260,
                placeholder="点击“从问题生成 SQL”或粘贴/修改 SQL",
            )

            if gen_clicked:
                q = (st.session_state.workbench_question or "").strip()
                if not q:
                    st.session_state.error = "请先输入问题。"
                else:
                    try:
                        with st.spinner("正在生成 SQL…"):
                            conn = connect()
                            try:
                                sql, df = run_query(question=q, conn=conn, context=None)
                            finally:
                                conn.close()
                        st.session_state.workbench_sql = sql
                        st.session_state.workbench_df = df
                        st.session_state.workbench_conclusion = ""
                        st.session_state.error = ""
                    except Exception as e:
                        st.session_state.error = str(e)

            if run_clicked:
                sql_raw = (st.session_state.workbench_sql or "").strip()
                if not sql_raw:
                    st.session_state.error = "请先输入/生成 SQL。"
                else:
                    try:
                        with st.spinner("正在执行 SQL…"):
                            conn = connect()
                            try:
                                sql_norm, df = run_sql(sql=sql_raw, conn=conn)
                            finally:
                                conn.close()
                        st.session_state.workbench_sql = sql_norm
                        st.session_state.workbench_df = df
                        st.session_state.workbench_conclusion = ""
                        st.session_state.error = ""
                    except Exception as e:
                        st.session_state.error = str(e)

    with panel_cols[1]:
        with st.container(border=True):
            st.markdown("**查询结果面板**")
            df = st.session_state.workbench_df
            if df is None or df.empty:
                st.info("暂无结果。先生成/运行 SQL。")
            else:
                preview = df.head(int(st.session_state.ui_result_rows))
                st.dataframe(preview, use_container_width=True, hide_index=True)
                if len(df) > len(preview):
                    st.caption(f"仅预览前 {len(preview)} 行（总 {len(df)} 行）。")

    with panel_cols[2]:
        with st.container(border=True):
            st.markdown("**生成结果面板（结论）**")
            concl_clicked = st.button("生成结论（基于结果表）", use_container_width=True)
            if concl_clicked:
                q = (st.session_state.workbench_question or "").strip()
                if not q:
                    st.session_state.error = "请先输入问题。"
                elif not st.session_state.workbench_sql:
                    st.session_state.error = "请先生成/运行 SQL。"
                elif st.session_state.workbench_df is None or st.session_state.workbench_df.empty:
                    st.session_state.error = "查询结果为空，无法生成结论。"
                else:
                    try:
                        with st.spinner("正在生成中文结论…"):
                            st.session_state.workbench_conclusion = generate_conclusion(
                                question=q,
                                sql=st.session_state.workbench_sql,
                                df=st.session_state.workbench_df,
                            )
                            st.session_state.error = ""
                    except Exception as e:
                        st.session_state.error = str(e)

            if st.session_state.workbench_conclusion:
                st.write(st.session_state.workbench_conclusion)
            else:
                st.info("尚未生成结论。")

    if bool(st.session_state.ui_chat_mode):
        st.divider()
        st.subheader("对话")
        st.caption("像 Cursor 一样：可以围绕同一问题多轮追问；每轮可自动追加 0-N 条取证/下钻查询。")

        # 渲染历史轮次
        for t in st.session_state.chat_turns:
            with st.chat_message("user"):
                st.write(t.get("question", ""))

            with st.chat_message("assistant"):
                cols = st.columns([1.15, 1.55, 1.3])

                with cols[0]:
                    with st.container(border=True):
                        st.markdown("**SQL 面板**")
                        st.code(t.get("sql", "") or "--", language="sql")
                        if t.get("reasoning"):
                            with st.expander("推理步骤/计划", expanded=False):
                                for line in t["reasoning"]:
                                    st.write(line)
                        if t.get("plan"):
                            plan: list[FollowupPlanItem] = t["plan"]
                            with st.expander(f"取证计划（{len(plan)}）", expanded=False):
                                for i, p in enumerate(plan, start=1):
                                    st.markdown(f"**{i}. {p.purpose}**")
                                    st.code(p.sql, language="sql")
                                    if not getattr(p, "ok", True):
                                        st.warning(f"未执行/被拦截：{getattr(p, 'reason', '')}")

                with cols[1]:
                    with st.container(border=True):
                        st.markdown("**结果面板**")
                        df = t.get("df")
                        if df is None or getattr(df, "empty", True):
                            st.info("结果为空/未执行")
                        else:
                            preview = df.head(int(st.session_state.ui_result_rows))
                            st.dataframe(preview, use_container_width=True, hide_index=True)
                            if len(df) > len(preview):
                                st.caption(f"仅预览前 {len(preview)} 行（总 {len(df)} 行）。")

                with cols[2]:
                    with st.container(border=True):
                        st.markdown("**结论面板**")
                        if t.get("answer"):
                            st.write(t["answer"])
                        else:
                            st.info("尚未生成结论")
                        if t.get("evidence"):
                            evs: list[EvidenceQuery] = t["evidence"]
                            with st.expander(f"取证查询结果（{len(evs)}）", expanded=False):
                                for i, ev in enumerate(evs, start=1):
                                    st.markdown(f"**{i}. {getattr(ev, 'purpose', '取证')}**")
                                    st.code(getattr(ev, 'sql', ''), language="sql")
                                    ev_df = getattr(ev, "df", None)
                                    if ev_df is None or ev_df.empty:
                                        st.info("结果为空")
                                    else:
                                        st.dataframe(
                                            ev_df.head(int(st.session_state.ui_result_rows)),
                                            use_container_width=True,
                                            hide_index=True,
                                        )

        # 输入框（新一轮）
        q = st.chat_input("继续提问 / 追问（例如：按SKU拆分看看，哪些SKU拉升利润率？）")
        if q:
            # 拼接上下文：最近 1 轮的问答+SQL（避免塞太多）
            ctx = ""
            if st.session_state.chat_turns:
                last = st.session_state.chat_turns[-1]
                ctx = (
                    f"上一轮问题：{last.get('question','')}\n"
                    f"上一轮SQL：{last.get('sql','')}\n"
                    f"上一轮结论（摘要）：{str(last.get('answer',''))[:500]}\n"
                )

            try:
                cfg_followups = int(st.session_state.ui_followup_n)
                # 临时通过 env 控制（保持改动最小）
                import os as _os

                _os.environ["MAX_FOLLOWUP_QUERIES"] = str(cfg_followups)

                reasoning: list[str] = [
                    "1) 解析问题与上下文，确定口径与时间窗",
                    "2) 生成并执行主查询（得到现象/对比结果）",
                ]
                if bool(st.session_state.ui_auto_evidence) and cfg_followups > 0:
                    reasoning += [
                        f"3) 基于主查询结果生成取证/下钻 SQL（最多 {cfg_followups} 条）",
                        "4) 执行取证查询并收集证据",
                        "5) 基于主结果 + 证据综合输出结论",
                    ]
                else:
                    reasoning += ["3) 基于主查询结果输出结论（不追加取证查询）"]

                with st.chat_message("assistant"):
                    with st.status("推理中…", expanded=True) as status:
                        for line in reasoning:
                            status.write(line)

                        conn = connect()
                        try:
                            status.update(label="执行主查询…", state="running")
                            if bool(st.session_state.ui_auto_evidence) and cfg_followups > 0:
                                main_sql, main_df, plan, evidence = run_query_with_evidence(question=q, conn=conn, context=ctx)
                                status.write(f"已生成取证计划：{len(plan)} 条；已执行：{len(evidence)} 条")
                                status.update(label="汇总结论（含证据）…", state="running")
                                answer = generate_conclusion_with_evidence(
                                    question=q,
                                    main_sql=main_sql,
                                    main_df=main_df,
                                    evidence=evidence,
                                )
                            else:
                                main_sql, main_df = run_query(question=q, conn=conn, context=ctx)
                                plan = []
                                evidence = []
                                status.update(label="汇总结论…", state="running")
                                answer = generate_conclusion(question=q, sql=main_sql, df=main_df)
                        finally:
                            conn.close()

                        status.update(label="完成", state="complete")

                st.session_state.chat_turns.append(
                    {
                        "question": q,
                        "sql": main_sql,
                        "df": main_df,
                        "answer": answer,
                        "reasoning": reasoning,
                        "plan": plan,
                        "evidence": evidence,
                    }
                )
                st.rerun()
            except Exception as e:
                st.session_state.error = str(e)
    else:
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

    if not bool(st.session_state.ui_chat_mode):
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

                # 追加证据查询展示（可折叠）
                evidence = st.session_state.evidence or []
                if evidence:
                    with st.expander(f"取证查询结果（{len(evidence)}）", expanded=False):
                        for i, ev in enumerate(evidence, start=1):
                            st.markdown(f"**{i}. {getattr(ev, 'purpose', '取证')}**")
                            st.code(getattr(ev, "sql", ""), language="sql")
                            ev_df = getattr(ev, "df", None)
                            if ev_df is None or ev_df.empty:
                                st.info("结果为空")
                            else:
                                st.dataframe(ev_df.head(int(st.session_state.ui_result_rows)), use_container_width=True, hide_index=True)

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


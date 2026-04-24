from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class FieldHint:
    column: str
    zh: str
    aliases: list[str]
    type: str
    remark: str | None


@dataclass(frozen=True)
class MetricHint:
    metric_key: str
    metric_name: str
    aliases: list[str]
    definition_type: str
    formula_sql: str
    required_fields: list[str]
    unit: str
    value_type: str
    status: str


def _repo_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _read_json(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_field_dictionary() -> list[FieldHint]:
    path = os.path.join(_repo_root(), "profit_analyst_mvp", "docs", "dict", "field_dictionary.json")
    obj = _read_json(path)
    fields = obj.get("fields") or []
    out: list[FieldHint] = []
    for it in fields:
        if not isinstance(it, dict):
            continue
        column = str(it.get("column") or "").strip()
        if not column:
            continue
        out.append(
            FieldHint(
                column=column,
                zh=str(it.get("zh") or "").strip(),
                aliases=[str(x).strip() for x in (it.get("aliases") or []) if str(x).strip()],
                type=str(it.get("type") or "unknown").strip() or "unknown",
                remark=(str(it.get("remark")).strip() if it.get("remark") is not None else None),
            )
        )
    return out


def load_metric_dictionary(*, only_active: bool = True) -> list[MetricHint]:
    path = os.path.join(_repo_root(), "profit_analyst_mvp", "docs", "dict", "metric_dictionary.json")
    obj = _read_json(path)
    metrics = obj.get("metrics") or []
    out: list[MetricHint] = []
    for it in metrics:
        if not isinstance(it, dict):
            continue
        status = str(it.get("status") or "active").strip() or "active"
        if only_active and status != "active":
            continue
        key = str(it.get("metric_key") or "").strip()
        if not key:
            continue
        out.append(
            MetricHint(
                metric_key=key,
                metric_name=str(it.get("metric_name") or "").strip(),
                aliases=[str(x).strip() for x in (it.get("aliases") or []) if str(x).strip()],
                definition_type=str(it.get("definition_type") or "").strip(),
                formula_sql=str(it.get("formula_sql") or "").strip(),
                required_fields=[str(x).strip() for x in (it.get("required_fields") or []) if str(x).strip()],
                unit=str(it.get("unit") or "").strip(),
                value_type=str(it.get("value_type") or "").strip(),
                status=status,
            )
        )
    return out


def _norm(s: str) -> str:
    return (s or "").strip().lower()


def retrieve_field_hints(question: str, fields: list[FieldHint], *, limit: int = 12) -> list[FieldHint]:
    q = _norm(question)
    scored: list[tuple[int, FieldHint]] = []
    keywords = ["平台", "国家", "站点", "店铺", "sku", "日期", "时间", "订单", "币种", "汇率"]
    for f in fields:
        score = 0
        for a in [f.column, f.zh, *f.aliases]:
            aa = _norm(a)
            if not aa:
                continue
            if aa in q:
                # 命中 alias：加分；越长越可能更具体
                score = max(score, 10 + min(len(aa), 20))
            else:
                # 兜底：关键词级匹配（适配“按平台/按国家/近30天”这类表达）
                for kw in keywords:
                    k = _norm(kw)
                    if k and (k in aa) and (k in q):
                        score = max(score, 8 + min(len(k), 6))
        if score > 0:
            scored.append((score, f))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [x[1] for x in scored[: int(limit)]]


def retrieve_metric_hints(question: str, metrics: list[MetricHint], *, limit: int = 8) -> list[MetricHint]:
    q = _norm(question)
    scored: list[tuple[int, MetricHint]] = []
    keywords = ["利润率", "毛利率", "净利率", "利润", "毛利", "gmv", "销售额", "成交额", "订单数", "订单量", "客单价"]
    for m in metrics:
        score = 0
        # metric_name / metric_key 也参与召回
        for a in [m.metric_key, m.metric_name, *m.aliases]:
            aa = _norm(a)
            if not aa:
                continue
            if aa in q:
                score = max(score, 10 + min(len(aa), 20))
            else:
                # 兜底：关键词级匹配（适配“为什么利润率下降”这类简写）
                for kw in keywords:
                    k = _norm(kw)
                    if k and (k in aa) and (k in q):
                        score = max(score, 8 + min(len(k), 6))
        if score > 0:
            scored.append((score, m))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [x[1] for x in scored[: int(limit)]]


def field_hints_to_prompt_text(hints: list[FieldHint]) -> str:
    if not hints:
        return "（未从字段对照表中召回到明确相关字段）\n"
    lines: list[str] = []
    for h in hints:
        remark = f"；备注：{h.remark}" if h.remark else ""
        lines.append(f"- {h.column}：{h.zh}{remark}")
    return "\n".join(lines) + "\n"


def metric_hints_to_prompt_text(hints: list[MetricHint]) -> str:
    if not hints:
        return "（未从指标字典中召回到明确相关指标）\n"
    lines: list[str] = []
    for h in hints:
        req = ", ".join(h.required_fields) if h.required_fields else "（无）"
        lines.append(
            f"- {h.metric_key}（{h.metric_name}，{h.value_type}，{h.unit}）：{h.formula_sql}；required_fields=[{req}]"
        )
    return "\n".join(lines) + "\n"


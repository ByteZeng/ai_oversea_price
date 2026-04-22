from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv
from openai import OpenAI
from typing import Any, cast


@dataclass(frozen=True)
class LLMConfig:
    api_key: str
    base_url: str | None
    model: str
    timeout_s: float


def _load_dotenv_files() -> None:
    # 允许将密钥放在本地 .env 文件中（不会入库）
    # 优先加载仓库根 .env，其次加载 profit_analyst_mvp/.env
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    load_dotenv(os.path.join(repo_root, ".env"), override=False)
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"), override=False)


def _load_common_config(*, model_env_key: str, model_compat_keys: list[str]) -> LLMConfig:
    _load_dotenv_files()

    # 通用命名（推荐）
    api_key = (os.getenv("LLM_API_KEY") or "").strip()
    base_url = (os.getenv("LLM_BASE_URL") or "").strip() or None
    model = (os.getenv(model_env_key) or "").strip()
    timeout_raw = (os.getenv("LLM_TIMEOUT_S") or "").strip()

    # 兼容 DeepSeek 命名
    if not api_key:
        api_key = (os.getenv("DEEPSEEK_API_KEY") or "").strip()
    if base_url is None:
        base_url = (os.getenv("DEEPSEEK_BASE_URL") or "").strip() or None
    if not model:
        for k in model_compat_keys:
            v = (os.getenv(k) or "").strip()
            if v:
                model = v
                break
    if not timeout_raw:
        timeout_raw = (os.getenv("DEEPSEEK_TIMEOUT_S") or "").strip()

    # 兼容 OpenAI 命名
    if not api_key:
        api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if base_url is None:
        base_url = (os.getenv("OPENAI_BASE_URL") or "").strip() or None
    if not model:
        v = (os.getenv("OPENAI_MODEL") or "").strip()
        if v:
            model = v

    if not api_key:
        raise RuntimeError("缺少 LLM 配置：请在 `.env` 中设置 `LLM_API_KEY`（或使用 DEEPSEEK_API_KEY / OPENAI_API_KEY 兼容名）。")

    model = model or "deepseek-chat"
    timeout_s = float(timeout_raw or "60")
    return LLMConfig(api_key=api_key, base_url=base_url, model=model, timeout_s=timeout_s)

def load_llm_config() -> LLMConfig:
    # LLM #1（SQL 生成）
    return _load_common_config(model_env_key="LLM_MODEL", model_compat_keys=["DEEPSEEK_MODEL"])


def load_llm_analysis_config() -> LLMConfig:
    # LLM #2（结论生成）
    # 优先使用 LLM_MODEL_ANALYSIS，否则回退到 LLM_MODEL
    cfg = _load_common_config(model_env_key="LLM_MODEL_ANALYSIS", model_compat_keys=["DEEPSEEK_MODEL_ANALYSIS"])
    if (os.getenv("LLM_MODEL_ANALYSIS") or "").strip():
        return cfg
    # 未设置时，回退到主模型（保持同一 key/base_url/timeout）
    base = load_llm_config()
    return LLMConfig(api_key=base.api_key, base_url=base.base_url, model=base.model, timeout_s=base.timeout_s)


def create_client(cfg: LLMConfig) -> OpenAI:
    # DeepSeek 采用 OpenAI 兼容接口；base_url 可选
    if cfg.base_url:
        return OpenAI(api_key=cfg.api_key, base_url=cfg.base_url, timeout=cfg.timeout_s)
    return OpenAI(api_key=cfg.api_key, timeout=cfg.timeout_s)


def chat_completion(*, cfg: LLMConfig, messages: list[dict[str, str]], temperature: float = 0.0) -> str:
    client = create_client(cfg)
    resp = client.chat.completions.create(
        model=cfg.model,
        # openai SDK 类型较严格；我们使用最小 dict 结构即可运行
        messages=cast(Any, messages),
        temperature=temperature,
    )
    content = resp.choices[0].message.content or ""
    return content.strip()


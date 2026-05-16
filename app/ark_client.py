"""火山方舟 Responses API（强制 web_search），供个股 AI 摘要等调用。"""
from __future__ import annotations

import logging
from typing import Any

from app.config import settings

_log = logging.getLogger(__name__)

_WEB_SEARCH_TOOLS: list[dict[str, str]] = [{"type": "web_search"}]
_WEB_SEARCH_TOOL_CHOICE: dict[str, str] = {"type": "web_search"}


def _messages_to_response_input(messages: list[dict[str, str]]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for m in messages:
        role = (str(m.get("role") or "user")).strip().lower()
        if role not in ("user", "system", "developer", "assistant"):
            role = "user"
        raw = m.get("content")
        text_v = str(raw).strip() if raw is not None else ""
        if not text_v:
            continue
        items.append({"type": "message", "role": role, "content": text_v})
    if not items:
        items.append({"type": "message", "role": "user", "content": ""})
    return items


def _text_from_ark_response(resp: Any) -> str | None:
    parts: list[str] = []
    for item in getattr(resp, "output", None) or []:
        if getattr(item, "type", None) != "message":
            continue
        if getattr(item, "role", None) != "assistant":
            continue
        for block in getattr(item, "content", None) or []:
            if getattr(block, "type", None) == "output_text":
                t = getattr(block, "text", None)
                if t:
                    parts.append(str(t))
    out = "".join(parts).strip()
    return out if out else None


def _response_error_detail(resp: Any) -> str | None:
    err = getattr(resp, "error", None)
    if err is None:
        return None
    code = getattr(err, "code", None) or ""
    msg = getattr(err, "message", None) or ""
    s = f"{code}: {msg}".strip(": ").strip()
    return s or None


def ark_chat_completion(
    messages: list[dict[str, str]],
    *,
    temperature: float = 0.7,
    timeout_sec: float = 120.0,
    instructions: str | None = None,
) -> tuple[str | None, str | None]:
    """调用方舟 Responses API：强制 web_search，返回 (正文, 错误信息)。"""
    api_key = (settings.ark_api_key or "").strip()
    model = (settings.ark_model or "").strip()
    base_url = (settings.ark_base_url or "").strip().rstrip("/") or "https://ark.cn-beijing.volces.com/api/v3"
    if not api_key:
        return None, "未配置 ARK_API_KEY（火山方舟 API Key）"
    if not model:
        return None, "未配置 ARK_MODEL（推理接入点 ID，如 ep-xxxx）"

    try:
        from volcenginesdkarkruntime import Ark
    except ImportError as e:
        _log.warning("volcenginesdkarkruntime 未安装: %s", e)
        return None, '请安装依赖：pip install "volcengine-python-sdk[ark]"'

    client = Ark(base_url=base_url, api_key=api_key)
    if not hasattr(client, "responses"):
        return None, "当前 volcengine-python-sdk[ark] 版本过旧，请升级以支持 Responses 联网（responses.create）"

    input_items = _messages_to_response_input(messages)
    try:
        resp = client.responses.create(
            model=model,
            input=input_items,
            instructions=instructions,
            temperature=float(temperature),
            timeout=float(timeout_sec),
            tools=_WEB_SEARCH_TOOLS,
            tool_choice=_WEB_SEARCH_TOOL_CHOICE,
            stream=False,
        )
    except Exception as e:
        _log.exception("Ark responses.create（web_search）失败")
        return None, str(e)

    status = getattr(resp, "status", None)
    if status == "failed":
        detail = _response_error_detail(resp)
        return None, detail or "Ark Response 状态为 failed"
    if status == "incomplete":
        inc = getattr(resp, "incomplete_details", None)
        reason = getattr(inc, "reason", None) if inc is not None else None
        detail = _response_error_detail(resp)
        suffix = f"；{detail}" if detail else ""
        return None, f"Ark Response 未完成（incomplete）{reason or ''}{suffix}".strip()

    text = _text_from_ark_response(resp)
    if not text:
        detail = _response_error_detail(resp)
        return None, detail or "Ark Response 无 assistant 正文（请确认接入点已开通联网/Web Search）"
    return text, None

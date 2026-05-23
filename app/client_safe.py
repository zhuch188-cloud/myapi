"""客户端面 API/页面错误脱敏；管理端日志请用 logging 保留原文。"""

from __future__ import annotations

import logging
import re
from typing import Any

_log = logging.getLogger(__name__)

# 对外固定句式（可按业务扩展 code → 文案）
_DEFAULT_PUBLIC = "操作未能完成，请稍后重试。若持续失败请联系管理员。"
_GENERIC_UNAVAILABLE = "数据暂不可用，请稍后重试。"

_RE_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", re.I)
_RE_IPV4 = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
_RE_URL = re.compile(r"https?://[^\s\"'<>]+", re.I)
_RE_WIN_PATH = re.compile(r"[A-Za-z]:\\[^\s\"'<>]+|/[\w./-]+\.(?:xlsx|xls|csv|db|env)\b", re.I)
_RE_ABS_UNIX = re.compile(r"(?:^|[\s\"'])(/[\w./-]{4,})")

_SENSITIVE_SUBSTRINGS = (
    "turso",
    "libsql",
    "hrana",
    "database",
    "db:",
    "数据库",
    "sqlalchemy",
    "sql",
    "odbc",
    "sql server",
    "winddb",
    "wind_sql",
    "render",
    "render.com",
    "smtp",
    "smtp_host",
    "smtp_user",
    "smtp_from",
    "server-data",
    "strategy_root",
    "traceback",
    "savepoint",
    "sqlite error",
    "operationalerror",
    "connection refused",
    "18456",
    "msodbcsql",
    "volces",
    "ark.cn",
    "tencent",
    "github",
    "password",
    "auth_token",
    "jwt_secret",
    "service provider",
    "internal endpoint",
    "environment variable",
)


def is_admin_api_path(path: str) -> bool:
    p = (path or "").strip().lower()
    return p.startswith("/api/admin") or p.startswith("/admin")


def is_client_surface_path(path: str) -> bool:
    """访客/终端用户可能访问的路由（相对管理端）。"""
    p = (path or "").strip().lower()
    if is_admin_api_path(p):
        return False
    if p.startswith("/health"):
        return True
    if p.startswith("/client") or p.startswith("/api/strategies"):
        return True
    if p.startswith("/api/auth") or p.startswith("/public/"):
        return True
    return False


def sanitize_client_message(raw: Any, *, fallback: str = _DEFAULT_PUBLIC) -> str:
    """移除或掩盖可能暴露基础设施/隐私的片段，供客户端展示。"""
    if raw is None:
        return fallback
    s = str(raw).strip()
    if not s:
        return fallback
    low = s.lower()
    if any(k in low for k in _SENSITIVE_SUBSTRINGS):
        return fallback
    s = _RE_EMAIL.sub("[已隐藏]", s)
    s = _RE_IPV4.sub("[已隐藏]", s)
    s = _RE_URL.sub("[链接已隐藏]", s)
    s = _RE_WIN_PATH.sub("[路径已隐藏]", s)
    s = _RE_ABS_UNIX.sub(" [路径已隐藏]", s)
    if len(s) > 240:
        s = s[:240] + "…"
    # 脱敏后仍像技术堆栈则退回通用句
    if any(k in s.lower() for k in _SENSITIVE_SUBSTRINGS):
        return fallback
    return s or fallback


def public_message(
    code: str = "error",
    *,
    fallback: str | None = None,
) -> str:
    """按业务码返回安全文案（不暴露内部实现）。"""
    table = {
        "error": _DEFAULT_PUBLIC,
        "unavailable": _GENERIC_UNAVAILABLE,
        "auth_failed": "用户名或密码错误",
        "forbidden": "无权执行此操作",
        "not_found": "未找到请求的资源",
        "rate_limit": "请求过于频繁，请稍后再试",
        "nav_unavailable": "净值数据暂不可用，请稍后重试",
        "import_unavailable": "导入暂不可用，请稍后重试",
    }
    return table.get(code, fallback or _DEFAULT_PUBLIC)


def log_client_safe_error(
    logger: logging.Logger,
    msg: str,
    exc: BaseException | None = None,
    **extra: Any,
) -> None:
    """管理端/服务端详细日志（可含路径、服务商错误）。"""
    if exc is not None:
        logger.exception(msg, exc_info=exc, extra=extra or None)
    else:
        logger.error(msg, extra=extra or None)


def client_http_detail(
    exc: BaseException,
    *,
    code: str = "error",
    request_path: str | None = None,
) -> str:
    """
    若 request_path 为客户端面，返回脱敏文案并在服务端记原文；
    管理端 API 可返回较完整信息（仍避免密码/令牌）。
    """
    raw = str(exc).strip() or repr(exc)
    if request_path and is_admin_api_path(request_path):
        return sanitize_client_message(raw, fallback=raw[:500] or _DEFAULT_PUBLIC)
    log_client_safe_error(
        _log,
        f"client-surface error path={request_path or '?'}",
        exc,
    )
    return public_message(code)

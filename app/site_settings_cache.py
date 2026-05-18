"""前台 site_settings 内存快照：公开接口在 Turso 流锁繁忙时仍可响应。"""

from __future__ import annotations

import threading
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

_lock = threading.Lock()
_snapshot: dict[str, str] | None = None

CLIENT_LOGIN_KEY = "client_require_login"
CLIENT_REGISTER_KEY = "client_allow_register"
CLIENT_CONTACT_KEY = "client_contact_enabled"
CLIENT_FEEDBACK_KEY = "client_feedback_enabled"


def _bool_val(raw: str, default: str) -> bool:
    v = (raw if raw is not None else default).strip().lower()
    return v not in ("0", "false", "no", "off")


def reload_from_session(db: Session) -> None:
    rows = db.execute(
        text("SELECT setting_key, setting_value FROM site_settings")
    ).mappings().all()
    data = {str(r["setting_key"]): str(r.get("setting_value") or "").strip() for r in rows}
    with _lock:
        global _snapshot
        _snapshot = data


def patch_key(key: str, value: str) -> None:
    with _lock:
        if _snapshot is None:
            return
        _snapshot[str(key)] = str(value).strip()


def has_snapshot() -> bool:
    with _lock:
        return _snapshot is not None


def client_access_mode_payload() -> dict[str, Any] | None:
    with _lock:
        snap = dict(_snapshot) if _snapshot is not None else None
    if snap is None:
        return None
    return {
        "require_login": _bool_val(snap.get(CLIENT_LOGIN_KEY, ""), "1"),
        "allow_register": _bool_val(snap.get(CLIENT_REGISTER_KEY, ""), "1"),
        "contact_enabled": _bool_val(snap.get(CLIENT_CONTACT_KEY, ""), "1"),
        "feedback_enabled": _bool_val(snap.get(CLIENT_FEEDBACK_KEY, ""), "0"),
    }

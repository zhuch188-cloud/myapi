"""Render 运行时标识，用于区分「部署/重启」是否换了 commit 或仅换实例。"""
from __future__ import annotations

import logging
import os
from typing import Any

_log = logging.getLogger(__name__)

_RENDER_KEYS = (
    "RENDER_SERVICE_NAME",
    "RENDER_SERVICE_ID",
    "RENDER_INSTANCE_ID",
    "RENDER_GIT_BRANCH",
    "RENDER_GIT_COMMIT",
    "RENDER_DEPLOY_ID",
    "RENDER_EXTERNAL_URL",
)


def render_runtime_snapshot() -> dict[str, str]:
    out: dict[str, str] = {}
    for k in _RENDER_KEYS:
        v = (os.environ.get(k) or "").strip()
        if v:
            out[k] = v
    return out


def log_render_runtime(where: str) -> None:
    snap = render_runtime_snapshot()
    if not snap:
        _log.info("%s: 非 Render 环境（无 RENDER_* 变量）", where)
        return
    short = snap.get("RENDER_GIT_COMMIT", "")[:12]
    _log.info(
        "%s: service=%s instance=%s deploy=%s commit=%s branch=%s",
        where,
        snap.get("RENDER_SERVICE_NAME", "?"),
        (snap.get("RENDER_INSTANCE_ID", "?"))[:24],
        snap.get("RENDER_DEPLOY_ID", "?"),
        short or "?",
        snap.get("RENDER_GIT_BRANCH", "?"),
    )


def render_identity_line() -> str:
    snap = render_runtime_snapshot()
    if not snap:
        return ""
    c = (snap.get("RENDER_GIT_COMMIT") or "")[:12]
    i = (snap.get("RENDER_INSTANCE_ID") or "")[:20]
    d = snap.get("RENDER_DEPLOY_ID") or ""
    return f"commit={c} instance={i} deploy={d}"

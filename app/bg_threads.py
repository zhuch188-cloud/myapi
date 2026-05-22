"""在独立线程中执行长任务，避免阻塞 FastAPI 事件循环。"""

from __future__ import annotations

import logging
import signal
import threading
from collections.abc import Callable
from typing import Any

_log = logging.getLogger(__name__)
_lock = threading.Lock()
_active: dict[str, threading.Thread] = {}
_sigterm_logged = False


def _on_sigterm(signum: int, frame: Any) -> None:
    global _sigterm_logged
    if not _sigterm_logged:
        _sigterm_logged = True
        alive = [n for n, t in _active.items() if t.is_alive()]
        try:
            from app.render_diag import render_identity_line

            rid = render_identity_line()
        except Exception:
            rid = ""
        _log.warning(
            "收到 SIGTERM（Render 部署/手动 Restart/健康检查失败60s/平台维护，不一定来自 git push）"
            "%s；活动后台线程: %s",
            f" [{rid}]" if rid else "",
            alive or "(无)",
        )


def register_shutdown_signals() -> None:
    """主线程注册一次；部署时便于区分 OOM 与正常滚动发布。"""
    try:
        signal.signal(signal.SIGTERM, _on_sigterm)
        signal.signal(signal.SIGINT, _on_sigterm)
    except (ValueError, OSError):
        pass


def spawn_daemon(name: str, target: Callable[..., Any], /, *args: Any, **kwargs: Any) -> None:
    """
    启动后台线程。导入/同步等长任务使用非 daemon，以便进程收到 SIGTERM 时
    lifespan 可短暂 join，减少「写到一半直接被掐掉」。
    """

    def _wrapper() -> None:
        _log.info("thread %s: start", name)
        try:
            target(*args, **kwargs)
            _log.info("thread %s: done", name)
        except Exception:
            _log.exception("thread %s: failed", name)
        finally:
            with _lock:
                _active.pop(name, None)

    t = threading.Thread(target=_wrapper, name=name, daemon=False)
    with _lock:
        _active[name] = t
    t.start()


def active_background_thread_names() -> list[str]:
    with _lock:
        return [n for n, t in _active.items() if t.is_alive()]


def join_background_threads(*, timeout: float = 28.0) -> None:
    """应用关闭时等待长任务收尾（Render 部署宽限期通常 ≤30s）。"""
    with _lock:
        threads = [(n, t) for n, t in _active.items() if t.is_alive()]
    if not threads:
        return
    _log.warning(
        "应用关闭：等待 %s 个后台线程（最多 %.0fs）…",
        len(threads),
        timeout,
    )
    per = max(1.0, timeout / len(threads))
    for name, t in threads:
        t.join(timeout=per)
        if t.is_alive():
            _log.warning("thread %s: 未在时限内结束（部署将强制杀进程）", name)

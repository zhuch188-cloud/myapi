"""Run long background jobs without blocking the FastAPI event loop."""

from __future__ import annotations

import logging
import os
import signal
import threading
from collections.abc import Callable
from typing import Any

_log = logging.getLogger(__name__)
_lock = threading.Lock()
_active: dict[str, threading.Thread] = {}
_sigterm_logged = False
_shutdown_requested = threading.Event()
_previous_handlers: dict[int, Any] = {}


class ShutdownRequested(RuntimeError):
    """Raised by long-running background work when the process is draining."""


def is_shutting_down() -> bool:
    return _shutdown_requested.is_set()


def raise_if_shutting_down() -> None:
    if is_shutting_down():
        raise ShutdownRequested("\u540e\u53f0\u4efb\u52a1\u88ab\u4e2d\u65ad\uff0c\u8bf7\u5728\u4efb\u52a1\u8868\u70b9\u51fb\u300c\u7eed\u4f20\u300d\u7ee7\u7eed")


def _delegate_signal(signum: int, frame: Any) -> None:
    prev = _previous_handlers.get(signum)
    if callable(prev) and prev is not _on_sigterm:
        prev(signum, frame)
    elif prev in (signal.SIG_DFL, None):
        signal.signal(signum, signal.SIG_DFL)
        os.kill(os.getpid(), signum)


def _on_sigterm(signum: int, frame: Any) -> None:
    global _sigterm_logged
    _shutdown_requested.set()
    if not _sigterm_logged:
        _sigterm_logged = True
        alive = [n for n, t in _active.items() if t.is_alive()]
        try:
            from app.render_diag import render_identity_line

            rid = render_identity_line()
        except Exception:
            rid = ""
        _log.warning(
            "shutdown signal received; background threads=%s%s",
            alive or [],
            f" [{rid}]" if rid else "",
        )
    _delegate_signal(signum, frame)


def register_shutdown_signals() -> None:
    """Log shutdowns, set a drain flag, then let the server's handler run."""
    try:
        _previous_handlers[signal.SIGTERM] = signal.getsignal(signal.SIGTERM)
        _previous_handlers[signal.SIGINT] = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGTERM, _on_sigterm)
        signal.signal(signal.SIGINT, _on_sigterm)
    except (ValueError, OSError):
        pass


def spawn_daemon(name: str, target: Callable[..., Any], /, *args: Any, **kwargs: Any) -> None:
    """
    Start a non-daemon background thread so shutdown can wait briefly for cleanup.
    Long jobs should poll `raise_if_shutting_down()` between expensive phases.
    """

    def _wrapper() -> None:
        _log.info("thread %s: start", name)
        try:
            target(*args, **kwargs)
            _log.info("thread %s: done", name)
        except ShutdownRequested as ex:
            _log.warning("thread %s: stopping for shutdown: %s", name, ex)
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
    """Wait briefly for long jobs during app shutdown."""
    with _lock:
        threads = [(n, t) for n, t in _active.items() if t.is_alive()]
    if not threads:
        return
    _log.warning(
        "application shutdown: waiting for %s background thread(s), max %.0fs",
        len(threads),
        timeout,
    )
    per = max(1.0, timeout / len(threads))
    for name, t in threads:
        t.join(timeout=per)
        if t.is_alive():
            _log.warning("thread %s did not finish before shutdown deadline", name)

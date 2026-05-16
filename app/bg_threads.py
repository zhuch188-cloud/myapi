"""在独立守护线程中执行长任务，避免 FastAPI BackgroundTasks 偶发不执行或阻塞事件循环。"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from typing import Any

_log = logging.getLogger(__name__)


def spawn_daemon(name: str, target: Callable[..., Any], /, *args: Any, **kwargs: Any) -> None:
    """启动守护线程；异常会写入日志，避免静默失败。"""

    def _wrapper() -> None:
        _log.info("thread %s: start", name)
        try:
            target(*args, **kwargs)
            _log.info("thread %s: done", name)
        except Exception:
            _log.exception("thread %s: failed", name)

    threading.Thread(target=_wrapper, name=name, daemon=True).start()

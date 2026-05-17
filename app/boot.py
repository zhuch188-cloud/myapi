"""后台启动：避免 Render 上远程 Turso 建表阻塞 uvicorn 绑定端口。"""

from __future__ import annotations

import logging
import threading
from typing import Any, Callable

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import text

from app.config import settings
from app.db import init_database
from app.sql_dialect import sql_now
from app.timeutil import BEIJING_TZ
from app import wind_sql

_log = logging.getLogger(__name__)
_lock = threading.Lock()
_state: dict[str, Any] = {"started": False, "ready": False, "error": None}


def is_ready() -> bool:
    return bool(_state["ready"])


def boot_error() -> str | None:
    err = _state.get("error")
    return str(err) if err else None


def _clear_stale_jobs() -> None:
    from app.db import SessionLocalFactory

    if SessionLocalFactory is None:
        return
    db = SessionLocalFactory()
    try:
        db.execute(
            text(
                f"""
                UPDATE strategy_update_jobs
                SET status='FAILED', finished_at={sql_now()},
                    message='stale RUNNING cleared on server startup'
                WHERE status='RUNNING'
                """
            )
        )
        db.execute(
            text(
                """
                UPDATE data_import_batches
                SET status='FAILED',
                    message=COALESCE(message, '') || '（服务重启：入队后未执行，已标失败；请重新导入或点续传）'
                WHERE status='QUEUED'
                """
            )
        )
        db.execute(
            text(
                """
                UPDATE data_import_batches
                SET status='FAILED',
                    message=COALESCE(message, '') || '（服务重启：导入中断，已标失败；可点续传）'
                WHERE status='RUNNING'
                """
            )
        )
        db.commit()
    finally:
        db.close()


def _start_scheduler(scheduler: BackgroundScheduler, scheduled_fn: Callable[[], None]) -> None:
    cron_raw = (settings.daily_job_cron or "").split()
    if len(cron_raw) != 5:
        _log.warning(
            "DAILY_JOB_CRON 格式无效（需 5 段：分 时 日 月 星期），已跳过定时任务: %r",
            settings.daily_job_cron,
        )
        return
    trigger = CronTrigger(
        minute=cron_raw[0],
        hour=cron_raw[1],
        day=cron_raw[2],
        month=cron_raw[3],
        day_of_week=cron_raw[4],
        timezone=BEIJING_TZ,
    )
    scheduler.add_job(scheduled_fn, trigger=trigger, id="daily_update", replace_existing=True)
    scheduler.start()


def _boot_worker(scheduler: BackgroundScheduler, scheduled_fn: Callable[[], None]) -> None:
    try:
        _log.info("后台启动：开始初始化 Turso 数据库（远程建表可能需 30～120 秒）…")
        init_database()
        try:
            wind_sql.init_wind_backend()
        except Exception as e:
            _log.warning("Wind 初始化异常（已忽略）: %s", e)
        _clear_stale_jobs()
        import app.services as _svc

        _svc._job_running = False
        _start_scheduler(scheduler, scheduled_fn)
        _state["ready"] = True
        _log.info("后台启动：数据库与调度器已就绪")
    except Exception as e:
        _state["error"] = str(e)
        _log.critical(
            "后台启动失败（请检查 TURSO_DATABASE_URL / TURSO_AUTH_TOKEN）: %s",
            e,
            exc_info=True,
        )


def start_background_boot(scheduler: BackgroundScheduler, scheduled_fn: Callable[[], None]) -> None:
    with _lock:
        if _state["started"]:
            return
        _state["started"] = True
    threading.Thread(
        target=_boot_worker,
        args=(scheduler, scheduled_fn),
        name="app-boot",
        daemon=True,
    ).start()

"""后台启动：避免 Render 上远程 Turso 建表阻塞 uvicorn 绑定端口。"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable

from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import text

from app.db import init_database, is_hrana_transient_error
from app.scheduled_update_config import (
    daily_job_cron_value,
    register_daily_update_job,
    resume_interrupted_update_after_restart,
)
from app.sql_dialect import sql_now
from app import wind_sql
from app.bg_threads import spawn_daemon

_log = logging.getLogger(__name__)
_lock = threading.Lock()
_state: dict[str, Any] = {"started": False, "ready": False, "error": None}


def is_ready() -> bool:
    return bool(_state["ready"])


def boot_error() -> str | None:
    err = _state.get("error")
    return str(err) if err else None


def _clear_stale_jobs() -> bool:
    """清理僵尸任务；若曾存在 RUNNING 的 strategy_update_jobs 则返回 True。"""
    from app.db import SessionLocalFactory, turso_stream_lock

    if SessionLocalFactory is None:
        return False
    with turso_stream_lock():
        db = SessionLocalFactory()
        try:
            return _clear_stale_jobs_on_session(db)
        finally:
            db.close()


def _clear_stale_jobs_on_session(db) -> bool:
    had_running_update = (
        db.execute(
            text("SELECT 1 FROM strategy_update_jobs WHERE status='RUNNING' LIMIT 1")
        ).first()
        is not None
    )
    try:
        db.execute(
            text(
                f"""
                UPDATE strategy_update_jobs
                SET status='FAILED', finished_at={sql_now()},
                    message='stale RUNNING cleared on server startup（重启后将自动重新执行更新）'
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
        db.execute(
            text(
                f"""
                UPDATE admin_sync_jobs
                SET status='FAILED', finished_at={sql_now()},
                    message=COALESCE(message, '') || '（服务重启：同步中断，已标失败；有断点可点续传）'
                WHERE status IN ('RUNNING', 'QUEUED')
                """
            )
        )
        db.execute(
            text(
                f"""
                UPDATE strategy_import_jobs
                SET status='FAILED', finished_at={sql_now()},
                    message=COALESCE(message, '') || '（进程重启/部署中断，已标失败；请点「续传」勿新建全量）'
                WHERE status IN ('RUNNING', 'QUEUED')
                """
            )
        )
        db.commit()
    except Exception:
        db.rollback()
        raise
    return had_running_update


def _start_scheduler(scheduler: BackgroundScheduler, scheduled_fn: Callable[[], None]) -> None:
    cron = daily_job_cron_value()
    if not register_daily_update_job(scheduler, scheduled_fn, cron):
        _log.warning(
            "定时任务 Cron 无效（需 5 段：分 时 日 月 星期，且日/月为 *）：%r",
            cron,
        )


def reschedule_daily_update_job(
    scheduler: BackgroundScheduler, scheduled_fn: Callable[[], None]
) -> bool:
    """保存仪表盘配置后热更新 APScheduler（无需重启进程）。"""
    return register_daily_update_job(scheduler, scheduled_fn, daily_job_cron_value())


def _boot_worker(scheduler: BackgroundScheduler, scheduled_fn: Callable[[], None]) -> None:
    boot_attempts = 5
    for boot_i in range(boot_attempts):
        try:
            _log.info("后台启动：开始初始化 Turso 数据库（远程建表可能需 30～120 秒）…")
            init_database()
            try:
                wind_sql.init_wind_backend()
            except Exception as e:
                _log.warning("Wind 初始化异常（已忽略）: %s", e)
            interrupted_update = _clear_stale_jobs()
            import app.services as _svc
            from app.db import SessionLocalFactory, turso_stream_lock
            from app.site_settings_cache import reload_from_session

            _svc._job_running = False
            with turso_stream_lock():
                db = SessionLocalFactory()
                try:
                    reload_from_session(db)
                finally:
                    db.close()
            _start_scheduler(scheduler, scheduled_fn)
            _state["ready"] = True
            _state["error"] = None
            _log.info("后台启动：数据库与调度器已就绪")
            if interrupted_update:
                _log.info("检测到重启前未完成的更新任务，将自动续跑 run_update")
                spawn_daemon("restart-resume-update", resume_interrupted_update_after_restart)
            return
        except Exception as e:
            if is_hrana_transient_error(e) and boot_i + 1 < boot_attempts:
                wait = min(3 * (2**boot_i), 30)
                _log.warning(
                    "数据库初始化 transient 失败，%ss 后重试 (%s/%s): %s",
                    wait,
                    boot_i + 2,
                    boot_attempts,
                    e,
                )
                time.sleep(wait)
                continue
            _state["error"] = str(e)
            _log.critical(
                "后台启动失败（请检查 TURSO_DATABASE_URL；连接 Turso 云库时需 TURSO_AUTH_TOKEN）: %s",
                e,
                exc_info=True,
            )
            return


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

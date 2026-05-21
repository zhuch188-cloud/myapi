"""每日定时数据更新：site_settings 配置、Cron 解析与调度器注册。"""

from __future__ import annotations

import logging
import time
from typing import Any

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config import settings
from app.timeutil import BEIJING_TZ

_log = logging.getLogger(__name__)

DAILY_JOB_CRON_KEY = "daily_job_cron"
SCHEDULED_UPDATE_MAX_ATTEMPTS_KEY = "scheduled_update_max_attempts"
SCHEDULED_UPDATE_RETRY_SLEEP_KEY = "scheduled_update_retry_sleep_sec"
RESTART_AUTO_UPDATE_KEY = "restart_auto_update_enabled"

_WEEKDAY_CN = ("周一", "周二", "周三", "周四", "周五", "周六", "周日")


def _snapshot_dict() -> dict[str, str]:
    try:
        from app.site_settings_cache import snapshot_dict

        return snapshot_dict()
    except Exception:
        return {}


def _clamp_int(raw: object, default: int, lo: int, hi: int) -> int:
    try:
        v = int(str(raw).strip())
    except (TypeError, ValueError):
        return default
    return max(lo, min(hi, v))


def default_daily_job_cron() -> str:
    return (settings.daily_job_cron or "0 17 * * 0-4").strip()


def daily_job_cron_value(snap: dict[str, str] | None = None) -> str:
    snap = snap if snap is not None else _snapshot_dict()
    raw = (snap.get(DAILY_JOB_CRON_KEY) or "").strip()
    return raw or default_daily_job_cron()


def scheduled_update_max_attempts(snap: dict[str, str] | None = None) -> int:
    snap = snap if snap is not None else _snapshot_dict()
    raw = (snap.get(SCHEDULED_UPDATE_MAX_ATTEMPTS_KEY) or "").strip()
    if raw:
        return _clamp_int(raw, 5, 1, 20)
    return _clamp_int(getattr(settings, "scheduled_update_max_attempts", 5), 5, 1, 20)


def scheduled_update_retry_sleep_sec(snap: dict[str, str] | None = None) -> int:
    snap = snap if snap is not None else _snapshot_dict()
    raw = (snap.get(SCHEDULED_UPDATE_RETRY_SLEEP_KEY) or "").strip()
    if raw:
        return _clamp_int(raw, 8, 1, 600)
    return _clamp_int(getattr(settings, "scheduled_update_retry_sleep_sec", 8), 8, 1, 600)


def restart_auto_update_enabled(snap: dict[str, str] | None = None) -> bool:
    snap = snap if snap is not None else _snapshot_dict()
    raw = (snap.get(RESTART_AUTO_UPDATE_KEY) or "").strip().lower()
    if raw:
        return raw not in ("0", "false", "no", "off")
    return bool(getattr(settings, "restart_auto_update_enabled", True))


def run_update_with_retries(
    job_type: str,
    triggered_by: str,
    *,
    skip_if_busy: bool = True,
    log_prefix: str = "Scheduled update",
) -> bool:
    """执行 run_update，失败时按 site_settings 重试；返回是否成功。"""
    import app.services as _svc
    from app import wind_sql
    from app.db import SessionLocalFactory

    if not wind_sql.use_remote_sqlserver():
        _log.warning("%s: skip, Wind SQL Server not configured or unavailable", log_prefix)
        return False
    if skip_if_busy and _svc._job_running:
        _log.info("%s: skip, run_update already active", log_prefix)
        return False

    max_attempts = scheduled_update_max_attempts()
    retry_sleep = scheduled_update_retry_sleep_sec()
    last_exc: BaseException | None = None
    for attempt in range(1, max_attempts + 1):
        if skip_if_busy and attempt > 1 and _svc._job_running:
            _log.info("%s: skip retry, another update started", log_prefix)
            return False
        db = SessionLocalFactory()
        try:
            _svc.run_update(db, job_type, triggered_by)
            if attempt > 1:
                _log.info(
                    "%s succeeded on attempt %s/%s",
                    log_prefix,
                    attempt,
                    max_attempts,
                )
            return True
        except Exception as ex:
            last_exc = ex
            _log.warning(
                "%s attempt %s/%s failed: %s",
                log_prefix,
                attempt,
                max_attempts,
                ex,
                exc_info=attempt >= max_attempts,
            )
        finally:
            db.close()
        if attempt < max_attempts:
            time.sleep(retry_sleep)
    _log.error(
        "%s exhausted %s attempts; last error: %s",
        log_prefix,
        max_attempts,
        last_exc,
    )
    return False


def resume_interrupted_update_after_restart() -> None:
    """进程启动后：若上次更新因重启中断，自动重新 run_update（含重试）。"""
    delay = max(0, int(getattr(settings, "restart_auto_update_delay_sec", 15) or 15))
    if delay:
        time.sleep(delay)
    if not restart_auto_update_enabled():
        _log.info("Restart resume update: disabled in site_settings")
        return
    run_update_with_retries(
        "RESUME",
        "system",
        skip_if_busy=True,
        log_prefix="Restart resume update",
    )


def parse_cron_parts(cron_raw: str) -> dict[str, Any] | None:
    """解析 5 段 cron（分 时 日 月 星期）；星期为 APScheduler 约定（周一=0）。"""
    parts = (cron_raw or "").split()
    if len(parts) != 5:
        return None
    minute_s, hour_s, day_s, month_s, dow_s = parts
    if day_s != "*" or month_s != "*":
        return None
    try:
        minute = int(minute_s)
        hour = int(hour_s)
    except ValueError:
        return None
    if not (0 <= minute <= 59 and 0 <= hour <= 23):
        return None
    weekdays = _parse_day_of_week(dow_s)
    if weekdays is None:
        return None
    return {
        "minute": minute,
        "hour": hour,
        "weekdays": weekdays,
        "cron": f"{minute} {hour} * * {_format_day_of_week(weekdays)}",
    }


def _parse_day_of_week(dow_s: str) -> list[int] | None:
    s = (dow_s or "").strip().lower()
    if not s or s == "*":
        return list(range(7))
    out: set[int] = set()
    for token in s.split(","):
        token = token.strip()
        if not token:
            continue
        if "-" in token:
            a, b = token.split("-", 1)
            try:
                lo, hi = int(a), int(b)
            except ValueError:
                return None
            if not (0 <= lo <= 6 and 0 <= hi <= 6 and lo <= hi):
                return None
            out.update(range(lo, hi + 1))
        else:
            try:
                d = int(token)
            except ValueError:
                return None
            if not (0 <= d <= 6):
                return None
            out.add(d)
    return sorted(out) if out else None


def _format_day_of_week(days: list[int]) -> str:
    if days == list(range(7)):
        return "*"
    if days == list(range(5)):
        return "0-4"
    return ",".join(str(d) for d in days)


def build_daily_job_cron(minute: int, hour: int, weekdays: list[int]) -> str:
    try:
        minute = max(0, min(59, int(minute)))
        hour = max(0, min(23, int(hour)))
    except (TypeError, ValueError):
        minute, hour = 0, 17
    wd = sorted({int(d) for d in weekdays if 0 <= int(d) <= 6})
    if not wd:
        wd = list(range(5))
    return f"{minute} {hour} * * {_format_day_of_week(wd)}"


def cron_label(cron_raw: str) -> str:
    parsed = parse_cron_parts(cron_raw)
    if not parsed:
        return cron_raw or "（无效 Cron）"
    wd = parsed["weekdays"]
    if wd == list(range(7)):
        wd_txt = "每天"
    elif wd == list(range(5)):
        wd_txt = "每个周一至周五"
    else:
        wd_txt = "、".join(_WEEKDAY_CN[d] for d in wd)
    return f"北京时间 {wd_txt} {parsed['hour']:02d}:{parsed['minute']:02d}"


def scheduled_update_config_payload(
    snap: dict[str, str] | None = None,
    *,
    next_run_at: str | None = None,
) -> dict[str, Any]:
    cron = daily_job_cron_value(snap)
    parsed = parse_cron_parts(cron)
    out: dict[str, Any] = {
        "daily_job_cron": cron,
        "cron_label": cron_label(cron),
        "max_attempts": scheduled_update_max_attempts(snap),
        "retry_sleep_sec": scheduled_update_retry_sleep_sec(snap),
        "restart_auto_update": restart_auto_update_enabled(snap),
        "timezone": "Asia/Shanghai",
        "next_run_at": next_run_at,
        "retry_scope": "single_trigger",
        "restart_note": (
            "重试在「一次定时触发」或「重启自动续跑」内连续进行（次数与间隔见上）。"
            "若更新进行中服务重启：进行中任务会标为失败，并在启动约 "
            f"{max(0, int(getattr(settings, 'restart_auto_update_delay_sec', 15) or 15))} 秒后"
            "自动重新执行更新（可在下方关闭）。错过的定时 Cron 点仍不补跑。"
        ),
    }
    if parsed:
        out["hour"] = parsed["hour"]
        out["minute"] = parsed["minute"]
        out["weekdays"] = parsed["weekdays"]
    return out


def register_daily_update_job(
    scheduler: BackgroundScheduler, scheduled_fn: Any, cron_raw: str | None = None
) -> bool:
    """注册/替换 daily_update 任务；scheduler 未 start 时会 start。"""
    cron = (cron_raw or daily_job_cron_value()).strip()
    parts = parse_cron_parts(cron)
    if not parts:
        return False
    trigger = CronTrigger(
        minute=str(parts["minute"]),
        hour=str(parts["hour"]),
        day="*",
        month="*",
        day_of_week=_format_day_of_week(parts["weekdays"]),
        timezone=BEIJING_TZ,
    )
    scheduler.add_job(
        scheduled_fn,
        trigger=trigger,
        id="daily_update",
        replace_existing=True,
        misfire_grace_time=3600,
    )
    if not scheduler.running:
        scheduler.start()
    return True


def next_daily_update_run_iso(scheduler: BackgroundScheduler | None) -> str | None:
    if scheduler is None or not scheduler.running:
        return None
    job = scheduler.get_job("daily_update")
    if not job or not job.next_run_time:
        return None
    return job.next_run_time.astimezone(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")

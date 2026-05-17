"""应用统一使用北京时间（Asia/Shanghai）。"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

BEIJING_TZ = ZoneInfo("Asia/Shanghai")
# Turso / libSQL 的 datetime('now') 为 UTC；SQL 层用 +8 小时得到北京时间
SQLITE_UTC_TO_BEIJING_OFFSET = "+8 hours"
SQLITE_NOW_BEIJING = f"datetime('now', '{SQLITE_UTC_TO_BEIJING_OFFSET}')"


def now() -> datetime:
    """当前北京时间（带时区）。"""
    return datetime.now(BEIJING_TZ)


def today() -> date:
    """当前北京日期。"""
    return now().date()


def now_naive() -> datetime:
    """无时区标注的北京时间，用于写入库表 TEXT 时间戳及与历史 naive 值比较。"""
    return now().replace(tzinfo=None)


def to_beijing_naive(v: datetime | None) -> datetime | None:
    if v is None:
        return None
    if v.tzinfo is None:
        return v
    return v.astimezone(BEIJING_TZ).replace(tzinfo=None)


def utc_now() -> datetime:
    """JWT 等需 UTC epoch 的场景。"""
    return datetime.now(timezone.utc)

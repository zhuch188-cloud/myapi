"""SQLite / Turso (libSQL) SQL 片段与元数据查询。"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.timeutil import SQLITE_NOW_BEIJING as _NOW_BJ


def sql_now() -> str:
    """SQLite/Turso 当前北京时间（库内 datetime('now') 为 UTC，+8 对齐上海）。"""
    return _NOW_BJ


def sql_curdate() -> str:
    return f"date({_NOW_BJ})"


def sql_hours_ago(param: str = ":hrs") -> str:
    return f"datetime({_NOW_BJ}, printf('-%d hours', {param}))"


def sql_days_ago(days: int) -> str:
    return f"datetime({_NOW_BJ}, '-{int(days)} days')"


def sql_curdate_days_ago(days: int) -> str:
    return f"date(datetime({_NOW_BJ}, '-{int(days)} days'))"


def sql_minutes_ago(param: str = ":mins") -> str:
    return f"datetime({_NOW_BJ}, printf('-%d minutes', {param}))"


def sql_timestampdiff_hours(col: str) -> str:
    return f"(julianday({_NOW_BJ}) - julianday({col})) * 24"


def sql_year(col: str) -> str:
    """从日期/时间列提取四位年份（SQLite/Turso strftime）。"""
    return f"strftime('%Y', {col})"


def normalize_sql_date_text(v: Any) -> str | None:
    """业务日期列 canonical 存储：YYYY-MM-DD（无法解析则 None）。"""
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date().isoformat()
    if isinstance(v, date):
        return v.isoformat()
    s = str(v).strip()
    if not s:
        return None
    compact = s.replace("-", "").replace("/", "")[:8]
    if len(compact) == 8 and compact.isdigit():
        try:
            datetime.strptime(compact, "%Y%m%d")
            return f"{compact[:4]}-{compact[4:6]}-{compact[6:8]}"
        except ValueError:
            return None
    try:
        return datetime.fromisoformat(s[:10]).date().isoformat()
    except ValueError:
        return None


def sql_date_to_iso_expr(col: str) -> str:
    """SQL：将 TEXT 日期列转为 YYYY-MM-DD（用于迁库 UPDATE）。"""
    c = str(col).strip()
    compact = f"REPLACE(SUBSTR({c}, 1, 10), '-', '')"
    return (
        f"CASE WHEN length({compact}) = 8 AND {compact} GLOB '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]' "
        f"THEN substr({compact}, 1, 4) || '-' || substr({compact}, 5, 2) || '-' || substr({compact}, 7, 2) "
        f"ELSE SUBSTR({c}, 1, 10) END"
    )


def sql_date_compact_expr(col: str) -> str:
    """
    将 TEXT 日期列规范为 YYYYMMDD 字符串，便于 MAX/MIN/ORDER BY 按日历序比较。
    库内业务日期列应以 YYYY-MM-DD 存储；兼容历史 YYYYMMDD 与带时间后缀的 TEXT。
    """
    c = str(col).strip()
    return f"REPLACE(SUBSTR({c}, 1, 10), '-', '')"


def sql_max_date_expr(col: str) -> str:
    """TEXT 日期列的日历最大日（返回 YYYYMMDD 形态，读出后须 _row_sql_date）。"""
    return f"MAX({sql_date_compact_expr(col)})"


def sql_order_date_asc(col: str) -> str:
    return f"{sql_date_compact_expr(col)} ASC"


def sql_order_date_desc(col: str) -> str:
    return f"{sql_date_compact_expr(col)} DESC"


def quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def list_table_columns(db: Session, table: str) -> set[str]:
    tn = str(table).replace('"', "")
    rows = db.execute(text(f'PRAGMA table_info("{tn}")')).fetchall()
    return {str(r[1]) for r in rows}


def list_table_column_names_lower(db: Session, table: str) -> set[str]:
    return {c.lower() for c in list_table_columns(db, table)}


def _looks_like_date_only_string(s: str) -> bool:
    t = s.strip()
    if not t or len(t) > 32:
        return False
    head = t[:10]
    compact = head.replace("-", "").replace("/", "")[:8]
    if len(compact) == 8 and compact.isdigit():
        return True
    return len(head) == 10 and head[4:5] == "-" and head[7:8] == "-"


def coerce_bind_value(v: Any) -> Any:
    """libsql 不接受 Python datetime/date 作为绑定参数，须转为 TEXT；日期列统一 YYYY-MM-DD。"""
    if isinstance(v, datetime):
        return v.replace(tzinfo=None).isoformat(sep=" ", timespec="microseconds")
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, str):
        if _looks_like_date_only_string(v):
            iso = normalize_sql_date_text(v)
            if iso:
                return iso
    return v


def executed_rowid(db: Session, result: Any) -> int:
    """Turso/libsql 下 CursorResult.lastrowid 有时为 0，回退 last_insert_rowid()。"""
    rid = int(getattr(result, "lastrowid", None) or 0)
    if rid:
        return rid
    row = db.execute(text("SELECT last_insert_rowid() AS id")).mappings().first()
    return int((row or {}).get("id") or 0)


def coerce_bind_parameters(parameters: Any) -> Any:
    if parameters is None:
        return parameters
    if isinstance(parameters, dict):
        return {k: coerce_bind_value(v) for k, v in parameters.items()}
    if isinstance(parameters, list):
        if parameters and isinstance(parameters[0], dict):
            return [{k: coerce_bind_value(v) for k, v in row.items()} for row in parameters]
        return [coerce_bind_value(v) for v in parameters]
    if isinstance(parameters, tuple):
        return tuple(coerce_bind_value(v) for v in parameters)
    return coerce_bind_value(parameters)

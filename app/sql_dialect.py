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


def quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def list_table_columns(db: Session, table: str) -> set[str]:
    tn = str(table).replace('"', "")
    rows = db.execute(text(f'PRAGMA table_info("{tn}")')).fetchall()
    return {str(r[1]) for r in rows}


def list_table_column_names_lower(db: Session, table: str) -> set[str]:
    return {c.lower() for c in list_table_columns(db, table)}


def coerce_bind_value(v: Any) -> Any:
    """libsql 不接受 Python datetime/date 作为绑定参数，须转为 TEXT。"""
    if isinstance(v, datetime):
        return v.replace(tzinfo=None).isoformat(sep=" ", timespec="microseconds")
    if isinstance(v, date):
        return v.isoformat()
    return v


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

"""策略列表快照指标计算（独立模块，供 strategy_list_metrics 与 main 共用，避免循环导入）。"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.services import _nav_rb_idx_on_date, _row_sql_date
from app.sql_dialect import sql_date_compact_expr, sql_order_date_asc, sql_order_date_desc

_log = logging.getLogger(__name__)

LIST_METRICS_EMPTY: dict[str, Any] = {
    "latest_nav": None,
    "last_1d_return": None,
    "last_5d_return": None,
    "period_since_rebalance_return": None,
    "month_return": None,
    "year_return": None,
}


def _trade_date_as_date(td: Any) -> date | None:
    d = _row_sql_date(td)
    if d is not None:
        return d
    if isinstance(td, datetime):
        return td.date()
    if isinstance(td, date):
        return td
    s = str(td).strip()[:10]
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def _safe_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _round_nav_unit(v: Any) -> float | None:
    fv = _safe_float(v)
    if fv is None:
        return None
    return round(fv, 4)


def _nav_unit_trading_days_offset(
    db: Session, strategy_id: str, asof: date, offset: int
) -> float | None:
    if offset < 0:
        return None
    row = db.execute(
        text(
            f"""
            SELECT nav_unit
            FROM strategy_nav_daily
            WHERE strategy_id=:sid
              AND {sql_date_compact_expr("trade_date")} <= :acmp
            ORDER BY {sql_order_date_desc("trade_date")}
            LIMIT 1 OFFSET {int(offset)}
            """
        ),
        {"sid": strategy_id, "acmp": asof.strftime("%Y%m%d")},
    ).mappings().first()
    if not row or row.get("nav_unit") is None:
        return None
    v = _safe_float(row["nav_unit"])
    return v if v is not None and v > 0 else None


def _nav_unit_last_before(db: Session, strategy_id: str, anchor_dt: date) -> float | None:
    row = db.execute(
        text(
            f"""
            SELECT nav_unit
            FROM strategy_nav_daily
            WHERE strategy_id=:sid
              AND {sql_date_compact_expr("trade_date")} < :acmp
            ORDER BY {sql_order_date_desc("trade_date")}
            LIMIT 1
            """
        ),
        {"sid": strategy_id, "acmp": anchor_dt.strftime("%Y%m%d")},
    ).mappings().first()
    if not row or row.get("nav_unit") is None:
        return None
    v = _safe_float(row["nav_unit"])
    return v if v is not None and v > 0 else None


def rolling_window_returns(
    db: Session,
    strategy_id: str,
    *,
    last_td: date | None = None,
    last_nav: float | None = None,
) -> dict[str, float | None]:
    sid = str(strategy_id).strip()
    out: dict[str, float | None] = {
        "last_5d_return": None,
        "month_return": None,
        "year_return": None,
    }
    if not sid:
        return out
    if last_td is None or last_nav is None:
        top = db.execute(
            text(
                f"""
                SELECT trade_date, nav_unit
                FROM strategy_nav_daily
                WHERE strategy_id=:sid
                ORDER BY {sql_order_date_desc("trade_date")}
                LIMIT 1
                """
            ),
            {"sid": sid},
        ).mappings().first()
        if not top:
            return out
        last_td = _trade_date_as_date(top["trade_date"])
        last_nav = _safe_float(top.get("nav_unit"))
    if last_td is None or last_nav is None or last_nav <= 0:
        return out
    base5 = _nav_unit_trading_days_offset(db, sid, last_td, 5)
    if base5 is not None and base5 > 0:
        out["last_5d_return"] = last_nav / base5 - 1.0
    month_cut = last_td.replace(day=1)
    year_cut = date(last_td.year, 1, 1)
    anchor_m = _nav_unit_last_before(db, sid, month_cut)
    anchor_y = _nav_unit_last_before(db, sid, year_cut)
    if anchor_m is not None and anchor_m > 0:
        out["month_return"] = last_nav / anchor_m - 1.0
    if anchor_y is not None and anchor_y > 0:
        out["year_return"] = last_nav / anchor_y - 1.0
    return out


def _period_rebalance_date(db: Session, strategy_id: str, last_td: date) -> date | None:
    rb_rows = db.execute(
        text(
            """
            SELECT DISTINCT rebalance_date
            FROM strategy_positions
            WHERE strategy_id=:sid
            """
        ),
        {"sid": strategy_id},
    ).mappings().all()
    rb_sorted: list[date] = []
    for r in rb_rows:
        d = _row_sql_date(r.get("rebalance_date"))
        if d is not None:
            rb_sorted.append(d)
    if not rb_sorted:
        return None
    rb_sorted.sort()
    _, current_rb = _nav_rb_idx_on_date(rb_sorted, last_td)
    return current_rb


def _period_start_nav_unit(db: Session, strategy_id: str, period_rb: date) -> float | None:
    from app.main import _anchor_from_rebalance_period, _resolve_nav_rebalance_period_key

    sd = period_rb.isoformat()
    rk = _resolve_nav_rebalance_period_key(db, strategy_id, sd)
    if rk:
        nu, _ = _anchor_from_rebalance_period(db, strategy_id, rk)
        if nu is not None and nu > 0:
            return float(nu)
    rb_cmp = period_rb.strftime("%Y%m%d")
    row = db.execute(
        text(
            f"""
            SELECT nav_unit
            FROM strategy_nav_daily
            WHERE strategy_id=:sid
              AND {sql_date_compact_expr("trade_date")} >= :rb_cmp
            ORDER BY {sql_order_date_asc("trade_date")}
            LIMIT 1
            """
        ),
        {"sid": strategy_id, "rb_cmp": rb_cmp},
    ).mappings().first()
    if not row or row.get("nav_unit") is None:
        return None
    v = _safe_float(row["nav_unit"])
    return v if v is not None and v > 0 else None


def compute_strategy_list_metrics_snapshot(
    db: Session, strategy_id: str
) -> dict[str, Any]:
    """按库内最新净值日计算策略列表一行指标（快照写入用）。"""
    sid = str(strategy_id).strip()
    if not sid:
        return dict(LIST_METRICS_EMPTY)
    top = db.execute(
        text(
            f"""
            SELECT trade_date, nav_unit, daily_ret
            FROM strategy_nav_daily
            WHERE strategy_id=:sid
            ORDER BY {sql_order_date_desc("trade_date")}
            LIMIT 1
            """
        ),
        {"sid": sid},
    ).mappings().first()
    if not top:
        return dict(LIST_METRICS_EMPTY)
    last_nav = _safe_float(top.get("nav_unit"))
    if last_nav is None or last_nav <= 0:
        return dict(LIST_METRICS_EMPTY)
    last_1d_return = _safe_float(top.get("daily_ret"))
    last_td = _trade_date_as_date(top["trade_date"])
    if last_td is None:
        return dict(LIST_METRICS_EMPTY)

    rolling = rolling_window_returns(db, sid, last_td=last_td, last_nav=last_nav)
    period_ret = None
    max_rb = _period_rebalance_date(db, sid, last_td)
    if max_rb is not None:
        p0 = _period_start_nav_unit(db, sid, max_rb)
        if p0 is not None and p0 > 0:
            period_ret = last_nav / p0 - 1.0

    out = {
        "latest_nav": _round_nav_unit(last_nav),
        "last_1d_return": last_1d_return,
        "last_5d_return": rolling["last_5d_return"],
        "period_since_rebalance_return": period_ret,
        "month_return": rolling["month_return"],
        "year_return": rolling["year_return"],
    }
    mr = out.get("month_return")
    yr = out.get("year_return")
    if mr is not None and yr is not None and abs(float(mr) - float(yr)) < 1e-12:
        _log.warning(
            "list metrics month==year sid=%s last_td=%s nav=%s (check month anchor)",
            sid,
            last_td,
            last_nav,
        )
    return out

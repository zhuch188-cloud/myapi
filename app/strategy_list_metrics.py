"""策略列表展示指标快照：客户端列表只读此表，避免每次扫全表 strategy_nav_daily。"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.sql_dialect import sql_now

_log = logging.getLogger(__name__)


def _enabled_visible_strategy_ids(db: Session) -> list[str]:
    rows = db.execute(
        text(
            """
            SELECT strategy_id
            FROM strategy_configs
            WHERE is_visible=1 AND status='enabled'
            ORDER BY updated_at DESC
            """
        )
    ).mappings().all()
    return [str(r["strategy_id"]).strip() for r in rows if str(r.get("strategy_id") or "").strip()]


def _metrics_ids_with_cache(db: Session, strategy_ids: list[str]) -> set[str]:
    if not strategy_ids:
        return set()
    quoted = ",".join("'" + s.replace("'", "''") + "'" for s in strategy_ids)
    rows = db.execute(
        text(
            f"""
            SELECT strategy_id FROM strategy_list_metrics
            WHERE strategy_id IN ({quoted})
            """
        )
    ).mappings().all()
    return {str(r["strategy_id"]).strip() for r in rows}


def refresh_strategy_list_metrics_cache(
    db: Session,
    strategy_ids: list[str] | None = None,
    *,
    do_commit: bool = True,
) -> int:
    """
    按与列表页相同口径重算并 UPSERT strategy_list_metrics。
    strategy_ids 为空时刷新全部已启用且可见策略。
    """
    from app.main import (
        _NAV_LIST_SUMMARY_EMPTY,
        _batch_nav_last_date_stock_count,
        _strategy_nav_list_summary_bounded,
        _nav_list_period_rebalance_date,
    )

    ids = [str(x).strip() for x in (strategy_ids or []) if str(x or "").strip()]
    if not ids:
        ids = _enabled_visible_strategy_ids(db)
    if not ids:
        return 0

    summaries: dict[str, dict[str, Any]] = {}
    for sid in ids:
        summaries[sid] = _strategy_nav_list_summary_bounded(db, sid)
    nav_meta = _batch_nav_last_date_stock_count(db, ids)
    now_expr = sql_now()
    upsert = text(
        f"""
        INSERT INTO strategy_list_metrics (
            strategy_id, latest_nav, last_1d_return, last_5d_return,
            period_since_rebalance_return, month_return, year_return,
            last_trade_date, stock_count_on_last_date, period_rebalance_date,
            updated_at
        ) VALUES (
            :sid, :latest_nav, :last_1d, :last_5d, :period_ret, :month_ret, :year_ret,
            :last_td, :stock_cnt, :period_rb, {now_expr}
        )
        ON CONFLICT(strategy_id) DO UPDATE SET
            latest_nav=excluded.latest_nav,
            last_1d_return=excluded.last_1d_return,
            last_5d_return=excluded.last_5d_return,
            period_since_rebalance_return=excluded.period_since_rebalance_return,
            month_return=excluded.month_return,
            year_return=excluded.year_return,
            last_trade_date=excluded.last_trade_date,
            stock_count_on_last_date=excluded.stock_count_on_last_date,
            period_rebalance_date=excluded.period_rebalance_date,
            updated_at=excluded.updated_at
        """
    )

    n = 0
    for sid in ids:
        s = summaries.get(sid) or dict(_NAV_LIST_SUMMARY_EMPTY)
        m = nav_meta.get(sid) or {}
        period_rb_str: str | None = None
        last_td_str = m.get("last_trade_date")
        if last_td_str:
            try:
                last_td = date.fromisoformat(str(last_td_str)[:10])
                period_rb = _nav_list_period_rebalance_date(db, sid, last_td)
                if period_rb is not None:
                    period_rb_str = period_rb.isoformat()
            except (TypeError, ValueError):
                pass
        db.execute(
            upsert,
            {
                "sid": sid,
                "latest_nav": s.get("latest_nav"),
                "last_1d": s.get("last_1d_return"),
                "last_5d": s.get("last_5d_return"),
                "period_ret": s.get("period_since_rebalance_return"),
                "month_ret": s.get("month_return"),
                "year_ret": s.get("year_return"),
                "last_td": last_td_str,
                "stock_cnt": m.get("stock_count"),
                "period_rb": period_rb_str,
            },
        )
        n += 1

    if do_commit:
        db.commit()
    return n


def ensure_strategy_list_metrics_for_list(
    db: Session, strategy_ids: list[str], *, do_commit: bool = True
) -> bool:
    """
    列表接口：补算尚无快照的策略；若快照末日与净值表 MAX(trade_date) 不一致则重算（口径变更或更新后）。
    返回是否发生了补算。
    """
    from app.main import _batch_nav_last_date_stock_count

    ids = [str(x).strip() for x in strategy_ids if str(x or "").strip()]
    if not ids:
        return False
    cached = _metrics_ids_with_cache(db, ids)
    missing = [s for s in ids if s not in cached]
    nav_meta = _batch_nav_last_date_stock_count(db, ids)
    stale: list[str] = []
    if cached:
        quoted = ",".join("'" + s.replace("'", "''") + "'" for s in cached)
        rows = db.execute(
            text(
                f"""
                SELECT strategy_id, last_trade_date
                FROM strategy_list_metrics
                WHERE strategy_id IN ({quoted})
                """
            )
        ).mappings().all()
        for r in rows:
            sid = str(r["strategy_id"]).strip()
            m_td = str(r.get("last_trade_date") or "").strip()[:10]
            n_td = str((nav_meta.get(sid) or {}).get("last_trade_date") or "").strip()[:10]
            if n_td and m_td != n_td:
                stale.append(sid)
    to_refresh = list(dict.fromkeys(missing + stale))
    if not to_refresh:
        return False
    try:
        refresh_strategy_list_metrics_cache(db, to_refresh, do_commit=do_commit)
        return True
    except Exception:
        _log.exception(
            "ensure strategy_list_metrics failed ids=%s", to_refresh[:20]
        )
        return False


def metrics_fields_from_row(row: dict[str, Any] | None) -> dict[str, Any]:
    if not row:
        return {
            "latest_nav": None,
            "last_1d_return": None,
            "last_5d_return": None,
            "period_since_rebalance_return": None,
            "month_return": None,
            "year_return": None,
            "last_trade_date": None,
            "stock_count_on_last_date": None,
        }
    return {
        "latest_nav": row.get("latest_nav"),
        "last_1d_return": row.get("last_1d_return"),
        "last_5d_return": row.get("last_5d_return"),
        "period_since_rebalance_return": row.get("period_since_rebalance_return"),
        "month_return": row.get("month_return"),
        "year_return": row.get("year_return"),
        "last_trade_date": row.get("last_trade_date"),
        "stock_count_on_last_date": row.get("stock_count_on_last_date"),
    }

"""策略列表页专用快照 strategy_list_metrics（/api/strategies 只读）；净值页等指标不在此表。"""

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


def _prune_list_metrics_not_in(db: Session, keep_ids: list[str], *, do_commit: bool = False) -> int:
    """删除不在当前列表策略集合内的快照行，使表行数与可见策略数一致。"""
    keep = [str(x).strip() for x in keep_ids if str(x or "").strip()]
    if not keep:
        cur = db.execute(text("SELECT COUNT(*) AS c FROM strategy_list_metrics")).mappings().first()
        n_before = int(cur["c"] or 0) if cur else 0
        if n_before:
            db.execute(text("DELETE FROM strategy_list_metrics"))
            if do_commit:
                db.commit()
        return n_before
    quoted = ",".join("'" + s.replace("'", "''") + "'" for s in keep)
    cur = db.execute(
        text(
            f"""
            SELECT COUNT(*) AS c FROM strategy_list_metrics
            WHERE strategy_id NOT IN ({quoted})
            """
        )
    ).mappings().first()
    n = int(cur["c"] or 0) if cur else 0
    if n:
        db.execute(
            text(f"DELETE FROM strategy_list_metrics WHERE strategy_id NOT IN ({quoted})")
        )
        if do_commit:
            db.commit()
    return n


def _upsert_metrics_rows(
    db: Session,
    strategy_ids: list[str],
    summaries: dict[str, dict[str, Any]],
    nav_meta: dict[str, dict],
    *,
    do_commit: bool = True,
) -> int:
    from app.main import _NAV_LIST_SUMMARY_EMPTY, _nav_list_period_rebalance_date

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
    for sid in strategy_ids:
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


def load_strategy_list_metrics_batch(
    db: Session, strategy_ids: list[str]
) -> dict[str, dict[str, Any]]:
    """单次查询读取快照（仅 /api/strategies，不扫净值表）。"""
    ids = [str(x).strip() for x in strategy_ids if str(x or "").strip()]
    if not ids:
        return {}
    quoted = ",".join("'" + s.replace("'", "''") + "'" for s in ids)
    rows = db.execute(
        text(
            f"""
            SELECT strategy_id, latest_nav, last_1d_return, last_5d_return,
                   period_since_rebalance_return, month_return, year_return,
                   last_trade_date, stock_count_on_last_date
            FROM strategy_list_metrics
            WHERE strategy_id IN ({quoted})
            """
        )
    ).mappings().all()
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        sid = str(r["strategy_id"]).strip()
        out[sid] = dict(r)
    return out


def refresh_strategy_list_metrics_cache(
    db: Session,
    strategy_ids: list[str] | None = None,
    *,
    do_commit: bool = True,
) -> int:
    """
    按列表口径重算并 UPSERT 快照。日常由 refresh_strategy_list_metrics_one 逐策略调用；
    strategy_ids 为空时刷新全部可见策略（仅脚本/手工补数，run_update 不再在末尾全量重算）。
    """
    from app.main import _batch_nav_last_date_stock_count
    from app.nav_list_metrics_calc import compute_strategy_list_metrics_snapshot

    ids = [str(x).strip() for x in (strategy_ids or []) if str(x or "").strip()]
    if not ids:
        ids = _enabled_visible_strategy_ids(db)
    if not ids:
        return 0

    summaries: dict[str, dict[str, Any]] = {}
    for sid in ids:
        summaries[sid] = compute_strategy_list_metrics_snapshot(db, sid)
        if len(ids) == 1:
            s = summaries[sid]
            _log.info(
                "strategy_list_metrics refresh sid=%s nav=%s 5d=%s month=%s year=%s period=%s",
                sid,
                s.get("latest_nav"),
                s.get("last_5d_return"),
                s.get("month_return"),
                s.get("year_return"),
                s.get("period_since_rebalance_return"),
            )
    nav_meta = _batch_nav_last_date_stock_count(db, ids)
    n = _upsert_metrics_rows(db, ids, summaries, nav_meta, do_commit=False)
    full_refresh = strategy_ids is None or not [
        x for x in (strategy_ids or []) if str(x or "").strip()
    ]
    if full_refresh:
        _prune_list_metrics_not_in(db, ids, do_commit=False)
    if do_commit:
        db.commit()
    return n


def refresh_strategy_list_metrics_one(
    db: Session, strategy_id: str, *, do_commit: bool = False
) -> None:
    """单策略数据更新完成后：重算并 UPSERT 该策略一行快照。"""
    sid = str(strategy_id or "").strip()
    if not sid:
        return
    refresh_strategy_list_metrics_cache(db, [sid], do_commit=do_commit)


def refresh_strategy_list_metrics_safe(
    db: Session, strategy_id: str, *, do_commit: bool = False
) -> None:
    """写快照失败只记日志，不中断 run_update。"""
    try:
        refresh_strategy_list_metrics_one(db, strategy_id, do_commit=do_commit)
    except Exception:
        _log.exception(
            "strategy_list_metrics refresh failed sid=%s", strategy_id
        )


def prune_strategy_list_metrics_orphans(db: Session, *, do_commit: bool = True) -> int:
    """全量任务收尾：删除已不在列表中的策略快照行（不重复重算指标）。"""
    ids = _enabled_visible_strategy_ids(db)
    return _prune_list_metrics_not_in(db, ids, do_commit=do_commit)


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

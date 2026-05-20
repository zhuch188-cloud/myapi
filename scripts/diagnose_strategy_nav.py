#!/usr/bin/env python3
"""诊断策略净值：尺度断裂、0513 前后、持仓快照、列表指标 vs 手算。"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text

from app.db import get_session
from app.main import (
    _batch_strategy_nav_list_summaries,
    _nav_list_period_rebalance_date,
    _nav_list_trade_date_as_date,
    _nav_period_start_nav_unit,
    _nav_unit_last_before,
)
from app.services import (
    _nav_last_good_trade_compact,
    _nav_rb_idx_on_date,
    _nav_scale_break_detected,
    _row_sql_date,
    latest_rebalance_date_by_strategy,
)
from app.sql_dialect import sql_date_compact_expr, sql_max_date_expr, sql_order_date_asc, sql_order_date_desc


def _around_0513(db, sid: str) -> None:
    rows = db.execute(
        text(
            f"""
            SELECT trade_date, nav_unit, daily_ret, rebalance_date
            FROM strategy_nav_daily
            WHERE strategy_id=:sid
              AND (
                {sql_date_compact_expr('trade_date')} BETWEEN '20260508' AND '20260525'
                OR trade_date BETWEEN '2026-05-08' AND '2026-05-25'
              )
            ORDER BY {sql_order_date_asc('trade_date')}
            """
        ),
        {"sid": sid},
    ).mappings().all()
    print(f"\n=== {sid} 20260508~20260525 净值（按日升序）===")
    prev_nu = None
    for r in rows:
        nu = float(r["nav_unit"] or 0)
        dr = r.get("daily_ret")
        jump = ""
        if prev_nu and prev_nu > 0 and nu > 0:
            ratio = nu / prev_nu
            if ratio < 0.9 or ratio > 1.1:
                jump = f"  *** 日环比 {ratio:.4f} ***"
        print(f"  {r['trade_date']}  nav={nu:.6f}  1d={dr}  rb={r.get('rebalance_date')}{jump}")
        prev_nu = nu


def _hold_0513(db, sid: str) -> None:
    rows = db.execute(
        text(
            """
            SELECT trade_date, rebalance_date, COUNT(*) cnt,
                   SUM(latest_weight) sw, AVG(period_return) apr
            FROM strategy_holding_daily
            WHERE strategy_id=:sid
              AND (
                trade_date IN ('2026-05-13', '20260513')
                OR rebalance_date IS NOT NULL
              )
            GROUP BY trade_date, rebalance_date
            ORDER BY trade_date DESC, rebalance_date DESC
            LIMIT 15
            """
        ),
        {"sid": sid},
    ).mappings().all()
    print(f"\n=== {sid} 持仓快照（近期 trade_date × rebalance_date）===")
    for r in rows:
        print(
            f"  T={r['trade_date']} rb={r['rebalance_date']} "
            f"n={r['cnt']} sum_w={float(r['sw'] or 0):.4f} avg本期={r['apr']}"
        )
    h513 = db.execute(
        text(
            """
            SELECT COUNT(*) c FROM strategy_holding_daily
            WHERE strategy_id=:sid
              AND (trade_date='2026-05-13' OR trade_date='20260513')
            """
        ),
        {"sid": sid},
    ).scalar()
    print(f"  2026-05-13 持仓行数: {int(h513 or 0)}  (增量净值需此快照推股数)")


def main() -> int:
    sid = (sys.argv[1] if len(sys.argv) > 1 else "CL6").strip()
    with get_session() as db:
        print(f"========== {sid} 诊断 ==========")
        print(f"scale_break={_nav_scale_break_detected(db, sid)}")
        print(f"last_good_nav_c={_nav_last_good_trade_compact(db, sid)}")
        lr = latest_rebalance_date_by_strategy(db).get(sid)
        print(f"positions.latest_rebalance={lr}")

        top = db.execute(
            text(
                f"""
                SELECT trade_date, nav_unit, daily_ret, rebalance_date
                FROM strategy_nav_daily WHERE strategy_id=:sid
                ORDER BY {sql_order_date_desc('trade_date')} LIMIT 8
                """
            ),
            {"sid": sid},
        ).mappings().all()
        if not top:
            print("无净值数据")
            return 1
        for r in top:
            print(f"  最新? {r['trade_date']} nav={r['nav_unit']} 1d={r.get('daily_ret')}")

        _around_0513(db, sid)
        _hold_0513(db, sid)

        last_td = _nav_list_trade_date_as_date(top[0]["trade_date"])
        prb = _nav_list_period_rebalance_date(db, sid, last_td)
        p0 = _nav_period_start_nav_unit(db, sid, prb) if prb else None
        mc = last_td.replace(day=1)
        yc = date(last_td.year, 1, 1)
        am = _nav_unit_last_before(db, sid, mc)
        ay = _nav_unit_last_before(db, sid, yc)
        ln = float(top[0]["nav_unit"])
        print(f"\n=== 手算 vs API ===")
        print(f"  last_td={last_td} period_rb={prb}")
        print(f"  period_start_nav={p0}  month_anchor={am}  year_anchor={ay}")
        if p0 and p0 > 0:
            print(f"  手算本期 = {ln/p0 - 1:.6f}")
        if am and am > 0:
            print(f"  手算本月 = {ln/am - 1:.6f}")
        if len(top) > 5 and float(top[5]["nav_unit"] or 0) > 0:
            n5 = float(top[5]["nav_unit"])
            print(f"  手算5日 = {ln/n5 - 1:.6f}  (第6条交易日 nav={n5})")
        print(f"  API摘要: {_batch_strategy_nav_list_summaries(db, [sid]).get(sid)}")

        # 调仓期数
        rb_rows = db.execute(
            text(
                """
                SELECT DISTINCT rebalance_date FROM strategy_positions
                WHERE strategy_id=:sid ORDER BY rebalance_date
                """
            ),
            {"sid": sid},
        ).mappings().all()
        rb_sorted = [_row_sql_date(r["rebalance_date"]) for r in rb_rows]
        rb_sorted = [d for d in rb_sorted if d]
        if rb_sorted:
            idx, cur = _nav_rb_idx_on_date(rb_sorted, last_td)
            print(f"\n=== 调仓期 === count={len(rb_sorted)} anchor_idx={idx} current_rb={cur}")

        mx = db.execute(
            text(
                f"""
                SELECT
                  (SELECT MAX({sql_max_date_expr('trade_date')}) FROM strategy_nav_daily WHERE strategy_id=:sid) nav_m,
                  (SELECT MAX({sql_max_date_expr('trade_date')}) FROM strategy_holding_daily WHERE strategy_id=:sid) hold_m,
                  (SELECT COUNT(*) FROM strategy_nav_daily WHERE strategy_id=:sid AND {sql_date_compact_expr('trade_date')} > '20260513') nav_after_513
                """
            ),
            {"sid": sid},
        ).mappings().first()
        print(f"\n=== 汇总 ===")
        print(f"  nav_max={mx.get('nav_m')} hold_max={mx.get('hold_m')} rows_after_0513={mx.get('nav_after_513')}")

        if _nav_scale_break_detected(db, sid):
            print("\n结论倾向: **基础数据/尺度断裂** — 0513 后与历史 nav_unit 不连续，指标会假。")
        elif int(h513 or 0) == 0:
            print("\n结论倾向: **基础数据缺失** — 无 0513 持仓快照，增量只能猜权重，易偏。")
        else:
            print("\n结论倾向: 若 0513 前后 nav 连续且快照存在，再查 **计算逻辑/期初锚定**。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

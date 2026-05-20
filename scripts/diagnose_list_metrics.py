#!/usr/bin/env python3
"""对照策略列表「本期/本月/本年」与净值序列手算是否一致。"""
from __future__ import annotations

import argparse
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
    _nav_period_start_nav_from_rows,
    _nav_anchor_nav_unit_before_rows,
    _row_sql_date,
)
from app.sql_dialect import sql_order_date_asc


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("strategy_id", nargs="?", default="cs66")
    args = ap.parse_args()
    sid = str(args.strategy_id).strip().lower()

    with get_session() as db:
        rows = db.execute(
            text(
                f"""
                SELECT trade_date, nav_unit, daily_ret, rebalance_date
                FROM strategy_nav_daily
                WHERE strategy_id=:sid
                ORDER BY {sql_order_date_asc("trade_date")}
                """
            ),
            {"sid": sid},
        ).mappings().all()
        if not rows:
            print(f"{sid}: 无 strategy_nav_daily 数据")
            return 1

        rows_asc = list(rows)
        rows_desc = list(reversed(rows_asc))
        top = rows_desc[0]
        last_td = _nav_list_trade_date_as_date(top["trade_date"])
        last_nav = float(top["nav_unit"])
        print(f"=== {sid} 末净值日 {top['trade_date']} nav_unit={last_nav:.6f} ===\n")

        rb_rows = db.execute(
            text(
                "SELECT DISTINCT rebalance_date FROM strategy_positions WHERE strategy_id=:sid"
            ),
            {"sid": sid},
        ).fetchall()
        rb_sorted = sorted(
            d for d in (_row_sql_date(r[0]) for r in rb_rows) if d is not None
        )
        max_rb = _nav_list_period_rebalance_date(db, sid, last_td)
        p0 = _nav_period_start_nav_from_rows(rows_asc, max_rb) if max_rb else None
        month_cut = last_td.replace(day=1)
        year_cut = date(last_td.year, 1, 1)
        am = _nav_anchor_nav_unit_before_rows(rows_desc, month_cut)
        ay = _nav_anchor_nav_unit_before_rows(rows_desc, year_cut)

        print(f"锚定调仓日( positions, ≤{last_td} ): {max_rb}")
        print(f"本期期初 nav (首个交易日≥调仓日): {p0}")
        if p0 and p0 > 0:
            print(f"  手算本期: {(last_nav / p0 - 1) * 100:.4f}%")
        print(f"本月锚定(严格早于 {month_cut}): {am}")
        if am and am > 0:
            print(f"  手算本月: {(last_nav / am - 1) * 100:.4f}%")
        print(f"本年锚定(严格早于 {year_cut}): {ay}")
        if ay and ay > 0:
            print(f"  手算本年: {(last_nav / ay - 1) * 100:.4f}%")

        api = _batch_strategy_nav_list_summaries(db, [sid]).get(sid, {})
        print("\n=== API 列表汇总 ===")
        print(f"  latest_nav: {api.get('latest_nav')}")
        print(f"  period: {api.get('period_since_rebalance_return')}")
        print(f"  month: {api.get('month_return')}")
        print(f"  year: {api.get('year_return')}")
        print(f"  1d: {api.get('last_1d_return')}")
        print(f"  5d: {api.get('last_5d_return')}")

        if max_rb and p0 and rb_sorted:
            from app.services import _nav_rb_idx_on_date as _rb_idx

            idx, cur = _rb_idx(rb_sorted, last_td)
            print(f"\n调仓期索引 {idx}/{len(rb_sorted) - 1} current_rb={cur}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

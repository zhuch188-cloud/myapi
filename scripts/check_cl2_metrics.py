#!/usr/bin/env python3
"""校验 CL2 列表指标（1日/5日/本期/本月）与库内净值、持仓是否一致。"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text

from app.db import get_session
from app.main import _batch_strategy_nav_list_summaries, _nav_list_summary_from_desc_rows
from app.main import _nav_list_trade_date_as_date
from app.sql_dialect import sql_max_date_expr, sql_order_date_desc


def main() -> int:
    sid = "CL2"
    with get_session() as db:
        nav_rows = db.execute(
            text(
                f"""
                SELECT trade_date, nav_unit, daily_ret, rebalance_date
                FROM strategy_nav_daily
                WHERE strategy_id=:sid
                ORDER BY {sql_order_date_desc('trade_date')}
                LIMIT 30
                """
            ),
            {"sid": sid},
        ).mappings().all()
        print(f"=== {sid} 最近 30 条净值 ===")
        for r in nav_rows[:15]:
            print(
                f"  {r['trade_date']} nav={r['nav_unit']:.6f} "
                f"daily_ret={r.get('daily_ret')} rb={r.get('rebalance_date')}"
            )

        rows_desc = list(nav_rows)
        max_rb = None
        if rows_desc and rows_desc[0].get("rebalance_date"):
            max_rb = _nav_list_trade_date_as_date(rows_desc[0]["rebalance_date"])
        pack = _nav_list_summary_from_desc_rows(rows_desc, max_rb)
        print(f"\n=== 用最近30条算的摘要（完整历史可能不同）===")
        print(f"  max_rb(最新行rebalance_date)={max_rb}")
        if pack:
            print(f"  last_nav={pack[0]:.6f} 1d={pack[1]} 5d={pack[2]} 本期={pack[3]} 本月={pack[4]} 本年={pack[5]}")

        full = _batch_strategy_nav_list_summaries(db, [sid])
        print(f"\n=== _batch_strategy_nav_list_summaries（全表）===")
        print(full.get(sid))

        mx_rb_pos = db.execute(
            text(
                f"SELECT {sql_max_date_expr('rebalance_date')} AS m "
                "FROM strategy_positions WHERE strategy_id=:sid"
            ),
            {"sid": sid},
        ).mappings().first()
        print(f"\n=== positions MAX(rebalance_date) 日历 = {mx_rb_pos}")

        h = db.execute(
            text(
                """
                SELECT trade_date, rebalance_date, COUNT(*) cnt,
                       AVG(period_return) avg_pr, AVG(ret_5d) avg_r5
                FROM strategy_holding_daily
                WHERE strategy_id=:sid
                GROUP BY trade_date, rebalance_date
                ORDER BY trade_date DESC, rebalance_date DESC
                LIMIT 8
                """
            ),
            {"sid": sid},
        ).mappings().all()
        print(f"\n=== 持仓按 trade_date/rebalance_date（最近）===")
        for r in h:
            print(
                f"  T={r['trade_date']} rb={r['rebalance_date']} n={r['cnt']} "
                f"avg本期={r['avg_pr']} avg5d={r['avg_r5']}"
            )

        # 本期净值：调仓日以来首条 nav
        if max_rb and rows_desc:
            all_nav = db.execute(
                text(
                    f"""
                    SELECT trade_date, nav_unit, rebalance_date
                    FROM strategy_nav_daily
                    WHERE strategy_id=:sid
                    ORDER BY {sql_order_date_desc('trade_date')}
                    """
                ),
                {"sid": sid},
            ).mappings().all()
            all_nav.sort(
                key=lambda r: _nav_list_trade_date_as_date(r["trade_date"]),
                reverse=True,
            )
            first_after = None
            for r in reversed(all_nav):
                td = _nav_list_trade_date_as_date(r["trade_date"])
                if td >= max_rb:
                    first_after = r
                    break
            print(f"\n=== 本期锚定 rebalance={max_rb} 首条 nav>={max_rb} ===")
            if first_after:
                print(
                    f"  {first_after['trade_date']} nav={first_after['nav_unit']} "
                    f"rb={first_after.get('rebalance_date')}"
                )
                if rows_desc:
                    ln = float(rows_desc[0]["nav_unit"])
                    fn = float(first_after["nav_unit"])
                    print(f"  手算本期 = {ln/fn-1:.6f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

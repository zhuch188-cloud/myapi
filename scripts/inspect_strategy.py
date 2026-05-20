#!/usr/bin/env python3
"""快速查看单策略：配置、调仓期、净值/持仓行数、末行样本。"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text

from app.db import get_session
from app.sql_dialect import sql_date_compact_expr, sql_max_date_expr, sql_order_date_desc


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("strategy_id", help="如 CS66")
    args = ap.parse_args()
    sid = str(args.strategy_id).strip()

    td_expr = sql_date_compact_expr("trade_date")
    rb_expr = sql_date_compact_expr("rebalance_date")

    with get_session() as db:
        cfg = db.execute(
            text(
                "SELECT strategy_id, strategy_name, status, file_dir, file_name, "
                "weight_display_mode, rebalance_freq FROM strategy_configs WHERE strategy_id=:sid"
            ),
            {"sid": sid},
        ).mappings().first()
        print("=== strategy_configs ===")
        print(dict(cfg) if cfg else "(无记录)")

        pos = db.execute(
            text(
                f"""
                SELECT COUNT(*) AS rows,
                       COUNT(DISTINCT {rb_expr}) AS periods,
                       MIN({rb_expr}) AS min_rb,
                       MAX({rb_expr}) AS max_rb
                FROM strategy_positions WHERE strategy_id=:sid
                """
            ),
            {"sid": sid},
        ).mappings().first()
        print("\n=== strategy_positions ===")
        print(dict(pos) if pos else {})

        rb_pos = db.execute(
            text(
                f"""
                SELECT DISTINCT rebalance_date
                FROM strategy_positions WHERE strategy_id=:sid
                ORDER BY {sql_order_date_desc('rebalance_date')}
                LIMIT 15
                """
            ),
            {"sid": sid},
        ).fetchall()
        if rb_pos:
            print("最近调仓日( positions，最多15个 ):")
            for r in rb_pos:
                print(f"  {r[0]}")

        nav = db.execute(
            text(
                f"""
                SELECT COUNT(*) AS rows,
                       MIN({td_expr}) AS min_td,
                       MAX({td_expr}) AS max_td
                FROM strategy_nav_daily WHERE strategy_id=:sid
                """
            ),
            {"sid": sid},
        ).mappings().first()
        print("\n=== strategy_nav_daily ===")
        print(dict(nav) if nav else {})

        hold = db.execute(
            text(
                f"""
                SELECT COUNT(*) AS rows,
                       MIN({td_expr}) AS min_td,
                       MAX({td_expr}) AS max_td
                FROM strategy_holding_daily WHERE strategy_id=:sid
                """
            ),
            {"sid": sid},
        ).mappings().first()
        print("\n=== strategy_holding_daily ===")
        print(dict(hold) if hold else {})

        # 客户端持仓页：latest_only=1 同源查询
        rb_latest = db.execute(
            text(
                f"""
                SELECT DISTINCT rebalance_date
                FROM strategy_holding_daily
                WHERE strategy_id=:sid
                  AND trade_date = (
                    SELECT {sql_max_date_expr('trade_date')}
                    FROM strategy_holding_daily WHERE strategy_id=:sid
                  )
                ORDER BY {sql_order_date_desc('rebalance_date')}
                """
            ),
            {"sid": sid},
        ).fetchall()
        print("\n=== API rebalance-dates?latest_only=1 (持仓页下拉) ===")
        if rb_latest:
            for r in rb_latest:
                print(f"  {r[0]}")
        else:
            print("  (空) — 通常因尚未跑持仓更新，strategy_holding_daily 无数据")

        if nav and int(nav.get("rows") or 0) > 0:
            tail = db.execute(
                text(
                    f"""
                    SELECT trade_date, nav_unit, daily_ret, rebalance_date
                    FROM strategy_nav_daily WHERE strategy_id=:sid
                    ORDER BY {td_expr} DESC LIMIT 5
                    """
                ),
                {"sid": sid},
            ).mappings().all()
            print("\n=== 净值末 5 行 ===")
            for r in reversed(tail):
                print(
                    f"  {r['trade_date']} nav={r['nav_unit']} ret={r['daily_ret']} rb={r['rebalance_date']}"
                )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

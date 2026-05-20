#!/usr/bin/env python3
"""CL3：删至 20260513 后增量更新 — 检查 0513/0514 尺度与列表指标。"""
from __future__ import annotations

import sys
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
)
from app.services import _nav_scale_break_detected
from app.sql_dialect import sql_max_date_expr, sql_order_date_desc


def main() -> int:
    sid = (sys.argv[1] if len(sys.argv) > 1 else "CL4").strip()
    with get_session() as db:
        print(f"=== {sid} 尺度断裂检测 ===")
        print(f"  scale_break_detected={_nav_scale_break_detected(db, sid)}")

        rows = db.execute(
            text(
                f"""
                SELECT trade_date, nav_unit, daily_ret, rebalance_date
                FROM strategy_nav_daily
                WHERE strategy_id=:sid
                ORDER BY {sql_order_date_desc('trade_date')}
                LIMIT 12
                """
            ),
            {"sid": sid},
        ).mappings().all()
        print(f"\n=== 最近净值（关注 2026-05-13 与次日）===")
        for r in rows:
            td = r["trade_date"]
            nu = float(r["nav_unit"] or 0)
            dr = r.get("daily_ret")
            print(f"  {td}  nav={nu:.6f}  1d={dr}  rb={r.get('rebalance_date')}")
        if len(rows) >= 2:
            a, b = float(rows[0]["nav_unit"]), float(rows[1]["nav_unit"])
            if b > 0:
                print(f"  最新/前一日 nav 比 = {a/b:.4f}  (若≈0.27 或≈3.7 多为尺度断)")

        last_td = _nav_list_trade_date_as_date(rows[0]["trade_date"])
        prb = _nav_list_period_rebalance_date(db, sid, last_td)
        p0 = _nav_period_start_nav_unit(db, sid, prb) if prb else None
        print(f"\n=== 本期锚定 ===")
        print(f"  period_rb(positions)={prb}")
        print(f"  period_start_nav_unit={p0}")
        if p0 and rows:
            ln = float(rows[0]["nav_unit"])
            print(f"  手算本期 = {ln/p0 - 1:.6f}")

        print(f"\n=== 列表摘要（API 口径）===")
        print(_batch_strategy_nav_list_summaries(db, [sid]).get(sid))

        mx = db.execute(
            text(
                f"""
                SELECT MAX({sql_max_date_expr('trade_date')}) AS nav_m,
                       (SELECT MAX({sql_max_date_expr('trade_date')})
                        FROM strategy_holding_daily WHERE strategy_id=:sid) AS hold_m
                FROM strategy_nav_daily WHERE strategy_id=:sid
                """
            ),
            {"sid": sid},
        ).mappings().first()
        print(f"\n=== 末日期 ===")
        print(f"  nav_max={mx.get('nav_m')}  holding_max={mx.get('hold_m')}")

    print(
        "\n修复后请: 1) 部署  2) purge --fix-scale-break 或删 >0513 净值  "
        "3) 仅更新 CL3"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

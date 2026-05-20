#!/usr/bin/env python3
"""
删除库内「最近 N 个自然日（含最新行情日）」的净值与同日持仓快照，便于清掉错误增量后重跑更新。

用法（仓库根目录，需 TURSO_DATABASE_URL / TURSO_AUTH_TOKEN 或本地 SQLite）：
  python scripts/purge_recent_nav_week.py              # 仅预览
  python scripts/purge_recent_nav_week.py --apply    # 执行删除
  python scripts/purge_recent_nav_week.py --days 7 --apply
  python scripts/purge_recent_nav_week.py --apply --strategy-id CL3
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text

from app.db import get_session
from app.sql_dialect import sql_date_compact_expr


def _compact_to_date(c: str) -> date:
    s = str(c).strip().replace("-", "")[:8]
    return datetime.strptime(s, "%Y%m%d").date()


def _date_to_cmp(d: date) -> str:
    return d.strftime("%Y%m%d")


def main() -> int:
    ap = argparse.ArgumentParser(description="删除最近一周净值及同日持仓快照")
    ap.add_argument("--days", type=int, default=7, help="自库内最新 trade_date 起向前 N 个自然日（含末日）")
    ap.add_argument("--apply", action="store_true", help="执行删除；默认仅统计预览")
    ap.add_argument("--strategy-id", action="append", dest="strategy_ids", help="仅处理指定策略，可重复")
    args = ap.parse_args()
    days = max(1, int(args.days))
    sid_filter = [str(x).strip() for x in (args.strategy_ids or []) if str(x).strip()]

    td_expr = sql_date_compact_expr("trade_date")
    sid_clause = ""
    params: dict = {}
    if sid_filter:
        quoted = ",".join("'" + s.replace("'", "''") + "'" for s in sid_filter)
        sid_clause = f" AND strategy_id IN ({quoted})"

    with get_session() as db:
        mx = db.execute(
            text(
                f"""
                SELECT MAX({td_expr}) AS mx
                FROM strategy_nav_daily
                WHERE trade_date IS NOT NULL{sid_clause}
                """
            ),
            params,
        ).mappings().first()
        mx_hold = db.execute(
            text(
                f"""
                SELECT MAX({td_expr}) AS mx
                FROM strategy_holding_daily
                WHERE trade_date IS NOT NULL{sid_clause}
                """
            ),
            params,
        ).mappings().first()
        mx_nav = mx.get("mx") if mx else None
        mx_h = mx_hold.get("mx") if mx_hold else None
        candidates = [x for x in (mx_nav, mx_h) if x]
        if not candidates:
            print("库内无 strategy_nav_daily / strategy_holding_daily 可删数据。")
            return 0
        latest_cmp = max(str(x) for x in candidates)
        latest_d = _compact_to_date(latest_cmp)
        cutoff_d = latest_d - timedelta(days=days - 1)
        cutoff_cmp = _date_to_cmp(cutoff_d)

        print(f"最新行情日（库内）: {latest_d.isoformat()} ({latest_cmp})")
        print(f"删除区间（含起止）: {cutoff_d.isoformat()} ~ {latest_d.isoformat()}  共 {days} 个自然日")
        if sid_filter:
            print(f"策略范围: {', '.join(sid_filter)}")
        else:
            print("策略范围: 全部")

        nav_cnt = db.execute(
            text(
                f"""
                SELECT COUNT(*) AS c FROM strategy_nav_daily
                WHERE {td_expr} >= :cutoff{sid_clause}
                """
            ),
            {**params, "cutoff": cutoff_cmp},
        ).scalar()
        hold_cnt = db.execute(
            text(
                f"""
                SELECT COUNT(*) AS c FROM strategy_holding_daily
                WHERE {td_expr} >= :cutoff{sid_clause}
                """
            ),
            {**params, "cutoff": cutoff_cmp},
        ).scalar()
        print(f"将删除 strategy_nav_daily: {int(nav_cnt or 0)} 行")
        print(f"将删除 strategy_holding_daily: {int(hold_cnt or 0)} 行")

        by_sid = db.execute(
            text(
                f"""
                SELECT strategy_id, COUNT(*) AS c
                FROM strategy_nav_daily
                WHERE {td_expr} >= :cutoff{sid_clause}
                GROUP BY strategy_id
                ORDER BY strategy_id
                """
            ),
            {**params, "cutoff": cutoff_cmp},
        ).mappings().all()
        if by_sid:
            print("按策略净值行数:")
            for r in by_sid:
                print(f"  {r['strategy_id']}: {r['c']}")

        if not args.apply:
            print("\n预览模式，未删除。确认后加 --apply 执行。")
            return 0

        db.execute(
            text(
                f"""
                DELETE FROM strategy_nav_daily
                WHERE {td_expr} >= :cutoff{sid_clause}
                """
            ),
            {**params, "cutoff": cutoff_cmp},
        )
        db.execute(
            text(
                f"""
                DELETE FROM strategy_holding_daily
                WHERE {td_expr} >= :cutoff{sid_clause}
                """
            ),
            {**params, "cutoff": cutoff_cmp},
        )
        db.commit()
        print("\n已提交删除。请在管理端对受影响策略执行「仅更新最新交易日」或全量重算净值。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

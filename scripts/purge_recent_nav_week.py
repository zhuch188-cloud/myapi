#!/usr/bin/env python3
"""
删除库内「最近 N 个自然日（含最新行情日）」的净值与同日持仓快照，便于清掉错误增量后重跑更新。

用法（仓库根目录，需 TURSO_DATABASE_URL / TURSO_AUTH_TOKEN 或本地 SQLite）：
  python scripts/purge_recent_nav_week.py              # 仅预览
  python scripts/purge_recent_nav_week.py --apply    # 执行删除
  python scripts/purge_recent_nav_week.py --days 7 --apply
  python scripts/purge_recent_nav_week.py --apply --strategy-id CL3
  python scripts/purge_recent_nav_week.py --days 5 --strategy-id CS66   # 仅 CS66，先预览
  python scripts/purge_recent_nav_week.py --days 5 --strategy-id CS66 --apply
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

from app.db import SessionLocalFactory, init_database
from app.services import _nav_last_good_trade_compact, _nav_scale_break_detected
from app.sql_dialect import sql_date_compact_expr


def _open_db():
    init_database()
    return SessionLocalFactory()


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
    ap.add_argument(
        "--fix-scale-break",
        action="store_true",
        help="删除末净值日之后、且与历史 nav_unit 尺度断裂的净值行（修复 -70% 假指标）",
    )
    args = ap.parse_args()
    days = max(1, int(args.days))
    sid_filter = [str(x).strip() for x in (args.strategy_ids or []) if str(x).strip()]
    fix_scale = bool(args.fix_scale_break)

    td_expr = sql_date_compact_expr("trade_date")
    sid_clause = ""
    params: dict = {}
    if sid_filter:
        quoted = ",".join("'" + s.replace("'", "''") + "'" for s in sid_filter)
        sid_clause = f" AND strategy_id IN ({quoted})"

    db = _open_db()
    try:
        if sid_filter:
            for sid in sid_filter:
                cfg = db.execute(
                    text(
                        "SELECT strategy_id, strategy_name, status FROM strategy_configs "
                        "WHERE strategy_id=:sid"
                    ),
                    {"sid": sid},
                ).mappings().first()
                if not cfg:
                    print(f"错误：strategy_configs 中不存在 strategy_id={sid!r}，已中止。")
                    return 1
                print(
                    f"已确认策略: {cfg['strategy_id']} "
                    f"({cfg.get('strategy_name') or ''}) status={cfg.get('status')}"
                )
            print()

        if fix_scale:
            strategies = sid_filter or [
                str(r[0]).strip()
                for r in db.execute(
                    text(
                        "SELECT DISTINCT strategy_id FROM strategy_nav_daily ORDER BY strategy_id"
                    )
                ).fetchall()
                if str(r[0] or "").strip()
            ]
            if not strategies:
                print("无净值策略。")
                return 0
            print("=== 尺度断裂修复：删除末净值日之后错误尺度的净值行 ===\n")
            total_del = 0
            for sid in strategies:
                if not _nav_scale_break_detected(db, sid):
                    continue
                ac = _nav_last_good_trade_compact(db, sid)
                if not ac or len(ac) < 8:
                    continue
                cnt = db.execute(
                    text(
                        f"""
                        SELECT COUNT(*) FROM strategy_nav_daily
                        WHERE strategy_id=:sid AND {td_expr} > :ac
                        """
                    ),
                    {"sid": sid, "ac": ac},
                ).scalar()
                cnt = int(cnt or 0)
                if cnt <= 0:
                    continue
                print(f"  {sid}: 末净值日 {ac}，将删其后 {cnt} 行")
                if args.apply:
                    db.execute(
                        text(
                            f"""
                            DELETE FROM strategy_nav_daily
                            WHERE strategy_id=:sid AND {td_expr} > :ac
                            """
                        ),
                        {"sid": sid, "ac": ac},
                    )
                    total_del += cnt
            if args.apply:
                db.commit()
                print(f"\n已删除 {total_del} 行。请部署新代码后执行「仅更新」重算净值。")
            else:
                print("\n预览模式，未删除。加 --apply 执行。")
            return 0

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

        if sid_filter:
            for sid in sid_filter:
                dates = db.execute(
                    text(
                        f"""
                        SELECT DISTINCT {td_expr} AS d
                        FROM strategy_nav_daily
                        WHERE strategy_id=:sid AND {td_expr} >= :cutoff
                        ORDER BY d
                        """
                    ),
                    {"sid": sid, "cutoff": cutoff_cmp},
                ).fetchall()
                if dates:
                    print(f"\n{sid} 将删净值交易日 ({len(dates)} 个):")
                    print("  " + ", ".join(str(r[0]) for r in dates))
                hold_dates = db.execute(
                    text(
                        f"""
                        SELECT DISTINCT {td_expr} AS d
                        FROM strategy_holding_daily
                        WHERE strategy_id=:sid AND {td_expr} >= :cutoff
                        ORDER BY d
                        """
                    ),
                    {"sid": sid, "cutoff": cutoff_cmp},
                ).fetchall()
                if hold_dates:
                    print(f"{sid} 将删持仓快照交易日 ({len(hold_dates)} 个):")
                    print("  " + ", ".join(str(r[0]) for r in hold_dates))
                anchor = db.execute(
                    text(
                        f"""
                        SELECT trade_date, nav_unit, daily_ret, rebalance_date
                        FROM strategy_nav_daily
                        WHERE strategy_id=:sid AND {td_expr} < :cutoff
                        ORDER BY {td_expr} DESC
                        LIMIT 1
                        """
                    ),
                    {"sid": sid, "cutoff": cutoff_cmp},
                ).mappings().first()
                if anchor:
                    print(
                        f"{sid} 删除后保留的末净值行: trade_date={anchor.get('trade_date')} "
                        f"nav_unit={anchor.get('nav_unit')} rebalance_date={anchor.get('rebalance_date')}"
                    )
                else:
                    print(f"{sid} 警告：cutoff 之前无净值行，删除后该策略净值将为空。")

        if not sid_filter and args.apply:
            print("\n错误：未指定 --strategy-id 时不允许 --apply（避免误删全部策略）。")
            return 1

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
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

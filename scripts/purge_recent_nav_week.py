#!/usr/bin/env python3
"""
删除库内最近净值/持仓快照，便于清掉错误增量后重跑更新。
同步清理 strategy_list_metrics 快照，避免列表页仍显示已删净值。

用法（仓库根目录，需 TURSO_DATABASE_URL / TURSO_AUTH_TOKEN）：
  python scripts/purge_recent_nav_week.py --strategy-id cs66 --latest-only
  python scripts/purge_recent_nav_week.py --strategy-id cs66 --latest-only --apply
  python scripts/purge_recent_nav_week.py --strategy-id cs66 --on-date 2026-05-21 --apply
  python scripts/purge_recent_nav_week.py --days 1 --strategy-id cs66 --apply
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
from app.sql_dialect import sql_date_compact_expr, sql_order_date_desc
from app.timeutil import today as beijing_today


def _open_db():
    init_database()
    return SessionLocalFactory()


def _compact_to_date(c: str) -> date:
    s = str(c).strip().replace("-", "")[:8]
    return datetime.strptime(s, "%Y%m%d").date()


def _date_to_cmp(d: date) -> str:
    return d.strftime("%Y%m%d")


def _resolve_strategy_ids(db, raw_ids: list[str]) -> list[str]:
    """按 strategy_configs 解析 canonical strategy_id（大小写不敏感）。"""
    out: list[str] = []
    for raw in raw_ids:
        key = str(raw or "").strip()
        if not key:
            continue
        row = db.execute(
            text(
                """
                SELECT strategy_id FROM strategy_configs
                WHERE LOWER(strategy_id) = LOWER(:k)
                LIMIT 1
                """
            ),
            {"k": key},
        ).mappings().first()
        if not row:
            raise ValueError(f"strategy_configs 中不存在 strategy_id={key!r}")
        sid = str(row["strategy_id"]).strip()
        if sid not in out:
            out.append(sid)
    return out


def _sid_in_clause(sids: list[str]) -> tuple[str, dict]:
    binds: dict = {}
    ph = []
    for i, sid in enumerate(sids):
        k = f"sid{i}"
        binds[k] = sid
        ph.append(f":{k}")
    return ",".join(ph), binds


def _purge_list_metrics(db, sids: list[str], *, do_commit: bool) -> int:
    if not sids:
        return 0
    in_clause, binds = _sid_in_clause(sids)
    cur = db.execute(
        text(f"DELETE FROM strategy_list_metrics WHERE strategy_id IN ({in_clause})"),
        binds,
    )
    if do_commit:
        db.commit()
    return int(getattr(cur, "rowcount", 0) or 0)


def _print_nav_tail(db, sids: list[str], td_expr: str) -> None:
    for sid in sids:
        row = db.execute(
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
        if row:
            print(
                f"  {sid} 当前末净值: trade_date={row.get('trade_date')} "
                f"nav_unit={row.get('nav_unit')}"
            )
        else:
            print(f"  {sid} 当前无净值行")


def main() -> int:
    ap = argparse.ArgumentParser(description="删除最近净值/持仓快照（可选清理列表指标缓存）")
    ap.add_argument("--days", type=int, default=7, help="自库内最新 trade_date 起向前 N 个自然日（含末日）")
    ap.add_argument("--apply", action="store_true", help="执行删除；默认仅预览")
    ap.add_argument("--strategy-id", action="append", dest="strategy_ids", help="策略 ID，可重复；apply 时必填")
    ap.add_argument(
        "--latest-only",
        action="store_true",
        help="仅删各策略库内 MAX(trade_date) 那一日（推荐：撤销最近一次更新）",
    )
    ap.add_argument(
        "--on-date",
        dest="on_date",
        metavar="YYYY-MM-DD",
        help="仅删指定交易日（北京日历日，兼容库内 YYYY-MM-DD / YYYYMMDD）",
    )
    ap.add_argument(
        "--fix-scale-break",
        action="store_true",
        help="删除末净值日之后尺度断裂的净值行",
    )
    args = ap.parse_args()
    days = max(1, int(args.days))
    fix_scale = bool(args.fix_scale_break)
    latest_only = bool(args.latest_only)
    on_date_raw = (args.on_date or "").strip()

    td_expr = sql_date_compact_expr("trade_date")

    db = _open_db()
    try:
        try:
            sid_filter = _resolve_strategy_ids(db, args.strategy_ids or [])
        except ValueError as ex:
            print(f"错误：{ex}")
            return 1

        if sid_filter:
            for sid in sid_filter:
                cfg = db.execute(
                    text(
                        "SELECT strategy_id, strategy_name, status FROM strategy_configs "
                        "WHERE strategy_id=:sid"
                    ),
                    {"sid": sid},
                ).mappings().first()
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
            touched: list[str] = []
            for sid in strategies:
                if not _nav_scale_break_detected(db, sid):
                    continue
                ac = _nav_last_good_trade_compact(db, sid) or ""
                if len(ac) < 8:
                    continue
                cnt = int(
                    db.execute(
                        text(
                            f"""
                            SELECT COUNT(*) FROM strategy_nav_daily
                            WHERE strategy_id=:sid AND {td_expr} > :ac
                            """
                        ),
                        {"sid": sid, "ac": ac},
                    ).scalar()
                    or 0
                )
                if cnt <= 0:
                    continue
                print(f"  {sid}: 末净值日 {ac}，将删其后 {cnt} 行")
                touched.append(sid)
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
                n_m = _purge_list_metrics(db, touched, do_commit=True)
                print(f"\n已删除净值 {total_del} 行；清理列表快照 {n_m} 条。")
                _print_nav_tail(db, touched, td_expr)
            else:
                print("\n预览模式，未删除。加 --apply 执行。")
            return 0

        if not sid_filter:
            print("请指定 --strategy-id（apply 时必须，预览也建议指定以免误判）。")
            return 1

        in_clause, sid_binds = _sid_in_clause(sid_filter)

        if latest_only:
            print("=== 模式：仅删除各策略库内最新 trade_date ===\n")
            preview_rows = db.execute(
                text(
                    f"""
                    SELECT n.strategy_id, n.trade_date, n.nav_unit, {td_expr} AS dcmp
                    FROM strategy_nav_daily n
                    INNER JOIN (
                        SELECT strategy_id, MAX({td_expr}) AS mx
                        FROM strategy_nav_daily
                        WHERE strategy_id IN ({in_clause})
                        GROUP BY strategy_id
                    ) t ON t.strategy_id = n.strategy_id AND {td_expr} = t.mx
                    WHERE n.strategy_id IN ({in_clause})
                    ORDER BY n.strategy_id
                    """
                ),
                sid_binds,
            ).mappings().all()
            if not preview_rows:
                print("无匹配净值行。")
                return 0
            print("将删除 strategy_nav_daily:")
            for r in preview_rows:
                print(
                    f"  {r['strategy_id']}: trade_date={r.get('trade_date')} "
                    f"nav_unit={r.get('nav_unit')}"
                )
            hold_cnt = int(
                db.execute(
                    text(
                        f"""
                        SELECT COUNT(*) FROM strategy_holding_daily h
                        WHERE h.strategy_id IN ({in_clause})
                          AND {td_expr} IN (
                            SELECT MAX({td_expr}) FROM strategy_nav_daily n
                            WHERE n.strategy_id = h.strategy_id
                          )
                        """
                    ),
                    sid_binds,
                ).scalar()
                or 0
            )
            print(f"将删除 strategy_holding_daily（同日末净值）: {hold_cnt} 行")
            print(f"将清理 strategy_list_metrics: {len(sid_filter)} 条（删后需重算或下次更新刷新）")

            if not args.apply:
                print("\n预览模式，未删除。确认后加 --apply。")
                return 0

            nav_del = db.execute(
                text(
                    f"""
                    DELETE FROM strategy_nav_daily
                    WHERE strategy_id IN ({in_clause})
                      AND {td_expr} IN (
                        SELECT MAX({td_expr}) FROM strategy_nav_daily n2
                        WHERE n2.strategy_id = strategy_nav_daily.strategy_id
                      )
                    """
                ),
                sid_binds,
            )
            hold_del = db.execute(
                text(
                    f"""
                    DELETE FROM strategy_holding_daily
                    WHERE strategy_id IN ({in_clause})
                      AND {td_expr} IN (
                        SELECT MAX({td_expr}) FROM strategy_nav_daily n2
                        WHERE n2.strategy_id = strategy_holding_daily.strategy_id
                      )
                    """
                ),
                sid_binds,
            )
            db.commit()
            n_m = _purge_list_metrics(db, sid_filter, do_commit=True)
            print(
                f"\n已删除净值 {int(getattr(nav_del, 'rowcount', 0) or 0)} 行、"
                f"持仓 {int(getattr(hold_del, 'rowcount', 0) or 0)} 行、"
                f"列表快照 {n_m} 条。"
            )
            print("删除后库内末净值：")
            _print_nav_tail(db, sid_filter, td_expr)
            return 0

        if on_date_raw:
            try:
                target_d = date.fromisoformat(on_date_raw[:10])
            except ValueError:
                print(f"错误：--on-date 格式无效 {on_date_raw!r}，应为 YYYY-MM-DD")
                return 1
            target_cmp = _date_to_cmp(target_d)
            print(f"=== 模式：仅删除 trade_date = {target_d.isoformat()} ===\n")
            nav_cnt = int(
                db.execute(
                    text(
                        f"""
                        SELECT COUNT(*) FROM strategy_nav_daily
                        WHERE strategy_id IN ({in_clause}) AND {td_expr} = :dc
                        """
                    ),
                    {**sid_binds, "dc": target_cmp},
                ).scalar()
                or 0
            )
            hold_cnt = int(
                db.execute(
                    text(
                        f"""
                        SELECT COUNT(*) FROM strategy_holding_daily
                        WHERE strategy_id IN ({in_clause}) AND {td_expr} = :dc
                        """
                    ),
                    {**sid_binds, "dc": target_cmp},
                ).scalar()
                or 0
            )
            print(f"将删除 strategy_nav_daily: {nav_cnt} 行")
            print(f"将删除 strategy_holding_daily: {hold_cnt} 行")
            if not args.apply:
                print("\n预览模式，未删除。加 --apply 执行。")
                return 0
            db.execute(
                text(
                    f"""
                    DELETE FROM strategy_nav_daily
                    WHERE strategy_id IN ({in_clause}) AND {td_expr} = :dc
                    """
                ),
                {**sid_binds, "dc": target_cmp},
            )
            db.execute(
                text(
                    f"""
                    DELETE FROM strategy_holding_daily
                    WHERE strategy_id IN ({in_clause}) AND {td_expr} = :dc
                    """
                ),
                {**sid_binds, "dc": target_cmp},
            )
            db.commit()
            n_m = _purge_list_metrics(db, sid_filter, do_commit=True)
            print(f"\n已删除；清理列表快照 {n_m} 条。")
            _print_nav_tail(db, sid_filter, td_expr)
            return 0

        # --days N：自库内 MAX(trade_date) 向前 N 个自然日
        mx = db.execute(
            text(
                f"""
                SELECT MAX({td_expr}) AS mx
                FROM strategy_nav_daily
                WHERE strategy_id IN ({in_clause})
                """
            ),
            sid_binds,
        ).mappings().first()
        mx_hold = db.execute(
            text(
                f"""
                SELECT MAX({td_expr}) AS mx
                FROM strategy_holding_daily
                WHERE strategy_id IN ({in_clause})
                """
            ),
            sid_binds,
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
        print(f"北京今日: {beijing_today().isoformat()}")
        print(f"删除区间（含起止）: {cutoff_d.isoformat()} ~ {latest_d.isoformat()}  共 {days} 个自然日")
        print(f"策略范围: {', '.join(sid_filter)}")

        nav_cnt = int(
            db.execute(
                text(
                    f"""
                    SELECT COUNT(*) FROM strategy_nav_daily
                    WHERE strategy_id IN ({in_clause}) AND {td_expr} >= :cutoff
                    """
                ),
                {**sid_binds, "cutoff": cutoff_cmp},
            ).scalar()
            or 0
        )
        hold_cnt = int(
            db.execute(
                text(
                    f"""
                    SELECT COUNT(*) FROM strategy_holding_daily
                    WHERE strategy_id IN ({in_clause}) AND {td_expr} >= :cutoff
                    """
                ),
                {**sid_binds, "cutoff": cutoff_cmp},
            ).scalar()
            or 0
        )
        print(f"将删除 strategy_nav_daily: {nav_cnt} 行")
        print(f"将删除 strategy_holding_daily: {hold_cnt} 行")

        for sid in sid_filter:
            dates = db.execute(
                text(
                    f"""
                    SELECT DISTINCT trade_date, {td_expr} AS dcmp
                    FROM strategy_nav_daily
                    WHERE strategy_id=:sid AND {td_expr} >= :cutoff
                    ORDER BY dcmp
                    """
                ),
                {"sid": sid, "cutoff": cutoff_cmp},
            ).fetchall()
            if dates:
                print(f"\n{sid} 将删净值交易日 ({len(dates)} 个):")
                print("  " + ", ".join(str(r[0]) for r in dates))

        if not args.apply:
            print("\n预览模式，未删除。确认后加 --apply。")
            print("提示：若只想删「最近一次更新」那一日，请用 --latest-only。")
            return 0

        db.execute(
            text(
                f"""
                DELETE FROM strategy_nav_daily
                WHERE strategy_id IN ({in_clause}) AND {td_expr} >= :cutoff
                """
            ),
            {**sid_binds, "cutoff": cutoff_cmp},
        )
        db.execute(
            text(
                f"""
                DELETE FROM strategy_holding_daily
                WHERE strategy_id IN ({in_clause}) AND {td_expr} >= :cutoff
                """
            ),
            {**sid_binds, "cutoff": cutoff_cmp},
        )
        db.commit()
        n_m = _purge_list_metrics(db, sid_filter, do_commit=True)
        print(f"\n已提交删除；清理列表快照 {n_m} 条。")
        print("删除后库内末净值：")
        _print_nav_tail(db, sid_filter, td_expr)
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

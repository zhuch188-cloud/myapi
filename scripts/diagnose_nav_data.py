#!/usr/bin/env python3
"""检查 Turso/SQLite 中持仓与净值日期格式、调仓期顺序，辅助排查迁库后净值异常。"""

from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text

from app.db import get_session
from app.sql_dialect import sql_date_compact_expr, sql_max_date_expr, sql_order_date_desc


def _sample_formats(rows: list, col: str, n: int = 3) -> list[str]:
    out: list[str] = []
    for r in rows:
        v = r.get(col)
        if v is None:
            continue
        s = str(v)
        if s not in out:
            out.append(s)
        if len(out) >= n:
            break
    return out


def main() -> int:
    with get_session() as db:
        strategies = [
            str(r[0]).strip()
            for r in db.execute(
                text("SELECT strategy_id FROM strategy_configs ORDER BY strategy_id")
            ).fetchall()
            if str(r[0] or "").strip()
        ]
        if not strategies:
            print("无 strategy_configs 记录。")
            return 0

        print("=== 调仓日：字典序 MAX vs 日历序 MAX（strategy_positions）===\n")
        for sid in strategies:
            row = db.execute(
                text(
                    f"""
                    SELECT
                      MAX(rebalance_date) AS lex_max,
                      {sql_max_date_expr("rebalance_date")} AS cal_max,
                      COUNT(*) AS cnt,
                      COUNT(DISTINCT rebalance_date) AS periods
                    FROM strategy_positions
                    WHERE strategy_id=:sid
                    """
                ),
                {"sid": sid},
            ).mappings().first()
            if not row or int(row["cnt"] or 0) == 0:
                print(f"  {sid}: 无持仓")
                continue
            lex_m = row.get("lex_max")
            cal_m = row.get("cal_max")
            warn = "  *** 不一致，增量导入/最新期可能错误 ***" if str(lex_m) != str(cal_m) else ""
            print(
                f"  {sid}: 行数={row['cnt']} 调仓期={row['periods']} "
                f"MAX(原文)={lex_m} MAX(日历)={cal_m}{warn}"
            )
            samples = db.execute(
                text(
                    """
                    SELECT rebalance_date FROM strategy_positions
                    WHERE strategy_id=:sid
                    ORDER BY rebalance_date DESC
                    LIMIT 5
                    """
                ),
                {"sid": sid},
            ).mappings().all()
            cal_order = db.execute(
                text(
                    f"""
                    SELECT rebalance_date FROM strategy_positions
                    WHERE strategy_id=:sid
                    ORDER BY {sql_order_date_desc("rebalance_date")}
                    LIMIT 5
                    """
                ),
                {"sid": sid},
            ).mappings().all()
            if [r["rebalance_date"] for r in samples] != [r["rebalance_date"] for r in cal_order]:
                print(f"    字典序 Top5: {_sample_formats(samples, 'rebalance_date', 5)}")
                print(f"    日历序 Top5: {_sample_formats(cal_order, 'rebalance_date', 5)}")

        print("\n=== 净值表 strategy_nav_daily ===\n")
        for sid in strategies:
            nav = db.execute(
                text(
                    f"""
                    SELECT
                      COUNT(*) AS n,
                      MIN({sql_date_compact_expr("trade_date")}) AS d0,
                      MAX({sql_date_compact_expr("trade_date")}) AS d1,
                      MIN(nav_unit) AS nav_min,
                      MAX(nav_unit) AS nav_max
                    FROM strategy_nav_daily
                    WHERE strategy_id=:sid
                    """
                ),
                {"sid": sid},
            ).mappings().first()
            if not nav or int(nav["n"] or 0) == 0:
                print(f"  {sid}: 无净值（需执行「全量重算净值」）")
                continue
            print(
                f"  {sid}: 交易日={nav['n']} 区间 {nav['d0']}..{nav['d1']} "
                f"nav_unit [{nav['nav_min']}, {nav['nav_max']}]"
            )

        print("\n=== 日期格式抽样（含 '-' 与纯数字混用则须全量重导+重算）===\n")
        for table, col in (
            ("strategy_positions", "rebalance_date"),
            ("strategy_nav_daily", "trade_date"),
        ):
            rows = db.execute(
                text(f"SELECT DISTINCT {col} AS d FROM {table} ORDER BY d DESC LIMIT 200")
            ).mappings().all()
            iso_n = 0
            compact_n = 0
            other: list[str] = []
            for r in rows:
                s = str(r["d"] or "").strip()
                if len(s) >= 10 and s[4:5] == "-":
                    iso_n += 1
                elif s.replace("-", "").isdigit() and len(s.replace("-", "")[:8]) == 8:
                    compact_n += 1
                elif len(other) < 3:
                    other.append(s)
            print(f"  {table}.{col}: ISO样例≈{iso_n} 纯数字样例≈{compact_n} 其它={other or '无'}")

    print(
        "\n建议：若 lex_max≠cal_max 或净值明显异常，请在管理端对该策略 "
        "「全量导入持仓」→「全量重算净值(full)」，勿仅依赖从 MySQL 复制的 strategy_nav_daily。"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

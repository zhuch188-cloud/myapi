#!/usr/bin/env python3
"""对照 CL1 Excel 与 Turso strategy_positions（与 app.services 相同解析规则）。"""
from __future__ import annotations

import os
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

EXCEL_DEFAULT = Path(r"D:\展示策略\展示策略\展示策略-沪深300指数增强.xlsx")
SID = "CL1"


def excel_period_counts(file_path: Path) -> tuple[dict[date, int], int]:
    from app.services import _iter_strategy_holdings_excel_batches

    counts: dict[date, int] = defaultdict(int)
    total = 0
    for chunk in _iter_strategy_holdings_excel_batches(str(file_path)):
        for rb, _code, _hw, _iw in chunk:
            counts[rb] += 1
            total += 1
        chunk.clear()
    return dict(counts), total


def db_period_counts() -> tuple[dict[date, int], int, date | None, date | None]:
    from sqlalchemy import text
    from app.db import SessionLocalFactory, init_database
    from app.services import _row_sql_date

    init_database()
    db = SessionLocalFactory()
    try:
        rows = db.execute(
            text(
                """
                SELECT rebalance_date, COUNT(*) AS cnt
                FROM strategy_positions
                WHERE strategy_id=:sid
                GROUP BY rebalance_date
                ORDER BY rebalance_date
                """
            ),
            {"sid": SID},
        ).mappings().all()
        counts: dict[date, int] = {}
        total = 0
        min_rb = max_rb = None
        for r in rows:
            rb = _row_sql_date(r["rebalance_date"])
            if rb is None:
                continue
            c = int(r["cnt"] or 0)
            counts[rb] = c
            total += c
            min_rb = rb if min_rb is None else min(min_rb, rb)
            max_rb = rb if max_rb is None else max(max_rb, rb)
        return counts, total, min_rb, max_rb
    finally:
        db.close()


def cumulative_at_date(period_counts: dict[date, int], through: date) -> int:
    return sum(c for rb, c in period_counts.items() if rb <= through)


def main() -> None:
    excel_path = Path(sys.argv[1] if len(sys.argv) > 1 else EXCEL_DEFAULT)
    if not excel_path.is_file():
        print(f"文件不存在: {excel_path}")
        sys.exit(1)

    print(f"Excel: {excel_path}")
    ex_counts, ex_total = excel_period_counts(excel_path)
    ex_periods = len(ex_counts)
    ex_min = min(ex_counts)
    ex_max = max(ex_counts)

    db_counts, db_total, db_min, db_max = db_period_counts()

    print("\n=== 汇总 ===")
    print(f"Excel: {ex_total} 行, {ex_periods} 个调仓日, {ex_min} ~ {ex_max}")
    print(f"Turso: {db_total} 行, {len(db_counts)} 个调仓日, {db_min} ~ {db_max}")

    if db_max:
        ex_cum_at_db_max = cumulative_at_date(ex_counts, db_max)
        print(
            f"\n若库内最后日期 {db_max} 正确，累计行数应约 {ex_cum_at_db_max}；"
            f"实际库内 {db_total}，差 {ex_cum_at_db_max - db_total}"
        )

    mismatches: list[tuple[date, int, int]] = []
    all_dates = sorted(set(ex_counts) | set(db_counts))
    for rb in all_dates:
        ec = ex_counts.get(rb, 0)
        dc = db_counts.get(rb, 0)
        if ec != dc:
            mismatches.append((rb, ec, dc))

    print(f"\n=== 调仓日不一致（共 {len(mismatches)} 期）===")
    for rb, ec, dc in mismatches[:15]:
        print(f"  {rb}: Excel={ec} DB={dc} Δ={dc - ec}")
    if len(mismatches) > 15:
        print(f"  … 另有 {len(mismatches) - 15} 期")

    if mismatches:
        first_bad = mismatches[0][0]
        print(f"\n建议续传/重导起点（首期不一致）: {first_bad}")
        print(
            f"  该日 Excel 累计至当日: {cumulative_at_date(ex_counts, first_bad)} 行"
        )

    if db_max and db_max in ex_counts:
        print(f"\n库内最后一期 {db_max}: Excel={ex_counts[db_max]} DB={db_counts.get(db_max, 0)}")

    ex_only = sorted(set(ex_counts) - set(db_counts))
    db_only = sorted(set(db_counts) - set(ex_counts))
    if ex_only:
        print(f"\nExcel 有但库内无的调仓日（前5）: {ex_only[:5]} … 共 {len(ex_only)}")
    if db_only:
        print(f"库内有但 Excel 无的调仓日: {db_only[:5]}")


if __name__ == "__main__":
    main()

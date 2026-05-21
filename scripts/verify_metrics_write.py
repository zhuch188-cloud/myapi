#!/usr/bin/env python3
"""验证 strategy_list_metrics 写入口径（需 .env 中 TURSO_*）。"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.db import SessionLocalFactory, init_database
from app.nav_list_metrics_calc import compute_strategy_list_metrics_snapshot
from app.sql_dialect import coerce_bind_value
from app.strategy_list_metrics import refresh_strategy_list_metrics_one

SID = (sys.argv[1] if len(sys.argv) > 1 else "cs66").strip().lower()


def main() -> int:
    assert coerce_bind_value("20260501") == "20260501", "compact bind must not become ISO"
    assert coerce_bind_value("2026-05-01") == "2026-05-01"
    init_database()
    db = SessionLocalFactory()
    try:
        snap = compute_strategy_list_metrics_snapshot(db, SID)
        print(f"compute {SID}:", snap)
        refresh_strategy_list_metrics_one(db, SID, do_commit=True)
        row = db.execute(
            __import__("sqlalchemy").text(
                "SELECT last_5d_return, month_return, year_return "
                "FROM strategy_list_metrics WHERE strategy_id=:sid"
            ),
            {"sid": SID},
        ).mappings().first()
        print(f"table {SID}:", dict(row) if row else None)
        m, y, d5 = snap.get("month_return"), snap.get("year_return"), snap.get("last_5d_return")
        if m is not None and y is not None and abs(m - y) < 1e-9:
            print("FAIL: month==year in compute")
            return 1
        if row and d5 is not None and abs(float(row["last_5d_return"]) - d5) > 1e-6:
            print("FAIL: table 5d != compute")
            return 1
        print("OK")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())

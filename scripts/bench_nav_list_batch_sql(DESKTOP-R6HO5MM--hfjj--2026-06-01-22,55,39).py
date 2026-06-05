"""
策略列表汇总耗时自测（与 main._batch_strategy_nav_list_summaries 当前实现一致）:
  - 一条 SQL：strategy_nav_daily 含 rebalance_date，WHERE IN，无 ORDER BY
  - 应用层按 strategy_id 分桶、按 trade_date 降序排序并算 max_rb

同一连接内连续跑两次相同 SELECT，便于区分「冷读 / 缓冲池已热」。

用法（在项目根目录）:
  python scripts/bench_nav_list_batch_sql.py
"""
from __future__ import annotations

import sys
import time
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from app.config import settings
from app.db import create_app_engine


def _quoted(ids: list[str]) -> str:
    return ",".join("'" + s.replace("'", "''") + "'" for s in ids)


def _as_date(td) -> date:
    if isinstance(td, datetime):
        return td.date()
    if isinstance(td, date):
        return td
    return date.fromisoformat(str(td)[:10])


def _nav_sql(quoted: str) -> str:
    return f"""
                SELECT strategy_id, trade_date, nav_unit, daily_ret, rebalance_date
                FROM strategy_nav_daily
                WHERE strategy_id IN ({quoted})
                """


def _run_nav_and_python(db: Session, quoted: str, ids: list[str]) -> tuple[float, float, int]:
    t0 = time.perf_counter()
    nav = db.execute(text(_nav_sql(quoted))).mappings().all()
    ms_sql = (time.perf_counter() - t0) * 1000.0

    t1 = time.perf_counter()
    by_sid: defaultdict[str, list] = defaultdict(list)
    max_rb_by: dict[str, date | None] = {}
    for row in nav:
        sid = str(row["strategy_id"]).strip()
        by_sid[sid].append(row)
        rd = row.get("rebalance_date")
        if rd is not None:
            d = _as_date(rd)
            cur = max_rb_by.get(sid)
            if cur is None or d > cur:
                max_rb_by[sid] = d
    for sid in ids:
        lst = by_sid.get(sid, [])
        if lst:
            lst.sort(key=lambda r: _as_date(r["trade_date"]), reverse=True)
    ms_py = (time.perf_counter() - t1) * 1000.0
    return ms_sql, ms_py, len(nav)


def main() -> None:
    engine = create_app_engine()
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    db: Session = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT strategy_id
                FROM strategy_configs
                WHERE is_visible=1 AND status='enabled'
                ORDER BY updated_at DESC
                """
            )
        ).mappings().all()
        ids = [str(r["strategy_id"]).strip() for r in rows if r.get("strategy_id")]
        if not ids:
            print("无可见策略，退出。")
            return
        quoted = _quoted(ids)
        print(f"可见策略数: {len(ids)}")
        print(f"库: {settings.db_name} @ {settings.db_host}:{settings.db_port}\n")

        ms1_sql, ms1_py, n1 = _run_nav_and_python(db, quoted, ids)
        ms2_sql, ms2_py, n2 = _run_nav_and_python(db, quoted, ids)

        print("  --- 第 1 次（多为冷读或部分在池内） ---")
        print(f"  nav SELECT              {ms1_sql:8.1f} ms  rows={n1}")
        print(f"  python bucket+sort      {ms1_py:8.1f} ms")
        print(f"  TOTAL                   {ms1_sql + ms1_py:8.1f} ms")

        print("\n  --- 第 2 次（数据页多在 InnoDB buffer pool） ---")
        print(f"  nav SELECT              {ms2_sql:8.1f} ms  rows={n2}")
        print(f"  python bucket+sort      {ms2_py:8.1f} ms")
        print(f"  TOTAL                   {ms2_sql + ms2_py:8.1f} ms")

        if ms1_sql > 0 and ms2_sql > 0:
            print(f"\n  第2次 SQL / 第1次 SQL = {ms2_sql / ms1_sql:.2f}")

        cnt = db.execute(
            text(f"SELECT COUNT(*) AS c FROM strategy_nav_daily WHERE strategy_id IN ({quoted})")
        ).mappings().first()
        nav_rows = int(cnt["c"] or 0) if cnt else 0
        print(f"\nstrategy_nav_daily 涉及行数(合计): {nav_rows}")
        print(
            "\n说明: 总耗时主要由「读回约 5.5 万行」决定，与是否 ORDER BY 关系往往不大；"
            "第 2 次明显更快说明瓶颈在缓冲池/磁盘而非 Python。"
        )
        print("      老库缺 strategy_positions 前缀索引时，应用启动会尝试补 idx_pos_date（与本次 nav 查询无关）。")
    finally:
        db.close()


if __name__ == "__main__":
    main()

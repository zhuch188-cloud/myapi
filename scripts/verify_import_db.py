"""一次性核对：strategy_configs / 磁盘文件 / strategy_positions / Excel 内调仓期数。"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pandas as pd
from sqlalchemy import create_engine, text

from app.config import settings
from app.db import DB_URL


def main() -> None:
    root = Path(settings.strategy_root_dir)
    print("STRATEGY_ROOT_DIR:", root, "| exists:", root.is_dir())

    engine = create_engine(DB_URL, pool_pre_ping=True)
    with engine.connect() as c:
        cfgs = c.execute(
            text(
                """
                SELECT strategy_id, strategy_name, file_dir, file_name, status, weight_display_mode
                FROM strategy_configs ORDER BY strategy_id
                """
            )
        ).mappings().all()
        print("\n--- strategy_configs ---")
        for r in cfgs:
            print(dict(r))

        pos = c.execute(
            text(
                """
                SELECT strategy_id, COUNT(*) AS n, COUNT(DISTINCT rebalance_date) AS periods,
                       MIN(rebalance_date) AS dmin, MAX(rebalance_date) AS dmax
                FROM strategy_positions GROUP BY strategy_id
                """
            )
        ).mappings().all()
        print("\n--- strategy_positions ---")
        for r in pos:
            print(dict(r))

        daily = c.execute(
            text(
                """
                SELECT strategy_id, COUNT(*) AS n, COUNT(DISTINCT rebalance_date) AS periods,
                       COUNT(DISTINCT trade_date) AS snap_days
                FROM strategy_holding_daily GROUP BY strategy_id
                """
            )
        ).mappings().all()
        print("\n--- strategy_holding_daily ---")
        for r in daily:
            print(dict(r))

        print("\n--- positions vs holding_daily（调仓期数应对齐；仅最新 trade_date 快照）---")
        pos_map = {r["strategy_id"]: r for r in pos}
        daily_map = {r["strategy_id"]: r for r in daily}
        for sid in sorted(set(pos_map) | set(daily_map)):
            pp = int(pos_map.get(sid, {}).get("periods") or 0)
            hp = int(daily_map.get(sid, {}).get("periods") or 0)
            ok = "OK" if pp == hp and pp > 0 else ("MISSING_HOLDING" if hp == 0 and pp > 0 else "MISMATCH")
            print(f"  {sid}: positions_periods={pp} holding_periods={hp} -> {ok}")

    print("\n--- xlsx under root (recursive) ---")
    files = sorted(root.rglob("*.xlsx"))
    for p in files:
        print(p)

    print("\n--- per enabled config: file exists + Excel distinct 调整日期 ---")
    for r in cfgs:
        if r["status"] != "enabled":
            continue
        fp = root / (r["file_dir"] or "") / r["file_name"]
        ok = fp.is_file()
        print(f"\n  strategy_id={r['strategy_id']} file={fp.name} exists={ok}")
        if not ok:
            # 尝试在根目录下模糊匹配
            matches = [x for x in files if x.name == r["file_name"]]
            if matches:
                print(f"    hint: same file_name found at {matches[0]}")
            continue
        df = pd.read_excel(fp, sheet_name=0)
        need = ("调整日期", "证券代码")
        miss = [x for x in need if x not in df.columns]
        if miss:
            print(f"    ERROR missing columns {miss}; actual columns={list(df.columns)}")
            continue
        dt = pd.to_datetime(df["调整日期"], errors="coerce").ffill().bfill()
        dlist = sorted(dt.dropna().dt.date.unique().tolist())
        print(f"    rows={len(df)} distinct_调整日期={len(dlist)} head_dates={dlist[:5]}")
        # 模拟导入行级校验（不写库）
        from app.services import normalize_code

        err = None
        try:
            d2 = df.assign(_rebalance_dt=dt)
            d2 = d2.loc[d2["_rebalance_dt"].notna() & d2["证券代码"].notna()]
            if d2.empty:
                err = "no valid rows after dropna"
            else:
                for idx, row in d2.iterrows():
                    try:
                        normalize_code(row["证券代码"])
                    except Exception as e:
                        err = f"row_index={idx} err={e!r}"
                        break
        except Exception as e:
            err = repr(e)
        print(f"    row_parse_ok={err is None} {err or ''}")


def simulate_rebalance_queries() -> None:
    engine = create_engine(DB_URL, pool_pre_ping=True)
    with engine.connect() as c:
        rows = c.execute(
            text(
                """
                SELECT DISTINCT rebalance_date AS d
                FROM strategy_positions
                WHERE strategy_id=:sid
                ORDER BY rebalance_date DESC
                """
            ),
            {"sid": "auto_001"},
        ).mappings().all()
        print("\n--- simulate run_update rebalance_rows ---")
        for rb in rows:
            d = rb["d"]
            print("  distinct row type=", type(d), "value=", repr(d))
            n = c.execute(
                text(
                    """
                    SELECT COUNT(*) AS c FROM strategy_positions
                    WHERE strategy_id=:sid AND rebalance_date=:rd
                    """
                ),
                {"sid": "auto_001", "rd": d},
            ).scalar_one()
            print("    positions count for this rd:", n)


def holding_detail() -> None:
    engine = create_engine(DB_URL, pool_pre_ping=True)
    with engine.connect() as c:
        r = c.execute(
            text(
                """
                SELECT rebalance_date, COUNT(*) AS n FROM strategy_holding_daily
                WHERE strategy_id=:sid GROUP BY rebalance_date
                """
            ),
            {"sid": "auto_001"},
        ).fetchall()
        print("holding_daily by rebalance_date (auto_001):", r)
        r2 = c.execute(
            text(
                """
                SELECT INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME FROM information_schema.statistics
                WHERE table_schema = DATABASE() AND table_name = 'strategy_holding_daily'
                  AND INDEX_NAME = 'uk_daily' ORDER BY SEQ_IN_INDEX
                """
            )
        ).fetchall()
        print("uk_daily index columns:", r2)
        r3 = c.execute(
            text(
                "SELECT id, status, LEFT(message,200) FROM strategy_update_jobs ORDER BY id DESC LIMIT 5"
            )
        ).fetchall()
        print("last update jobs:", r3)
        r4 = c.execute(
            text(
                "SELECT strategy_id, COUNT(*) FROM strategy_positions GROUP BY strategy_id"
            )
        ).fetchall()
        print("positions rows per strategy:", r4)


if __name__ == "__main__":
    main()
    print()
    holding_detail()
    simulate_rebalance_queries()

"""
Turso / libSQL 导入相关性能探测：延迟、批量 UPSERT、ALTER 列。

用法（项目根目录）:
  python scripts/bench_turso_import.py
  python scripts/bench_turso_import.py --file "D:/path/公司资料.xlsx"
"""
from __future__ import annotations

import argparse
import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pandas as pd
from sqlalchemy import text

from app.config import settings
from app.db import create_app_engine


def _pct(xs: list[float], p: float) -> float:
    if not xs:
        return 0.0
    xs = sorted(xs)
    i = min(len(xs) - 1, max(0, int(len(xs) * p)))
    return xs[i]


def _bench(name: str, fn, repeat: int = 5) -> dict:
    samples: list[float] = []
    err = None
    for _ in range(repeat):
        t0 = time.perf_counter()
        try:
            fn()
        except Exception as e:
            err = str(e)
            break
        samples.append(time.perf_counter() - t0)
    return {
        "name": name,
        "ok": err is None,
        "error": err,
        "n": len(samples),
        "mean_ms": statistics.mean(samples) * 1000 if samples else None,
        "p50_ms": _pct(samples, 0.5) * 1000 if samples else None,
        "p95_ms": _pct(samples, 0.95) * 1000 if samples else None,
    }


def _probe_file(path: Path) -> dict:
    if not path.is_file():
        return {"path": str(path), "exists": False}
    suf = path.suffix.lower()
    t0 = time.perf_counter()
    if suf == ".csv":
        df = pd.read_csv(path, dtype=object, encoding="utf-8-sig", nrows=None)
    else:
        df = pd.read_excel(path, sheet_name=0, dtype=object)
    read_s = time.perf_counter() - t0
    return {
        "path": str(path),
        "exists": True,
        "size_mb": path.stat().st_size / (1024 * 1024),
        "rows": len(df),
        "cols": len(df.columns),
        "read_sec": round(read_s, 2),
    }


def _estimate_import_sec(
    *,
    rows: int,
    data_cols: int,
    batch_size: int,
    batch_roundtrip_ms: float,
    alter_cols: int,
    alter_ms: float,
    read_sec: float,
) -> dict:
    batches = max(1, (rows + batch_size - 1) // batch_size)
    # 每批：executemany + commit + 更新批次进度（约 2 次往返，保守按 2x RTT）
    db_sec = batches * (batch_roundtrip_ms / 1000.0) * 2.0
    db_sec += alter_cols * (alter_ms / 1000.0)
    low = read_sec + db_sec * 0.7
    high = read_sec + db_sec * 1.5
    return {
        "rows": rows,
        "batch_size": batch_size,
        "batches": batches,
        "alter_cols_assumed": alter_cols,
        "read_sec": round(read_sec, 1),
        "db_sec_est": round(db_sec, 1),
        "total_sec_low": round(low, 0),
        "total_sec_high": round(high, 0),
        "rows_per_sec_est": round(rows / max(db_sec + read_sec, 0.1), 1),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", help="补充数据 Excel/CSV，用于统计行数")
    ap.add_argument("--batch-size", type=int, default=settings.supplement_import_batch_size)
    ap.add_argument("--repeat", type=int, default=5)
    args = ap.parse_args()

    url = (settings.turso_database_url or "").strip()
    replica = (settings.turso_local_replica or "").strip()
    mode = "local_replica+sync" if replica else "remote_only"
    print("=== Turso 导入性能探测 ===")
    print(f"连接模式: {mode}")
    print(f"URL: {url[:48]}..." if len(url) > 48 else f"URL: {url or '(未配置)'}")
    print(f"批次大小 SUPPLEMENT_IMPORT_BATCH_SIZE: {args.batch_size}")
    print()

    if not url:
        print("错误: 未配置 TURSO_DATABASE_URL，请在 .env 中填写后重试。")
        sys.exit(1)

    file_info: dict | None = None
    if args.file:
        file_info = _probe_file(Path(args.file))
    else:
        default = Path(settings.strategy_root_dir) / "数据" / "公司资料.xlsx"
        if (settings.supplement_company_excel_path or "").strip():
            default = Path(settings.supplement_company_excel_path.strip())
        if default.is_file():
            file_info = _probe_file(default)

    if file_info:
        print("--- 本地文件 ---")
        for k, v in file_info.items():
            print(f"  {k}: {v}")
        print()

    engine = create_app_engine()
    results: list[dict] = []

    def q1(c):
        c.execute(text("SELECT 1")).fetchone()

    def pragma_profiles(c):
        c.execute(text("PRAGMA table_info(supplement_company_profiles)")).fetchall()

    def single_upsert(c):
        c.execute(
            text(
                """
                INSERT INTO supplement_company_profiles
                (definition_code, stock_code, last_batch_id)
                VALUES ('__bench__', :sc, 0)
                ON CONFLICT(definition_code, stock_code) DO UPDATE SET
                  last_batch_id=excluded.last_batch_id
                """
            ),
            {"sc": f"bench_{int(time.time() * 1000) % 1000000}"},
        )
        c.commit()

    batch_n = min(args.batch_size, 200)
    upsert_sql = f"""
        INSERT INTO supplement_company_profiles
        (definition_code, stock_code, last_batch_id)
        VALUES ('__bench__', :sc, 0)
        ON CONFLICT(definition_code, stock_code) DO UPDATE SET
          last_batch_id=excluded.last_batch_id
    """

    def batch_upsert(c):
        params = [{"sc": f"b{i}_{int(time.time()) % 10000}"} for i in range(batch_n)]
        c.execute(text(upsert_sql), params)
        c.commit()

    with engine.connect() as conn:
        for name, fn in [
            ("SELECT 1（往返延迟）", lambda: q1(conn)),
            ("PRAGMA table_info（读表结构）", lambda: pragma_profiles(conn)),
            ("单行 UPSERT + commit", lambda: single_upsert(conn)),
            (f"批量 UPSERT {batch_n} 行 + commit（≈导入一批）", lambda: batch_upsert(conn)),
        ]:
            r = _bench(name, fn, repeat=args.repeat)
            results.append(r)
            if r["ok"]:
                print(
                    f"{r['name']}: "
                    f"mean={r['mean_ms']:.0f}ms p50={r['p50_ms']:.0f}ms p95={r['p95_ms']:.0f}ms"
                )
            else:
                print(f"{r['name']}: 失败 — {r['error']}")

        # 清理探测行
        try:
            conn.execute(
                text(
                    "DELETE FROM supplement_company_profiles WHERE definition_code='__bench__'"
                )
            )
            conn.commit()
        except Exception:
            pass

    print()
    batch_r = next((x for x in results if "批量 UPSERT" in x["name"]), None)
    single_r = next((x for x in results if x["name"] == "单行 UPSERT + commit"), None)
    pragma_r = next((x for x in results if "PRAGMA" in x["name"]), None)

    if not batch_r or not batch_r["ok"]:
        print("无法估算：批量写入探测未成功。")
        sys.exit(2)

    batch_ms = float(batch_r["mean_ms"] or 0)
    rows = int((file_info or {}).get("rows") or 10000)
    cols = int((file_info or {}).get("cols") or 30)
    read_sec = float((file_info or {}).get("read_sec") or 3.0)
    data_cols = max(0, cols - 2)
    # 首次导入若大量新列，每列一次 ALTER（远程时很慢）
    alter_ms = float((pragma_r or {}).get("mean_ms") or batch_ms) * 3

    est = _estimate_import_sec(
        rows=rows,
        data_cols=data_cols,
        batch_size=args.batch_size,
        batch_roundtrip_ms=batch_ms,
        alter_cols=min(data_cols, 80),
        alter_ms=alter_ms,
        read_sec=read_sec,
    )

    print("--- 合理耗时估算（基于实测批量写入 + 保守系数）---")
    for k, v in est.items():
        print(f"  {k}: {v}")

    if single_r and single_r["ok"]:
        per_row_remote = float(single_r["mean_ms"]) / 1000.0
        print()
        print(
            f"  若误用逐行写入（未批量）: 约 {rows * per_row_remote / 60:.0f}～"
            f"{rows * per_row_remote * 1.2 / 60:.0f} 分钟（{rows} 行 × {per_row_remote*1000:.0f}ms/行）"
        )

    print()
    print("--- 结论参考 ---")
    if batch_ms > 3000:
        print("  批量一次 >3s：纯远程 Turso，4MB 文件导入数分钟～数十分钟属常见；强烈建议配置 TURSO_LOCAL_REPLICA。")
    elif batch_ms > 800:
        print("  批量一次 0.8～3s：跨区远程正常；万行级约 1～5 分钟可接受。")
    else:
        print("  批量一次 <0.8s：库较快；若导入仍很慢，优先查是否 QUEUED 未启动或 ALTER 列过多。")

    if mode == "remote_only":
        print("  当前为纯远程：建议 .env 增加 TURSO_LOCAL_REPLICA=C:/Users/你/.turso/app.db（纯英文路径）。")


if __name__ == "__main__":
    main()

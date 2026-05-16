"""本机连接 Wind SQL Server，拉取 AShareIncome 样例行（与个股页同一逻辑），用于核对 8115 等错误是否消失。"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main() -> int:
    from app import wind_sql
    from app.wind_holders import fetch_top10_holders
    from app.wind_income import _fetch_raw_rows, build_income_series_for_stock

    try:
        wind_sql.init_wind_backend()
    except Exception as e:
        print("init_wind_backend 失败:", e)
        return 1
    if not wind_sql.use_remote_sqlserver():
        print("未配置 WIND_SQLSERVER_SERVER 等，跳过。")
        return 1

    code = (sys.argv[1] if len(sys.argv) > 1 else "603629.SH").strip()
    with wind_sql.get_wind_engine().connect() as conn:
        try:
            rows = _fetch_raw_rows(conn, code)
        except Exception as e:
            print("_fetch_raw_rows 失败:", e)
            return 2
        print("raw rows:", len(rows))
        if rows:
            print("first row (string columns):", json.dumps(rows[0], ensure_ascii=False, default=str))
        try:
            s = build_income_series_for_stock(conn, code)
        except Exception as e:
            print("build_income_series_for_stock 失败:", e)
            return 3
        print("income_series.error:", s.get("error"))
        if not s.get("error"):
            print("q1 cumulative len:", len(s.get("q1", {}).get("cumulative", [])))
        h = fetch_top10_holders(conn, code)
        print("top10_holders.error:", h.get("error"))
        print("top10_holders as_of_end_dt:", h.get("as_of_end_dt"), "items:", len(h.get("items") or []))
        if h.get("items"):
            print("top10 first:", json.dumps(h["items"][0], ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

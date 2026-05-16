"""
个股页近一年走势：与 run_update / rebuild_nav_series 共用 wind_bulk 拉数（S_DQ_ADJCLOSE 后复权收盘为主）。

- 个股：dbo.AShareEODPrices，S_DQ_ADJCLOSE（后复权收盘，缺失时回退 S_DQ_CLOSE）+ S_DQ_CLOSE；
- 指数：dbo.AIndexEODPrices 多数库无复权列，仅用 S_DQ_CLOSE 作涨跌（raw/adj 同源）；
  区间结束日为 wind_sql.sql_max_trade_dt()。

走势曲线：每个交易日先按「复权收盘 ×（最新日不复权收盘 / 最新日复权收盘）」折算到与当前真实价可比，
再在交集交易日上相对窗口首日归一化为 100。净值侧日收益用复权收盘链，与 sql_quote 中展示用的不复权现价独立。
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import text

from app import wind_bulk, wind_sql
from app.services import normalize_code


def wind_benchmark_for_stock(stock_wind: str) -> tuple[str, str]:
    """
    返回 (指数 Wind 代码, 中文名称)。
    规则：688→科创综指；002→中小板综；6→上证；3→创业板；其余 0 开头→深证成指；其他默认上证。
    """
    s = (stock_wind or "").strip().upper()
    digits = s.split(".", 1)[0] if "." in s else s
    digits = "".join(digits.split())
    if digits.startswith("688"):
        return ("000680.SH", "科创综指")
    if digits.startswith("002"):
        return ("399101.SZ", "中小板综")
    if digits.startswith("6"):
        return ("000001.SH", "上证综指")
    if digits.startswith("3"):
        return ("399006.SZ", "创业板指")
    if digits.startswith("0"):
        return ("399001.SZ", "深证成指")
    return ("000001.SH", "上证综指")


def _compact_to_date(c: str) -> datetime:
    x = (c or "").strip()[:8]
    return datetime.strptime(x, "%Y%m%d")


def _adj_raw_maps_from_quads(
    quads: list[wind_bulk.WindEodQuad],
) -> tuple[dict[str, float], dict[str, float]]:
    adj: dict[str, float] = {}
    raw: dict[str, float] = {}
    for d, a, _p, r in quads:
        dc = wind_bulk._dt_compact(d)
        if len(dc) < 8:
            continue
        if a == a and float(a) > 0:
            adj[dc] = float(a)
        if r == r and float(r) > 0:
            raw[dc] = float(r)
    return adj, raw


def _latest_raw_adj_scale(adj: dict[str, float], raw: dict[str, float], dates: list[str]) -> float:
    """在 dates 中从后往前找首个同时有复权、不复权收盘的日，返回 raw/adj；找不到则 1.0。"""
    for d in reversed(dates):
        a, r = adj.get(d), raw.get(d)
        if a is not None and r is not None and a > 0 and r > 0:
            return r / a
    return 1.0


def _merge_scaled_norm_series(
    stock_quads: list[wind_bulk.WindEodQuad],
    index_quads: list[wind_bulk.WindEodQuad],
    end_compact: str,
    calendar_days: int = 370,
) -> tuple[list[str], list[float], list[float]]:
    """
    交集交易日（仅要求复权价齐全）；截断为 end 前约一年；
    各点 price = 当日复权收盘 ×（锚定日不复权收盘 / 锚定日复权收盘），再相对首日归一化到 100。
    """
    end_d = _compact_to_date(end_compact).date()
    cut = end_d - timedelta(days=calendar_days)

    s_adj, s_raw = _adj_raw_maps_from_quads(stock_quads)
    i_adj, i_raw = _adj_raw_maps_from_quads(index_quads)

    dates = sorted(set(s_adj) & set(i_adj))
    dates = [d for d in dates if _compact_to_date(d).date() >= cut]
    if len(dates) < 2:
        return [], [], []

    ks = _latest_raw_adj_scale(s_adj, s_raw, dates)
    ki = _latest_raw_adj_scale(i_adj, i_raw, dates)

    scaled_s = [s_adj[d] * ks for d in dates]
    scaled_i = [i_adj[d] * ki for d in dates]
    if scaled_s[0] <= 0 or scaled_i[0] <= 0:
        return [], [], []
    s0, i0 = scaled_s[0], scaled_i[0]
    stock_norm = [100.0 * x / s0 for x in scaled_s]
    index_norm = [100.0 * x / i0 for x in scaled_i]
    return dates, stock_norm, index_norm


def compute_stock_index_year_trend(wind: Any, stock_code_raw: str) -> dict[str, Any]:
    """
    近一年个股与对标指数：复权收盘拉数 + 按最新日「不复权/复权」比折算后再归一化绘图。
    """
    try:
        stock_wind = normalize_code(stock_code_raw)
    except Exception as e:
        return {"error": f"代码无效: {e}"}

    bench_code, bench_name = wind_benchmark_for_stock(stock_wind)
    max_row = wind.execute(text(wind_sql.sql_max_trade_dt())).mappings().first()
    if not max_row or max_row.get("d") is None:
        return {"error": "Wind 无可用交易日"}

    end_compact = wind_bulk._dt_compact(max_row["d"])
    if len(end_compact) < 8:
        return {"error": "Wind 最新交易日格式异常"}

    end_d = _compact_to_date(end_compact).date()
    start_compact = (end_d - timedelta(days=420)).strftime("%Y%m%d")

    wind, eod_stock = wind_bulk.load_eod_by_code(wind, [stock_wind], start_compact, end_compact, None)
    wind, eod_idx = wind_bulk.load_index_eod_by_code(wind, [bench_code], start_compact, end_compact, None)

    sk = wind_bulk._wk(stock_wind)
    bk = wind_bulk._wk(bench_code)
    s_series = eod_stock.get(sk, [])
    i_series = eod_idx.get(bk, [])

    dates, sn, inn = _merge_scaled_norm_series(s_series, i_series, end_compact, calendar_days=370)
    if not dates:
        return {
            "error": "近一年无重叠行情（请检查 Wind 代码或指数是否存在）",
            "benchmark_code": bench_code,
            "benchmark_name": bench_name,
            "stock_windcode": stock_wind,
        }

    return {
        "benchmark_code": bench_code,
        "benchmark_name": bench_name,
        "stock_windcode": stock_wind,
        "dates": dates,
        "stock_norm": [round(x, 4) for x in sn],
        "index_norm": [round(x, 4) for x in inn],
    }

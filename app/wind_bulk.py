"""Wind 批量读：按策略合并股票与日期区间，减少往返；增量落库可后续扩展。"""
from __future__ import annotations

import logging
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any

from sqlalchemy import text

from app import wind_sql
from app.config import settings

_log = logging.getLogger(__name__)

# 单次 IN 的股票数；过大时 SQL Server 易在拉取大量日 K 行时断连(10054/SQLGetData)
EOD_STOCK_CHUNK = 80


def eod_stock_chunk_size() -> int:
    n = int(getattr(settings, "wind_eod_stock_chunk", 0) or 0)
    if n > 0:
        return max(4, min(n, EOD_STOCK_CHUNK))
    if bool(getattr(settings, "wind_low_memory_mode", True)):
        return 8
    return EOD_STOCK_CHUNK
_EOD_WIND_MAX_ATTEMPTS = 5

# 单根 K：(TRADE_DT compact, 后复权收盘 S_DQ_ADJCLOSE, 上一交易日后复权收盘作昨收, 不复权收盘 S_DQ_CLOSE)
WindEodQuad = tuple[str, float, float, float]


def _wk(code: object) -> str:
    return str(code).strip().upper()


def _dt_compact(d: Any) -> str:
    """统一为 YYYYMMDD 字符串，便于与调仓日 compact 比较。"""
    if d is None:
        return ""
    if isinstance(d, datetime):
        return d.strftime("%Y%m%d")
    if isinstance(d, date):
        return d.strftime("%Y%m%d")
    s = str(d).strip().replace("-", "")
    if len(s) >= 8 and s[:8].isdigit():
        return s[:8]
    try:
        return datetime.fromisoformat(str(d)[:10]).strftime("%Y%m%d")
    except ValueError:
        return s[:8].ljust(8, "0")[:8]


def _sql_float(v: Any) -> float:
    if v is None:
        return float("nan")
    try:
        return float(v)
    except (TypeError, ValueError):
        return float("nan")


def _sorted_triples_to_adj_quads(rows: list[tuple[str, float, float]]) -> list[WindEodQuad]:
    """
    rows: (d, adj_cl, raw_cl) 已按 d 升序。
    返回 (d, adj_close, prev_adj_close, raw_close)；日涨跌用 adj / prev_adj - 1。
    """
    out: list[WindEodQuad] = []
    prev_adj: float | None = None
    for d, adj, raw in rows:
        adjv = _sql_float(adj)
        rawv = _sql_float(raw)
        if adjv != adjv or adjv <= 0:
            pc = float("nan")
            out.append((d, adjv, pc, rawv))
            continue
        pc = float("nan")
        if prev_adj is not None and prev_adj > 0:
            pc = prev_adj
        out.append((d, adjv, pc, rawv))
        prev_adj = adjv
    return out


def _wind_transient_disconnect(exc: BaseException) -> bool:
    s = str(exc).lower()
    if "10054" in s or "08s01" in s:
        return True
    if "通讯链接失败" in str(exc) or "远程主机强迫关闭" in str(exc):
        return True
    return "connection" in s and ("forcibly closed" in s or "broken pipe" in s)


def eod_range_segments(
    st_compact: str,
    td_compact: str,
    *,
    step_months: int | None = None,
) -> list[tuple[str, str]]:
    """低内存净值重建：step_months>0 时按月分段；否则读配置；再否则按自然年。"""
    st = str(st_compact).strip()[:8]
    td = str(td_compact).strip()[:8]
    months = int(step_months) if step_months is not None else 0
    if months <= 0:
        months = int(getattr(settings, "nav_rebuild_eod_months", 0) or 0)
    if months > 0 and bool(getattr(settings, "wind_low_memory_mode", True)):
        return month_compact_segments(st, td, step_months=months)
    if months > 0:
        return month_compact_segments(st, td, step_months=months)
    return _year_compact_segments(st, td)


def _year_compact_segments(st_compact: str, td_compact: str) -> list[tuple[str, str]]:
    """将 [st,td] 按自然年拆成多段，减小单次 EOD 结果集，降低 Wind SQL Server 断连概率。"""
    st = str(st_compact).strip()[:8]
    td = str(td_compact).strip()[:8]
    if len(st) < 8 or len(td) < 8:
        return [(st, td)]
    if st > td:
        return []
    y0, y1 = int(st[:4]), int(td[:4])
    out: list[tuple[str, str]] = []
    for y in range(y0, y1 + 1):
        y4 = f"{y:04d}"
        seg_st = max(st, f"{y4}0101")
        seg_ed = min(td, f"{y4}1231")
        if seg_st <= seg_ed:
            out.append((seg_st, seg_ed))
    return out if out else [(st, td)]


def month_compact_segments(
    st_compact: str, td_compact: str, *, step_months: int = 3
) -> list[tuple[str, str]]:
    """将 [st,td] 按 N 个自然月拆段，用于净值重建降低 day_map 峰值（Render 低内存）。"""
    st = str(st_compact).strip()[:8]
    td = str(td_compact).strip()[:8]
    step = max(1, int(step_months))
    if len(st) < 8 or len(td) < 8:
        return [(st, td)]
    if st > td:
        return []
    cur = datetime.strptime(st, "%Y%m%d").date()
    end = datetime.strptime(td, "%Y%m%d").date()
    out: list[tuple[str, str]] = []
    while cur <= end:
        y, m = cur.year, cur.month
        em = m + step - 1
        ey = y
        while em > 12:
            em -= 12
            ey += 1
        if em == 12:
            seg_end_d = date(ey, 12, 31)
        else:
            seg_end_d = date(ey, em + 1, 1) - timedelta(days=1)
        seg_end_d = min(seg_end_d, end)
        out.append((cur.strftime("%Y%m%d"), seg_end_d.strftime("%Y%m%d")))
        cur = seg_end_d + timedelta(days=1)
    return out if out else [(st, td)]


def bulk_eod_start_compact(trade_date: date | str | Any, min_rebalance_date: date | str | Any) -> str:
    """全量/首建：EOD 起点取调仓日、上年与约 420 自然日回溯中最早者（覆盖 YTD 与长周期指标）。"""
    td_c = _dt_compact(trade_date)
    rb_c = _dt_compact(min_rebalance_date)
    if len(td_c) < 8 or len(rb_c) < 8:
        raise ValueError("invalid trade_date or min_rebalance_date for EOD range")
    td_d = datetime.strptime(td_c[:8], "%Y%m%d").date()
    y_prev = f"{td_d.year - 1}0101"
    lb = (td_d - timedelta(days=420)).strftime("%Y%m%d")
    return min(y_prev, rb_c, lb)


def holding_eod_lookback_calendar_days() -> int:
    """日常增量持仓：自行情日向前日历天数（覆盖 ret_60 约 60 个交易日 + 缓冲）。"""
    n = int(getattr(settings, "holding_eod_lookback_calendar_days", 0) or 0)
    return max(75, min(n, 200)) if n > 0 else 110


def holding_eod_desc_max_bars() -> int:
    """从 EOD 序列取近 N 根交易日算 5/20/60 日涨跌（须 ≤ 实际拉取长度）。"""
    n = int(getattr(settings, "holding_eod_desc_max_bars", 0) or 0)
    return max(65, min(n, 280)) if n > 0 else 65


def holding_eod_start_for_period(
    trade_date: date | str | Any,
    rebalance_date: date | str | Any,
    *,
    full_refresh: bool,
) -> str:
    """
    持仓单期 EOD 起点：全量用 bulk_eod_start_compact；
    增量时短周期指标用约 N 日回溯，但「本期收益」须含调仓日 → 起点不晚于调仓日。
    """
    if full_refresh:
        return bulk_eod_start_compact(trade_date, rebalance_date)
    rb_c = _dt_compact(rebalance_date)
    inc = holding_eod_start_incremental(trade_date, rebalance_date)
    if len(rb_c) >= 8 and len(inc) >= 8 and rb_c < inc:
        return rb_c
    return inc


def holding_eod_start_incremental(
    trade_date: date | str | Any, rebalance_date: date | str | Any
) -> str:
    """
    日常增量持仓 EOD 起点（取**最早**必要日，以覆盖三类指标）。

    - 5/20/60 日：约 N 自然日回溯 lb；
    - 今年以来：须含当年 1/1 之前最近收盘 → 至少从 y_prev（上年 1/1）起；
    - 本期收益：须含调仓日（期中更新时 rb 可能晚于 lookback_floor）。

    期中：max(rb, lookback_floor)，老调仓期不会从 2016 拉至今；
    调仓日=行情日：仅 lookback_floor，避免 rb 把起点顶到当天只剩 1 根 K。
    """
    td_c = _dt_compact(trade_date)
    rb_c = _dt_compact(rebalance_date)
    if len(td_c) < 8 or len(rb_c) < 8:
        raise ValueError("invalid trade_date or rebalance_date for incremental EOD")
    td_d = datetime.strptime(td_c[:8], "%Y%m%d").date()
    y_prev = f"{td_d.year - 1}0101"
    lb = (td_d - timedelta(days=holding_eod_lookback_calendar_days())).strftime("%Y%m%d")
    lookback_floor = min(lb, y_prev)
    if td_c[:8] == rb_c[:8]:
        return lookback_floor
    return max(rb_c, lookback_floor)


def nav_incremental_eod_start(
    append_after_c: str,
    rb_sorted: list[date],
    last_nav_d: date,
    latest_d: date,
    trade_days: list[str],
) -> str:
    """净值增量 EOD：默认末净值日；若其间有新调仓，起点前移到该调仓首个交易日。"""
    start = str(append_after_c).strip()[:8]
    if len(start) < 8:
        return start
    for rb in rb_sorted:
        if rb <= last_nav_d:
            continue
        if rb > latest_d:
            break
        p0 = first_trade_on_or_after(rb, trade_days)
        if p0 and p0 < start:
            start = p0
    return start


def first_trade_on_or_after(rb: date, trade_days: list[str]) -> str | None:
    """trade_days 升序 compact；首个 >= 调仓日的交易日。"""
    rb_c = _dt_compact(rb)
    for td in trade_days:
        if td >= rb_c:
            return td
    return None


def load_eod_by_code(
    wind: Any,
    codes: list[str],
    start_compact: str,
    td_compact: str,
    db: Any = None,
) -> tuple[Any, dict[str, list[WindEodQuad]]]:
    """
    Wind 个股日 K：wind_sql.sql_bulk_eod_range → dbo.AShareEODPrices。
    按「自然年 + 股票分批」多次查询合并，避免单次结果集过大触发 10054(SQLGetData)；
    若传入 db 且遇瞬断，会 close 后 open_wind 重试。
    返回 (wind_connection, wind_code -> [(d, adj, prev_adj, raw), ...] 升序)；调用方须用返回的 wind 继续后续查询。
    """
    out: dict[str, list[tuple[str, float, float]]] = defaultdict(list)
    if not codes:
        return wind, {}
    td_s = str(td_compact).strip()
    st_s = str(start_compact).strip()
    segs = eod_range_segments(st_s, td_s)
    if not segs:
        segs = [(st_s, td_s)]
    w = wind
    for seg_st, seg_ed in segs:
        chunk_sz = eod_stock_chunk_size()
        for i in range(0, len(codes), chunk_sz):
            part = codes[i : i + chunk_sz]
            quoted = ",".join("'" + str(c).replace("'", "''") + "'" for c in part)
            for attempt in range(_EOD_WIND_MAX_ATTEMPTS):
                try:
                    rows = w.execute(
                        text(wind_sql.sql_bulk_eod_range(quoted)),
                        {"st": seg_st, "td": seg_ed},
                    ).mappings().all()
                    for r in rows:
                        c = _wk(r["c"])
                        dc = _dt_compact(r["d"])
                        adjv = _sql_float(r.get("adj_cl"))
                        rawv = _sql_float(r.get("raw_cl"))
                        out[c].append((dc, adjv, rawv))
                    break
                except Exception as ex:
                    if attempt >= _EOD_WIND_MAX_ATTEMPTS - 1 or not _wind_transient_disconnect(ex) or db is None:
                        raise
                    _log.warning(
                        "Wind load_eod_by_code 重试 seg=%s~%s codes=%s..%s attempt=%s: %s",
                        seg_st,
                        seg_ed,
                        i,
                        min(i + chunk_sz, len(codes)),
                        attempt + 1,
                        ex,
                    )
                    time.sleep(0.45 * (2**attempt))
                    try:
                        w.close()
                    except Exception:
                        pass
                    w = wind_sql.open_wind(db)
    quad_out: dict[str, list[WindEodQuad]] = {}
    for c, triples in out.items():
        triples.sort(key=lambda x: x[0])
        quad_out[c] = _sorted_triples_to_adj_quads(triples)
    return w, quad_out


def _fill_missing_preclose_from_prior_close(
    series_asc: list[tuple[str, float, float]],
) -> list[tuple[str, float, float]]:
    """指数/股票日序列（三元组 close,preclose）：昨收为空时用上一根收盘。新行情链路已用四元组+复权链，本函数仅保留兼容。"""
    out: list[tuple[str, float, float]] = []
    prev_cl: float | None = None
    for d, cl, pc in series_asc:
        clv = None if (isinstance(cl, float) and cl != cl) else (float(cl) if cl is not None else None)
        pcv = None if (isinstance(pc, float) and pc != pc) else (float(pc) if pc is not None else None)
        if pcv is None and prev_cl is not None and prev_cl > 0:
            pcv = prev_cl
        out.append(
            (
                d,
                float("nan") if clv is None else clv,
                float("nan") if pcv is None else pcv,
            )
        )
        if clv is not None and clv > 0:
            prev_cl = clv
    return out


def load_index_eod_by_code(
    wind: Any,
    codes: list[str],
    start_compact: str,
    td_compact: str,
    db: Any = None,
) -> tuple[Any, dict[str, list[WindEodQuad]]]:
    """
    Wind 指数日 K：wind_sql.sql_bulk_index_eod_range → dbo.AIndexEODPrices；
    与 load_eod_by_code 相同：按年 + 分批 + 可选重连。
    返回 (wind, code -> 升序四元组序列)。
    """
    out: dict[str, list[tuple[str, float, float]]] = defaultdict(list)
    if not codes:
        return wind, {}
    td_s = str(td_compact).strip()
    st_s = str(start_compact).strip()
    segs = eod_range_segments(st_s, td_s)
    if not segs:
        segs = [(st_s, td_s)]
    w = wind
    for seg_st, seg_ed in segs:
        chunk_sz = eod_stock_chunk_size()
        for i in range(0, len(codes), chunk_sz):
            part = codes[i : i + chunk_sz]
            quoted = ",".join("'" + str(c).replace("'", "''") + "'" for c in part)
            for attempt in range(_EOD_WIND_MAX_ATTEMPTS):
                try:
                    rows = w.execute(
                        text(wind_sql.sql_bulk_index_eod_range(quoted)),
                        {"st": seg_st, "td": seg_ed},
                    ).mappings().all()
                    for r in rows:
                        c = _wk(r["c"])
                        dc = _dt_compact(r["d"])
                        adjv = _sql_float(r.get("adj_cl"))
                        rawv = _sql_float(r.get("raw_cl"))
                        out[c].append((dc, adjv, rawv))
                    break
                except Exception as ex:
                    if attempt >= _EOD_WIND_MAX_ATTEMPTS - 1 or not _wind_transient_disconnect(ex) or db is None:
                        raise
                    _log.warning(
                        "Wind load_index_eod_by_code 重试 seg=%s~%s attempt=%s: %s",
                        seg_st,
                        seg_ed,
                        attempt + 1,
                        ex,
                    )
                    time.sleep(0.45 * (2**attempt))
                    try:
                        w.close()
                    except Exception:
                        pass
                    w = wind_sql.open_wind(db)
    quad_out: dict[str, list[WindEodQuad]] = {}
    for c, triples in out.items():
        triples.sort(key=lambda x: x[0])
        quad_out[c] = _sorted_triples_to_adj_quads(triples)
    return w, quad_out


def fetch_trade_date_compacts(
    wind: Any, db: Any, start_compact: str, end_compact: str
) -> tuple[Any, list[str]]:
    """交易日列表：按年分段查询并合并，降低单次结果集过大导致断连。"""
    st0 = str(start_compact).strip()[:8]
    ed0 = str(end_compact).strip()[:8]
    segs = _year_compact_segments(st0, ed0)
    if not segs:
        segs = [(st0, ed0)]
    combined: list[str] = []
    w = wind
    for seg_st, seg_ed in segs:
        for attempt in range(_EOD_WIND_MAX_ATTEMPTS):
            try:
                rows = w.execute(
                    text(wind_sql.sql_trade_calendar_range()),
                    {"st": seg_st, "ed": seg_ed},
                ).mappings().all()
                combined.extend(_dt_compact(r["d"]) for r in rows)
                break
            except Exception as ex:
                if attempt >= _EOD_WIND_MAX_ATTEMPTS - 1 or not _wind_transient_disconnect(ex) or db is None:
                    raise
                _log.warning(
                    "Wind fetch_trade_date_compacts 重试 seg=%s~%s attempt=%s: %s",
                    seg_st,
                    seg_ed,
                    attempt + 1,
                    ex,
                )
                time.sleep(0.45 * (2**attempt))
                try:
                    w.close()
                except Exception:
                    pass
                w = wind_sql.open_wind(db)
    seen: set[str] = set()
    out: list[str] = []
    for d in combined:
        if d and d not in seen:
            seen.add(d)
            out.append(d)
    out.sort()
    return w, out


def day_return_adj_for_asof(series_asc: list[WindEodQuad], asof_compact: str) -> float | None:
    """升序四元组序列中，最后一个 TRADE_DT <= asof 的交易日的复权日收益 adj/prev_adj-1。"""
    fc = str(asof_compact).replace("-", "")[:8].zfill(8)
    last: WindEodQuad | None = None
    for row in series_asc:
        if row[0] > fc:
            break
        last = row
    if last is None:
        return None
    _, adj, prev_adj, _raw = last
    if isinstance(adj, float) and adj != adj:
        return None
    if isinstance(prev_adj, float) and prev_adj != prev_adj:
        return None
    if prev_adj is None or float(prev_adj) <= 0:
        return None
    if float(adj) <= 0:
        return None
    return float(adj) / float(prev_adj) - 1.0


def index_close_preclose_for_compact_day(
    series_asc: list[WindEodQuad], day_compact: str
) -> tuple[float | None, float | None]:
    """在升序序列中取 TRADE_DT==day_compact 的复权收盘与「昨收」（上一交易日复权收盘）。"""
    fc = str(day_compact).replace("-", "")[:8].zfill(8)
    prev_adj: float | None = None
    for d, cl, pc, _raw in series_asc:
        dc = _dt_compact(d)
        clv = None if (isinstance(cl, float) and cl != cl) else float(cl)
        pcv = None if (isinstance(pc, float) and pc != pc) else float(pc)
        if pcv is None and prev_adj is not None and prev_adj > 0:
            pcv = prev_adj
        if dc == fc:
            return clv, pcv
        if clv is not None and clv > 0:
            prev_adj = clv
    return None, None


def closes_desc_from_asc(series_asc: list[WindEodQuad], max_n: int = 280) -> list[float]:
    """按交易日从近到远排列的有效复权收盘价。"""
    tail = series_asc[-max_n:] if len(series_asc) > max_n else series_asc
    out: list[float] = []
    for _, cl, _, _ in reversed(tail):
        if isinstance(cl, float) and cl != cl:
            continue
        out.append(float(cl))
    return out


def close_n_trading_days_ago(desc_closes_newest_first: list[float], n: int) -> float | None:
    """N 取正整数：相对最新收盘的第 N 个交易日前的收盘（10 日涨跌 = 最新 / 第 N 根前收盘 - 1）。"""
    if n <= 0 or len(desc_closes_newest_first) <= n:
        return None
    v = float(desc_closes_newest_first[n])
    if v <= 0:
        return None
    return v


def last_close_on_or_before(
    series_asc: list[WindEodQuad], end_compact: str
) -> float | None:
    """升序序列中，TRADE_DT <= end_compact 的最后一个有效复权收盘价（含调仓/段末日当日）。"""
    ec = str(end_compact).replace("-", "")[:8].zfill(8)
    last: float | None = None
    for d, cl, _, _ in series_asc:
        if _dt_compact(d) > ec:
            continue
        if isinstance(cl, float) and cl != cl:
            continue
        v = float(cl)
        if v <= 0:
            continue
        last = v
    return last


def series_on_or_before(
    series_asc: list[WindEodQuad], end_compact: str
) -> list[WindEodQuad]:
    """保留 TRADE_DT <= end_compact 的 K 线（持有段内序列，含段末日）。"""
    ec = str(end_compact).replace("-", "")[:8].zfill(8)
    return [row for row in series_asc if _dt_compact(row[0]) <= ec]


def last_close_before_calendar_date(
    series_asc: list[WindEodQuad], cutoff_compact: str
) -> float | None:
    """升序序列中，TRADE_DT 严格早于 cutoff_compact 的最后一个有效复权收盘价。"""
    fc = str(cutoff_compact).replace("-", "")[:8].zfill(8)
    last: float | None = None
    for d, cl, _, _ in series_asc:
        if _dt_compact(d) >= fc:
            continue
        if isinstance(cl, float) and cl != cl:
            continue
        v = float(cl)
        if v <= 0:
            continue
        last = v
    return last


def first_close_on_or_after(series_asc: list[WindEodQuad], from_compact: str) -> float | None:
    """首个交易日 >= from_compact 且复权收盘价存在。"""
    fc = str(from_compact).replace("-", "")[:8]
    if len(fc) < 8:
        fc = fc.zfill(8)
    for d, cl, _, _ in series_asc:
        if _dt_compact(d) < fc:
            continue
        if isinstance(cl, float) and cl != cl:
            continue
        v = float(cl)
        if v <= 0:
            continue
        return v
    return None

"""
Wind 利润表（dbo.AShareIncome）一次拉取、应用侧算累计/单季与同比。
列名与 STATEMENT_TYPE 以 Wind 数据字典为准；若查询报错，请改本模块顶部常量。
"""
from __future__ import annotations

import logging
import re
from datetime import date
from decimal import Decimal
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from app import wind_sql
from app.config import settings

logger = logging.getLogger(__name__)

# —— 与 Wind 库不一致时只改这里 ——
INCOME_TABLE = "ashareincome"
# 合并报表相关 STATEMENT_TYPE。多数 Wind 库为 9 位（如 408004000），字典亦见 12 位（408004000000）；
# 查询时会自动把每种配置扩展为「短码+×1000 长码」一并 IN，避免对不上。
STATEMENT_TYPES_MERGE: tuple[str, ...] = (
    "408001000",  # 合并报表
    "408003000",  # 合并报表（调整前）
    "408004000",  # 合并报表（调整后）
)
COL_WINDCODE = "S_INFO_WINDCODE"
COL_REPORT_PERIOD = "REPORT_PERIOD"
COL_ANN_DT = "ANN_DT"
# 营业收入：优先 OPER_REV，缺失时 Wind 常存 TOT_OPER_REV（营业总收入）
COL_OPER_REV = "OPER_REV"
COL_TOT_OPER_REV = "TOT_OPER_REV"
COL_STMT = "STATEMENT_TYPE"
# 归母净利润：不同 Wind 版本列名不同；优先读 .env WIND_INCOME_NET_PROFIT_COLUMN，否则 INFORMATION_SCHEMA 按序探测
NET_PROFIT_COLUMN_CANDIDATES: tuple[str, ...] = (
    "NET_PROFIT_EXCL_MIN_INT_INC",
    "NET_PROFIT_EXCL_MIN_INT_INC_",
    "N_INCOME_ATTR_P",
    "NET_PROFIT_ATTR_P",
    "NET_PROFIT_ATTR_PARENT",
)

_cached_net_profit_column: str | None = None


def _quote_ident_sql(name: str) -> str:
    """仅用于 Wind 固定白名单列名，防注入。"""
    if not name.replace("_", "").isalnum():
        raise ValueError(f"invalid SQL identifier: {name!r}")
    return f"[{name}]"


def _detect_net_profit_column(conn: Connection) -> str:
    """在 dbo.AShareIncome 上按 NET_PROFIT_COLUMN_CANDIDATES 顺序匹配实际列名。"""
    t = INCOME_TABLE.replace("'", "''")
    in_list = ",".join("'" + c.replace("'", "''") + "'" for c in NET_PROFIT_COLUMN_CANDIDATES)
    sql = f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = N'dbo'
          AND UPPER(TABLE_NAME) = UPPER(N'{t}')
          AND COLUMN_NAME IN ({in_list})
        """
    rows = [str(r[0]) for r in conn.execute(text(sql))]
    if not rows:
        raise RuntimeError(
            "AShareIncome 中未找到归母净利润候选列（"
            + ", ".join(NET_PROFIT_COLUMN_CANDIDATES)
            + "）。请在 .env 设置 WIND_INCOME_NET_PROFIT_COLUMN 为库中实际列名。"
        )
    upper_map = {x.upper(): x for x in rows}
    for cand in NET_PROFIT_COLUMN_CANDIDATES:
        if cand.upper() in upper_map:
            return upper_map[cand.upper()]
    return rows[0]


def _get_net_profit_column(conn: Connection) -> str:
    global _cached_net_profit_column
    if _cached_net_profit_column:
        return _cached_net_profit_column
    manual = (settings.wind_income_net_profit_column or "").strip()
    if manual:
        _cached_net_profit_column = manual
        return _cached_net_profit_column
    _cached_net_profit_column = _detect_net_profit_column(conn)
    logger.info("Wind AShareIncome 归母净利润列使用: %s", _cached_net_profit_column)
    return _cached_net_profit_column

# 展示最近 N 个报告期（同类型）
DISPLAY_PERIODS = 5
# 向前多取年数（用于单季差分与同比）
YEARS_LOOKBACK = 8

REPORT_KEYS = ("q1", "interim", "q3", "annual")
REPORT_SUFFIX = {"q1": 331, "interim": 630, "q3": 930, "annual": 1231}


def _tbl(name: str) -> str:
    return wind_sql._tbl(name)


def _merge_statement_types() -> tuple[str, ...]:
    raw = (getattr(settings, "wind_income_statement_types", None) or "").strip()
    if not raw:
        return STATEMENT_TYPES_MERGE
    parts = tuple(x.strip() for x in raw.split(",") if x.strip())
    ok = tuple(p for p in parts if p.isdigit() and len(p) <= 20)
    return ok if ok else STATEMENT_TYPES_MERGE


def _statement_type_decimal_values() -> tuple[int, ...]:
    """
    将配置的 STATEMENT_TYPE 展开为库内可能出现的数值：12 位则同时加入 n//1000（9 位），
    9 位且末三位为 000 则同时加入 n*1000（12 位），与 Wind 两种存法对齐。
    """
    seen: set[int] = set()
    for st in _merge_statement_types():
        if not st.isdigit() or len(st) > 20:
            continue
        n = int(st)
        seen.add(n)
        ln = len(st)
        if ln >= 10:
            seen.add(n // 1000)
        elif ln <= 9 and n % 1000 == 0:
            seen.add(n * 1000)
    if not seen:
        for v in (408001000, 408003000, 408004000):
            seen.add(v)
            seen.add(v * 1000)
    return tuple(sorted(seen))


def _statement_type_sql_in_literals() -> str:
    """生成 IN (CAST(... AS DECIMAL(38,0)), ...) 片段，避免 ODBC 对超大整型参数绑定错误。"""
    vals = _statement_type_decimal_values()
    return ", ".join(f"CAST({v} AS DECIMAL(38,0))" for v in vals)


def _rp_to_int(rp: Any) -> int | None:
    if rp is None:
        return None
    if isinstance(rp, Decimal):
        try:
            if rp == rp.to_integral_value():
                s = str(int(rp))
            else:
                s = re.sub(r"[^\d]", "", str(rp))[:8]
        except Exception:
            s = re.sub(r"[^\d]", "", str(rp))[:8]
        if len(s) >= 8:
            s = s[:8]
        if len(s) < 8 or not s.isdigit():
            return None
        return int(s)
    if hasattr(rp, "strftime"):
        try:
            s = rp.strftime("%Y%m%d")
        except Exception:
            s = str(rp).strip().replace("-", "")[:8]
    else:
        s = str(rp).strip().replace("-", "").replace("/", "")
        if "." in s and s.replace(".", "").isdigit():
            try:
                s = str(int(float(s)))
            except (TypeError, ValueError):
                pass
        s = s[:8]
    if len(s) < 8 or not s.isdigit():
        return None
    return int(s)


def _ann_to_int(ann: Any) -> int:
    v = _rp_to_int(ann)
    return v if v is not None else 0


def _prev_year_same_rp(rp: int) -> int:
    return rp - 10000


def _parse_money(val: Any) -> float | None:
    """Wind 列可能为 nvarchar / 千分位 / 空标记；在应用侧解析，避免 SQL 端 8115。"""
    if val is None:
        return None
    s = str(val).strip()
    if not s or s.upper() in ("NONE", "NULL", "-", "--", "N/A", ".", "NA"):
        return None
    s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


def _fetch_raw_rows(conn: Connection, wind_code: str) -> list[dict[str, Any]]:
    t = _tbl(INCOME_TABLE)
    np_col = _quote_ident_sql(_get_net_profit_column(conn))
    min_y = date.today().year - YEARS_LOOKBACK
    min_rp_str = f"{min_y:04d}0101"
    stmt_in = _statement_type_sql_in_literals()
    # STATEMENT_TYPE：列多为 DECIMAL/FLOAT，与字符串 '408001000000' 比较会全不匹配；改为 DECIMAL 字面量 IN
    stmt_coalesce = (
        f"COALESCE("
        f"TRY_CAST(LTRIM(RTRIM(CONVERT(VARCHAR(100), t.{COL_STMT}))) AS DECIMAL(38,0)), "
        f"TRY_CAST(t.{COL_STMT} AS DECIMAL(38,0))"
        f")"
    )
    # REPORT_PERIOD：可能是 int / varchar / date，统一在子查询里算出 rp8 再筛
    rp8_case = f"""
        CASE
          WHEN TRY_CONVERT(int, t.{COL_REPORT_PERIOD}) IS NOT NULL
               AND TRY_CONVERT(int, t.{COL_REPORT_PERIOD}) BETWEEN 19900101 AND 21001231
            THEN CONVERT(VARCHAR(8), TRY_CONVERT(int, t.{COL_REPORT_PERIOD}))
          WHEN TRY_CONVERT(date, t.{COL_REPORT_PERIOD}) IS NOT NULL
            THEN CONVERT(VARCHAR(8), TRY_CONVERT(date, t.{COL_REPORT_PERIOD}), 112)
          ELSE LEFT(REPLACE(REPLACE(LTRIM(RTRIM(CONVERT(VARCHAR(32), t.{COL_REPORT_PERIOD}))), '-', ''), '/', ''), 8)
        END
    """
    params: dict[str, Any] = {"code": wind_code, "min_rp_str": min_rp_str}
    sql = f"""
        WITH x AS (
          SELECT
            t.{COL_REPORT_PERIOD} AS rp_raw,
            t.{COL_ANN_DT} AS ann_raw,
            t.{COL_OPER_REV} AS oper_rev_raw,
            t.{COL_TOT_OPER_REV} AS tot_oper_rev_raw,
            t.{np_col} AS ni_attr_raw,
            t.{COL_STMT} AS stmt_raw,
            {rp8_case} AS rp8
          FROM {t} t
          WHERE t.{COL_WINDCODE} = :code
            AND {stmt_coalesce} IN ({stmt_in})
        )
        SELECT
          x.rp_raw,
          x.ann_raw,
          x.oper_rev_raw,
          x.tot_oper_rev_raw,
          x.ni_attr_raw,
          x.stmt_raw
        FROM x
        WHERE LEN(x.rp8) = 8
          AND PATINDEX('%[^0-9]%', x.rp8) = 0
          AND x.rp8 >= :min_rp_str
        ORDER BY x.rp8 ASC, CONVERT(VARCHAR(32), x.ann_raw) ASC
        """
    rows = conn.execute(text(sql), params).mappings().all()
    return [dict(r) for r in rows]


def _diag_income_sample(conn: Connection, wind_code: str, limit: int = 12) -> str:
    """无数据时拉几条原始行，便于核对 STATEMENT_TYPE / REPORT_PERIOD 形态。"""
    t = _tbl(INCOME_TABLE)
    lim = max(1, min(int(limit), 30))
    sql = f"""
        SELECT TOP ({lim})
          CONVERT(VARCHAR(64), t.{COL_STMT}) AS st,
          CONVERT(VARCHAR(40), t.{COL_REPORT_PERIOD}) AS rp,
          CONVERT(VARCHAR(40), t.{COL_ANN_DT}) AS ann
        FROM {t} t
        WHERE t.{COL_WINDCODE} = :code
        ORDER BY t.{COL_ANN_DT} DESC
        """
    try:
        rows = conn.execute(text(sql), {"code": wind_code}).mappings().all()
    except Exception as e:
        return f"(诊断查询失败: {e})"
    if not rows:
        return "（该代码在 AShareIncome 中无任何行）"
    bits = []
    for r in rows:
        bits.append(f"st={r.get('st')!r} rp={r.get('rp')!r} ann={r.get('ann')!r}")
    return "; ".join(bits)


def _dedupe_latest_ann(rows: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    """同一报告期保留公告日最新的一条（重述后）。"""
    best: dict[int, dict[str, Any]] = {}
    for r in rows:
        rp = _rp_to_int(r.get("rp_raw"))
        if rp is None:
            continue
        ann = _ann_to_int(r.get("ann_raw"))
        rev = _parse_money(r.get("oper_rev_raw"))
        if rev is None:
            rev = _parse_money(r.get("tot_oper_rev_raw"))
        pfv = _parse_money(r.get("ni_attr_raw"))
        stmt = (r.get("stmt_raw") or r.get("stmt") or "")
        stmt = str(stmt).strip()
        prev = best.get(rp)
        if prev is None or ann >= prev["_ann"]:
            best[rp] = {
                "_ann": ann,
                "revenue": rev,
                "profit": pfv,
                "stmt": stmt,
            }
    for v in best.values():
        v.pop("_ann", None)
    return best


def _suffix_of(rp: int) -> int:
    return rp % 10000


def _periods_for_suffix(by_rp: dict[int, dict[str, Any]], suffix: int) -> list[int]:
    rps = sorted(rp for rp in by_rp if _suffix_of(rp) == suffix)
    return rps


def _single_quarter_values(
    by_rp: dict[int, dict[str, Any]], rp: int, suffix: int
) -> tuple[float | None, float | None] | None:
    """返回 (revenue_single, profit_single)；无法计算则 None。"""
    y = rp // 10000
    if suffix == 331:
        cur = by_rp.get(rp)
        if not cur:
            return None
        return (cur.get("revenue"), cur.get("profit"))
    if suffix == 630:
        q1 = by_rp.get(y * 10000 + 331)
        h1 = by_rp.get(rp)
        if not h1:
            return None
        if not q1:
            return (h1.get("revenue"), h1.get("profit"))
        tr = _diff(h1.get("revenue"), q1.get("revenue"))
        tp = _diff(h1.get("profit"), q1.get("profit"))
        return (tr, tp)
    if suffix == 930:
        h1 = by_rp.get(y * 10000 + 630)
        q3 = by_rp.get(rp)
        if not q3:
            return None
        if not h1:
            return (q3.get("revenue"), q3.get("profit"))
        return (_diff(q3.get("revenue"), h1.get("revenue")), _diff(q3.get("profit"), h1.get("profit")))
    if suffix == 1231:
        q3 = by_rp.get(y * 10000 + 930)
        fy = by_rp.get(rp)
        if not fy:
            return None
        if not q3:
            return (fy.get("revenue"), fy.get("profit"))
        return (_diff(fy.get("revenue"), q3.get("revenue")), _diff(fy.get("profit"), q3.get("profit")))
    return None


def _diff(a: float | None, b: float | None) -> float | None:
    if a is None or b is None:
        return None
    try:
        return float(a) - float(b)
    except (TypeError, ValueError):
        return None


def _yoy_ratio(cur: float | None, prev: float | None) -> float | None:
    if cur is None or prev is None:
        return None
    if prev == 0:
        return None
    return (float(cur) - float(prev)) / float(prev)


def _series_for_kind(
    by_rp: dict[int, dict[str, Any]],
    rkey: str,
    cumulative: bool,
) -> list[dict[str, Any]]:
    suffix = REPORT_SUFFIX[rkey]
    all_rps = _periods_for_suffix(by_rp, suffix)
    if len(all_rps) < DISPLAY_PERIODS:
        tail = all_rps
    else:
        tail = all_rps[-DISPLAY_PERIODS:]
    out: list[dict[str, Any]] = []
    for rp in tail:
        if cumulative:
            row = by_rp.get(rp)
            if not row:
                continue
            rev, prof = row.get("revenue"), row.get("profit")
        else:
            sq = _single_quarter_values(by_rp, rp, suffix)
            if sq is None:
                continue
            rev, prof = sq
        prp = _prev_year_same_rp(rp)
        if cumulative:
            prow = by_rp.get(prp)
            prev_rev = prow.get("revenue") if prow else None
            prev_prof = prow.get("profit") if prow else None
        else:
            psq = _single_quarter_values(by_rp, prp, suffix)
            if psq:
                prev_rev, prev_prof = psq
            else:
                prev_rev, prev_prof = None, None
        out.append(
            {
                "report_period": str(rp),
                "revenue": rev,
                "profit": prof,
                "revenue_yoy": _yoy_ratio(rev, prev_rev),
                "profit_yoy": _yoy_ratio(prof, prev_prof),
            }
        )
    return out


def build_income_series_for_stock(conn: Connection, wind_code: str) -> dict[str, Any]:
    """
    一次 Wind 查询，返回 4 报告期 ×（累计、单季）共 8 组序列。
    金额单位与 Wind 表一致（通常为「元」）；前端可自行换算为亿元。
    """
    code = (wind_code or "").strip()
    if not code:
        return {"error": "empty wind code"}

    try:
        raw = _fetch_raw_rows(conn, code)
    except Exception as e:
        logger.exception("Wind AShareIncome 查询失败: %s", code)
        return {"error": str(e)}

    by_rp = _dedupe_latest_ann(raw)
    if not by_rp:
        diag = _diag_income_sample(conn, code)
        hint = (
            "无利润表数据：主查询在合并 STATEMENT_TYPE 与报告期过滤后为空。"
            " 请用下方样例核对库中 STATEMENT_TYPE 是否与内置/ WIND_INCOME_STATEMENT_TYPES 一致；"
            " 或在 .env 设置 WIND_INCOME_STATEMENT_TYPES=库中实际值（逗号分隔）。 "
            f"样例: {diag}"
        )
        logger.warning("income empty for %s | %s", code, diag[:500])
        return {"error": hint}

    payload: dict[str, Any] = {"error": None, "currency_note": "金额单位与 Wind 表一致（一般为元）"}
    for rk in REPORT_KEYS:
        payload[rk] = {
            "cumulative": _series_for_kind(by_rp, rk, True),
            "single": _series_for_kind(by_rp, rk, False),
        }
    return payload


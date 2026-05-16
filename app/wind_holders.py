"""
Wind 中国A股前十大股东 dbo.AShareInsideHolder。
列名随 Wind 版本可能不同：首次查询时从 INFORMATION_SCHEMA 按候选列匹配；与《中国A股前十大股东》数据字典不一致时改 HOLDER_COLUMN_CANDIDATES。
"""
from __future__ import annotations

import logging
import re
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from app import wind_sql
from app.config import settings

logger = logging.getLogger(__name__)

HOLDER_TABLE = "ashareinsideholder"

# 不要求在库中出现的角色（缺省时 SQL/逻辑另有处理）。share_cat 在循环中单独跳过必填。
_OPTIONAL_ROLES = frozenset({"rank"})

# 角色 -> 候选列名（大写比较），按优先级从前到后
HOLDER_COLUMN_CANDIDATES: dict[str, tuple[str, ...]] = {
    "windcode": ("S_INFO_WINDCODE",),
    "end_dt": (
        "END_DT",
        "S_END_DT",
        "REPORT_PERIOD",
        "END_DATE",
        "S_FELLOW_END_DT",
        "S_INFO_ENDDATE",
    ),
    "ann_dt": ("ANN_DT", "S_ANN_DT", "ANN_DATE"),
    # 华安/部分 Wind 镜像列名与标准库不同，尽量多列；仍无时按持股数量推断排名
    "rank": (
        "S_HOLDER_RANK",
        "S_HOLDER_TOP10ORDER",
        "S_TOP10HOLDERORDER",
        "HOLDER_TOP10ORDER",
        "TOP10HOLDERORDER",
        "S_HOLD_RANK",
        "HOLD_RANK",
        "HOLDER_RANK",
        "S_RANK_ORDER",
        "S_DISPLAY_ORDER",
        "S_INFO_ORDER",
        "S_ORDER",
        "HOLDER_ORDER",
        "S_HOLDER_ORDER",
        "S_SEQUENCE",
        "SEQUENCE",
        "S_NO",
        "HOLDER_NO",
        "F_RANK",
        "RANK",
        "RNK",
        "S_RANK",
        "S_HOLDER_NUM",
    ),
    "name": ("S_HOLDER_NAME", "HOLDER_NAME", "S_HOLD_INST_NAME", "S_HOLD_INST"),
    "vol": ("S_HOLD_NUM", "S_HOLD_VOL", "HOLD_VOL", "S_HOLDER_QUANTITY", "HOLD_NUM", "S_HOLDER_HOLDNUM"),
    "pct": ("S_HOLD_RATIO", "HOLD_RATIO", "S_HOLDER_PCT", "S_HOLDER_RATIO", "PCT_OF_TOTAL"),
    "share_cat": ("S_SHARE_CATEGORY", "HOLDER_SHARECATEGORY", "S_HOLDER_CATEGORY", "SHARE_CATEGORY"),
}

_cached_cols: dict[str, str] | None = None


def _tbl(name: str) -> str:
    return wind_sql._tbl(name)


def _quote_ident(name: str) -> str:
    if not name.replace("_", "").isalnum():
        raise ValueError(f"invalid identifier: {name!r}")
    return f"[{name}]"


def _upper_columns(conn: Connection) -> dict[str, str]:
    """COLUMN_NAME -> 库中实际大小写。"""
    t = HOLDER_TABLE.replace("'", "''")
    sql = f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = N'dbo' AND UPPER(TABLE_NAME) = UPPER(N'{t}')
        """
    rows = conn.execute(text(sql)).fetchall()
    return {str(r[0]).upper(): str(r[0]) for r in rows}


def _resolve_holder_columns(conn: Connection) -> dict[str, str]:
    global _cached_cols
    if _cached_cols is not None:
        return _cached_cols
    upper = _upper_columns(conn)
    if not upper:
        raise RuntimeError(f"未找到表 dbo.{HOLDER_TABLE}（请确认 Wind 库中存在中国A股前十大股东表）")
    out: dict[str, str] = {}
    manual_rank = (getattr(settings, "wind_inside_holder_rank_column", None) or "").strip()
    if manual_rank:
        mr = manual_rank.upper()
        if mr in upper:
            out["rank"] = upper[mr]
        else:
            logger.warning(
                "WIND_INSIDE_HOLDER_RANK_COLUMN=%s 在 AShareInsideHolder 中不存在，将尝试自动候选或按持股数推断",
                manual_rank,
            )
    for role, cands in HOLDER_COLUMN_CANDIDATES.items():
        if role == "share_cat":
            for c in cands:
                if c.upper() in upper:
                    out[role] = upper[c.upper()]
                    break
            continue
        if role == "rank" and "rank" in out:
            continue
        for c in cands:
            if c.upper() in upper:
                out[role] = upper[c.upper()]
                break
        if role not in out and role not in _OPTIONAL_ROLES:
            raise RuntimeError(
                f"AShareInsideHolder 缺少字段角色「{role}」，候选: {cands}。"
                "请对照数据字典在 app/wind_holders.py 的 HOLDER_COLUMN_CANDIDATES 中增加实际列名。"
            )
    if "rank" not in out:
        logger.info("AShareInsideHolder 无排名列，将按持股数量降序推断前十大股东")
    _cached_cols = out
    logger.info("Wind AShareInsideHolder 列映射: %s", out)
    return out


def _parse_intish(v: Any) -> int | None:
    if v is None:
        return None
    if isinstance(v, int):
        return int(v)
    s = str(v).strip().replace("-", "").replace("/", "")[:8]
    if len(s) >= 8 and s[:8].isdigit():
        return int(s[:8])
    m = re.search(r"(\d{8})", str(v))
    if m:
        return int(m.group(1))
    try:
        x = int(float(str(v).replace(",", "")))
        if 19900101 <= x <= 21001231:
            return x
    except (TypeError, ValueError):
        pass
    return None


def _parse_floatish(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(str(v).strip().replace(",", ""))
    except (TypeError, ValueError):
        return None


def fetch_top10_holders(conn: Connection, wind_code: str) -> dict[str, Any]:
    """
    返回最近报告期（按截止日期/报告期字段取 max）下的前十大股东。
    items: rank, holder_name, hold_vol, hold_pct, share_category?, ann_dt?, end_dt?
    hold_pct 为 Wind 表中的原始比例（若库中为百分数如 12.34 表示 12.34%，前端自行展示）。
    """
    code = (wind_code or "").strip()
    if not code:
        return {"error": "empty wind code", "items": []}

    try:
        qc = _resolve_holder_columns(conn)
    except Exception as e:
        logger.exception("AShareInsideHolder 列解析失败")
        return {"error": str(e), "items": []}

    t = _tbl(HOLDER_TABLE)
    wc = _quote_ident(qc["windcode"])
    end_c = _quote_ident(qc["end_dt"])
    has_rank_col = "rank" in qc
    rank_sel = (
        f"LTRIM(RTRIM(CONVERT(NVARCHAR(64), h.{_quote_ident(qc['rank'])}))) AS rank_raw,\n          "
        if has_rank_col
        else "CAST(NULL AS NVARCHAR(64)) AS rank_raw,\n          "
    )
    name_c = _quote_ident(qc["name"])
    vol_c = _quote_ident(qc["vol"])
    pct_c = _quote_ident(qc["pct"])
    ann_sel = ""
    if "ann_dt" in qc:
        ann_sel = f"LTRIM(RTRIM(CONVERT(NVARCHAR(32), h.{_quote_ident(qc['ann_dt'])}))) AS ann_dt_raw,\n          "
    else:
        ann_sel = "CAST(NULL AS NVARCHAR(32)) AS ann_dt_raw,\n          "
    share_extra = ""
    if "share_cat" in qc:
        share_extra = f",\n          LTRIM(RTRIM(CONVERT(NVARCHAR(200), h.{_quote_ident(qc['share_cat'])}))) AS share_cat_raw"

    sql = f"""
        SELECT TOP 2000
          LTRIM(RTRIM(CONVERT(NVARCHAR(32), h.{end_c}))) AS end_dt_raw,
          {ann_sel}
          {rank_sel}
          LTRIM(RTRIM(CONVERT(NVARCHAR(512), h.{name_c}))) AS holder_name_raw,
          h.{vol_c} AS hold_vol_raw,
          h.{pct_c} AS hold_pct_raw{share_extra}
        FROM {t} h
        WHERE h.{wc} = :code
        """
    try:
        rows = conn.execute(text(sql), {"code": code}).mappings().all()
    except Exception as e:
        logger.exception("AShareInsideHolder 查询失败: %s", code)
        return {"error": str(e), "items": []}

    if not rows:
        return {"error": None, "items": [], "as_of_end_dt": None, "as_of_ann_dt": None}

    rows = [dict(r) for r in rows]
    latest_int: int | None = None
    for r in rows:
        v = _parse_intish(r.get("end_dt_raw"))
        if v is not None:
            latest_int = v if latest_int is None else max(latest_int, v)

    best_key: str | None = None
    if latest_int is not None:
        best_key = str(latest_int)
    else:
        for r in rows:
            ek = (r.get("end_dt_raw") or "").strip()
            if not ek:
                continue
            if best_key is None or ek > best_key:
                best_key = ek

    if best_key is None:
        return {"error": None, "items": [], "as_of_end_dt": None, "as_of_ann_dt": None}

    period_rows: list[dict[str, Any]] = []
    for r in rows:
        ri = _parse_intish(r.get("end_dt_raw"))
        if latest_int is not None:
            if ri != latest_int:
                continue
        else:
            if (r.get("end_dt_raw") or "").strip() != best_key:
                continue
        period_rows.append(r)

    picked: list[dict[str, Any]] = []
    if has_rank_col:
        for r in period_rows:
            rk = r.get("rank_raw")
            try:
                rki = int(float(str(rk).strip())) if rk is not None and str(rk).strip() != "" else 999
            except (TypeError, ValueError):
                rki = 999
            if rki > 10:
                continue
            picked.append(
                {
                    "_rki": rki,
                    "holder_name": (r.get("holder_name_raw") or "").strip() or None,
                    "hold_vol": _parse_floatish(r.get("hold_vol_raw")),
                    "hold_pct": _parse_floatish(r.get("hold_pct_raw")),
                    "share_category": (r.get("share_cat_raw") or "").strip() or None,
                    "ann_dt": (r.get("ann_dt_raw") or "").strip() or None,
                    "end_dt": (r.get("end_dt_raw") or "").strip() or None,
                }
            )
        picked.sort(key=lambda x: (x["_rki"], x.get("holder_name") or ""))
    else:
        period_rows.sort(
            key=lambda x: (-(_parse_floatish(x.get("hold_vol_raw")) or -1.0), (x.get("holder_name_raw") or "")),
        )
        seen_nm: set[str] = set()
        seq = 0
        for r in period_rows:
            nm = (r.get("holder_name_raw") or "").strip()
            if not nm or nm in seen_nm:
                continue
            seen_nm.add(nm)
            seq += 1
            if seq > 10:
                break
            picked.append(
                {
                    "_rki": seq,
                    "holder_name": nm or None,
                    "hold_vol": _parse_floatish(r.get("hold_vol_raw")),
                    "hold_pct": _parse_floatish(r.get("hold_pct_raw")),
                    "share_category": (r.get("share_cat_raw") or "").strip() or None,
                    "ann_dt": (r.get("ann_dt_raw") or "").strip() or None,
                    "end_dt": (r.get("end_dt_raw") or "").strip() or None,
                }
            )
    out_items: list[dict[str, Any]] = []
    ann_any: str | None = None
    for p in picked[:10]:
        d = {k: v for k, v in p.items() if k != "_rki"}
        d["rank"] = p["_rki"]
        out_items.append(d)
        if ann_any is None and d.get("ann_dt"):
            ann_any = d["ann_dt"]

    return {
        "error": None,
        "items": out_items,
        "as_of_end_dt": best_key,
        "as_of_ann_dt": ann_any,
    }

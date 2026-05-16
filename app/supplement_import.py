"""可扩展的补充数据导入：定义在 data_import_definitions，批次在 data_import_batches。"""

from __future__ import annotations

import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.config import settings
from app.services import normalize_code

CODE_COMPANY_PROFILE_EXCEL = "company_profile_excel"

TABLE_COMPANY_PROFILES = "supplement_company_profiles"
# 与 supplement_company_profiles 固定列冲突时自动改名（如 Excel 列名恰好为 id）
_RESERVED_COL_LOWER = frozenset(
    x.lower() for x in ("id", "definition_code", "stock_code", "last_batch_id", "updated_at")
)


class ImportDefinitionNotFoundError(Exception):
    """data_import_definitions 中无对应 code。"""

_ENTITY_COLUMN_CANDIDATES = [
    "证券代码",
    "股票代码",
    "代码",
    "wind代码",
    "Wind代码",
    "WIND代码",
    "S_INFO_WINDCODE",
]


def default_company_profile_xlsx_path() -> str:
    raw = (getattr(settings, "supplement_company_excel_path", None) or "").strip()
    if raw:
        return raw
    return str(Path(settings.strategy_root_dir) / "数据" / "公司资料.xlsx")


def _norm_col_name(s: Any) -> str:
    return str(s or "").strip().upper().replace(" ", "")


def _parse_definition_meta(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return {}
        try:
            o = json.loads(s)
            return o if isinstance(o, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _match_header_to_column(want: str, headers: list[str]) -> str | None:
    w = (want or "").strip()
    if not w:
        return None
    if w in headers:
        return w
    wn = _norm_col_name(w)
    for h in headers:
        if _norm_col_name(h) == wn:
            return h
    return None


def _meta_explicit_unique_columns(meta: dict[str, Any]) -> list[str]:
    arr = meta.get("unique_source_columns")
    if isinstance(arr, list) and arr:
        return [str(x).strip() for x in arr if str(x).strip()]
    s = str(meta.get("unique_source_column") or "").strip()
    if s:
        return [p.strip() for p in re.split(r"[,，;；]", s) if p.strip()]
    return []


def _resolve_unique_key_columns(df: pd.DataFrame, explicit: list[str]) -> list[str]:
    """explicit 为配置或请求中的列名列表；空列表则按证券代码类列启发式取单列。"""
    headers = [str(h) for h in df.columns]
    if explicit:
        out: list[str] = []
        for w in explicit:
            m = _match_header_to_column(w, headers)
            if m is None:
                raise ValueError(f"unknown_unique_key_column:{w}")
            out.append(m)
        return out
    c = _find_code_column(df)
    if not c:
        raise ValueError("cannot resolve unique source column")
    return [c]


def _find_code_column(df: pd.DataFrame) -> str | None:
    cols = list(df.columns)
    norm_map: dict[str, str] = {}
    for c in cols:
        norm_map[_norm_col_name(c)] = str(c)
    for cand in _ENTITY_COLUMN_CANDIDATES:
        k = _norm_col_name(cand)
        if k in norm_map:
            return norm_map[k]
    for c in cols:
        if _norm_col_name(c) in ("证券代码", "股票代码", "WIND代码"):
            return str(c)
    return None


_WIND_LIKE = re.compile(r"^[0-9]{6}\.(SH|SZ|BJ)$", re.IGNORECASE)


def _quote_ident(name: str) -> str:
    return "`" + str(name).replace("`", "``") + "`"


def _list_table_columns(db: Session, table: str) -> set[str]:
    rows = db.execute(
        text(
            """
            SELECT COLUMN_NAME FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :tn
            """
        ),
        {"tn": table},
    ).fetchall()
    return {str(r[0]) for r in rows}


def _dedupe_excel_headers(raw_cols: list[Any]) -> list[str]:
    counts: dict[str, int] = {}
    out: list[str] = []
    for name in raw_cols:
        base = str(name).strip() if name is not None else ""
        if not base:
            base = "列"
        n = counts.get(base, 0)
        if n == 0:
            out.append(base)
        else:
            out.append(f"{base}_{n}")
        counts[base] = n + 1
    return out


def _allocate_sql_column_name(want: str, used_lower: set[str]) -> str:
    raw = str(want).replace("`", "").replace("\n", " ").replace("\r", " ").strip()
    if len(raw) > 64:
        raw = raw[:64]
    if not raw:
        raw = "列"
    base = raw
    trial = base
    n = 2
    while trial.lower() in _RESERVED_COL_LOWER or trial.lower() in used_lower:
        suf = f"_{n}"
        trial = (base[: max(1, 64 - len(suf))] + suf)[:64]
        n += 1
    used_lower.add(trial.lower())
    return trial


def _cell_to_python_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    if isinstance(v, (pd.Timestamp, datetime)):
        return v.isoformat()[:19]
    if isinstance(v, date) and not isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        f = float(v)
        if f.is_integer() and abs(f) < 1e15:
            return int(f)
        return f
    if isinstance(v, (np.bool_,)):
        return bool(v)
    if isinstance(v, bool):
        return v
    if isinstance(v, (bytes,)):
        try:
            return v.decode("utf-8", errors="replace")
        except Exception:
            return str(v)
    return str(v).strip() if isinstance(v, str) else v


def _cell_to_db_text(v: Any) -> str | None:
    x = _cell_to_python_value(v)
    if x is None:
        return None
    if isinstance(x, (dict, list)):
        return json.dumps(x, ensure_ascii=False)
    if isinstance(x, bool):
        return "1" if x else "0"
    return str(x)


def _ensure_profile_columns(db: Session, sql_cols: list[str]) -> None:
    exist_lower = {c.lower() for c in _list_table_columns(db, TABLE_COMPANY_PROFILES)}
    for col in sql_cols:
        if col.lower() not in exist_lower:
            db.execute(
                text(
                    f"ALTER TABLE {_quote_ident(TABLE_COMPANY_PROFILES)} "
                    f"ADD COLUMN {_quote_ident(col)} TEXT NULL"
                )
            )
            exist_lower.add(col.lower())
    db.commit()


def _build_upsert_sql(excel_headers: list[str], sql_by_excel: dict[str, str]) -> str:
    cols_q = [_quote_ident("definition_code"), _quote_ident("stock_code")]
    ph = [":dc", ":sc"]
    for i, eh in enumerate(excel_headers):
        sn = sql_by_excel[eh]
        cols_q.append(_quote_ident(sn))
        ph.append(f":c{i}")
    cols_q.append(_quote_ident("last_batch_id"))
    ph.append(":bid")
    upd_parts = [
        f"{_quote_ident(sql_by_excel[eh])}=VALUES({_quote_ident(sql_by_excel[eh])})"
        for eh in excel_headers
    ]
    upd_parts.append(f"{_quote_ident('last_batch_id')}=VALUES({_quote_ident('last_batch_id')})")
    tbl = _quote_ident(TABLE_COMPANY_PROFILES)
    return (
        f"INSERT INTO {tbl} ({', '.join(cols_q)}) VALUES ({', '.join(ph)}) "
        f"ON DUPLICATE KEY UPDATE {', '.join(upd_parts)}"
    )


def _normalize_key_segment(v: Any) -> str:
    """唯一键的一段：尽量对证券代码 Wind 风格做 normalize，否则为去空字符串。"""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    if isinstance(v, float) and v == int(v):
        v = int(v)
    s = str(v).strip()
    if not s:
        return ""
    t = s.upper().replace(" ", "")
    if _WIND_LIKE.match(t) or (t.isdigit() and len(t) == 6):
        try:
            return normalize_code(v)
        except ValueError:
            return s
    if isinstance(v, (int, np.integer)):
        try:
            return normalize_code(int(v))
        except ValueError:
            return str(int(v))
    return s


def _build_entity_key(row: pd.Series, key_cols: list[str]) -> str | None:
    parts: list[str] = []
    for c in key_cols:
        if c not in row.index:
            return None
        seg = _normalize_key_segment(row[c])
        if not seg:
            return None
        parts.append(seg)
    key = "\x1f".join(parts)
    if not key:
        return None
    return key[:512]


def _read_tabular_file(path: Path, sheet: int | str = 0) -> pd.DataFrame:
    suf = path.suffix.lower()
    if suf == ".csv":
        for enc in ("utf-8-sig", "utf-8", "gbk", "gb18030"):
            try:
                return pd.read_csv(path, dtype=object, encoding=enc)
            except UnicodeDecodeError:
                continue
        return pd.read_csv(path, dtype=object, encoding_errors="replace")
    if suf in (".xlsx", ".xlsm", ".xls"):
        return pd.read_excel(path, sheet_name=sheet, dtype=object, engine=None)
    raise ValueError("unsupported import file type")


def import_company_profile_excel(
    db: Session,
    *,
    source_path: str,
    definition_code: str,
    actor_user_id: int | None,
    explicit_unique_headers: list[str],
) -> dict[str, Any]:
    path = Path(source_path)
    if not path.is_file():
        raise FileNotFoundError(source_path)

    df = _read_tabular_file(path, sheet=0)
    if df.empty:
        db.execute(
            text(
                """
                INSERT INTO data_import_batches
                (definition_code, source_file_path, status, rows_ok, rows_fail, message, actor_user_id)
                VALUES (:dc, :fp, 'SUCCESS', 0, 0, :m, :uid)
                """
            ),
            {
                "dc": definition_code,
                "fp": str(path)[:1024],
                "m": "导入文件无数据行",
                "uid": actor_user_id,
            },
        )
        db.commit()
        br = db.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()
        return {
            "ok": True,
            "batch_id": int(br["id"]) if br and br.get("id") is not None else 0,
            "rows_total": 0,
            "rows_ok": 0,
            "rows_fail": 0,
            "message": "导入文件无数据行",
            "unique_key_columns": [],
            "unique_source_column": None,
            "code_column_guess": None,
            "column_mapping": {},
        }

    excel_headers = _dedupe_excel_headers(list(df.columns))
    df.columns = excel_headers

    key_cols = _resolve_unique_key_columns(df, list(explicit_unique_headers or []))
    key_set = set(key_cols)
    data_headers = [h for h in excel_headers if h not in key_set]

    alloc_used: set[str] = set()
    sql_by_excel: dict[str, str] = {}
    for eh in data_headers:
        sql_by_excel[eh] = _allocate_sql_column_name(eh, alloc_used)

    sql_cols_ordered = list(dict.fromkeys(sql_by_excel[eh] for eh in data_headers))
    _ensure_profile_columns(db, sql_cols_ordered)

    upsert_sql = _build_upsert_sql(data_headers, sql_by_excel)
    rows_total = len(df)
    rows_ok = 0
    rows_fail = 0
    fail_samples: list[str] = []

    db.execute(
        text(
            """
            INSERT INTO data_import_batches
            (definition_code, source_file_path, status, rows_ok, rows_fail, message, actor_user_id)
            VALUES (:dc, :fp, 'RUNNING', 0, 0, '', :uid)
            """
        ),
        {"dc": definition_code, "fp": str(path)[:1024], "uid": actor_user_id},
    )
    db.commit()
    bid_row = db.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()
    batch_id = int(bid_row["id"]) if bid_row and bid_row.get("id") is not None else 0

    try:
        for _, ser in df.iterrows():
            ek = _build_entity_key(ser, key_cols)
            if not ek:
                rows_fail += 1
                if len(fail_samples) < 8:
                    fail_samples.append("无法根据唯一键列拼出键的一行（存在空值）")
                continue
            params: dict[str, Any] = {
                "dc": definition_code,
                "sc": ek[:512],
                "bid": batch_id or None,
            }
            for i, eh in enumerate(data_headers):
                params[f"c{i}"] = _cell_to_db_text(ser[eh])
            db.execute(text(upsert_sql), params)
            rows_ok += 1

        col_preview = {eh: sql_by_excel[eh] for eh in data_headers[:30]}
        extra = ""
        if len(data_headers) > 30:
            extra = f"（共 {len(data_headers)} 列动态字段，此处仅展示前 30 列映射）"
        key_desc = "、".join(key_cols)
        msg = (
            f"完成：成功 {rows_ok}，失败 {rows_fail}，共扫描 {rows_total} 行；"
            f"唯一键列「{key_desc}」→ stock_code（多列按列顺序拼接为键，最长 512 字符）；"
            f"(definition_code, stock_code) 相同则更新；其余列与表字段一一对应{extra}"
        )
        if fail_samples:
            msg += "；" + "；".join(fail_samples)
        db.execute(
            text(
                """
                UPDATE data_import_batches
                SET status='SUCCESS', rows_ok=:ok, rows_fail=:fail, message=:m
                WHERE id=:id
                """
            ),
            {"ok": rows_ok, "fail": rows_fail, "m": msg[:65000], "id": batch_id},
        )
        db.commit()
        return {
            "ok": True,
            "batch_id": batch_id,
            "rows_total": rows_total,
            "rows_ok": rows_ok,
            "rows_fail": rows_fail,
            "message": msg,
            "unique_key_columns": key_cols,
            "unique_source_column": key_cols[0] if len(key_cols) == 1 else None,
            "code_column_guess": key_cols[0] if key_cols else None,
            "column_mapping": col_preview,
            "column_count": len(data_headers),
        }
    except Exception as e:
        try:
            db.execute(
                text(
                    """
                    UPDATE data_import_batches
                    SET status='FAILED', rows_ok=:ok, rows_fail=:fail, message=:m
                    WHERE id=:id
                    """
                ),
                {
                    "ok": rows_ok,
                    "fail": rows_fail,
                    "m": str(e)[:65000],
                    "id": batch_id,
                },
            )
            db.commit()
        except Exception:
            db.rollback()
        raise


Runner = Callable[..., dict[str, Any]]

_IMPORT_RUNNERS: dict[str, Runner] = {}


def run_import_by_code(
    db: Session,
    *,
    code: str,
    file_path: str | None,
    actor_user_id: int | None,
    unique_source_column: str | None = None,
    unique_source_columns: list[str] | None = None,
) -> dict[str, Any]:
    row = db.execute(
        text(
            """
            SELECT code, display_name, default_file_path, enabled, meta_json
            FROM data_import_definitions
            WHERE code = :c
            LIMIT 1
            """
        ),
        {"c": code},
    ).mappings().first()
    if not row:
        raise ImportDefinitionNotFoundError()
    if not int(row.get("enabled") or 0):
        raise ValueError("import definition disabled")

    path = (file_path or "").strip()
    if not path:
        path = (row.get("default_file_path") or "").strip()
    if not path:
        if code == CODE_COMPANY_PROFILE_EXCEL:
            path = default_company_profile_xlsx_path()
    if not path:
        raise ValueError("import file path empty")

    meta = _parse_definition_meta(row.get("meta_json"))

    api_explicit: list[str] = []
    if unique_source_columns:
        api_explicit = [str(x).strip() for x in unique_source_columns if str(x).strip()]
    elif unique_source_column and str(unique_source_column).strip():
        api_explicit = [
            p.strip()
            for p in re.split(r"[,，;；]", str(unique_source_column).strip())
            if p.strip()
        ]
    merged_unique = api_explicit if api_explicit else _meta_explicit_unique_columns(meta)

    if code == CODE_COMPANY_PROFILE_EXCEL:
        return import_company_profile_excel(
            db,
            source_path=path,
            definition_code=code,
            actor_user_id=actor_user_id,
            explicit_unique_headers=merged_unique,
        )

    runner = _IMPORT_RUNNERS.get(code)
    if not runner:
        raise ValueError("unsupported import code")

    return runner(db, source_path=path, definition_code=code, actor_user_id=actor_user_id)

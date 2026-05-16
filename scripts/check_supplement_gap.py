"""对比 Excel 与 Turso supplement_company_profiles 行数差异。"""
from __future__ import annotations

from collections import Counter
from pathlib import Path

import libsql

from app.config import settings
from app.supplement_import import (
    CODE_COMPANY_PROFILE_EXCEL,
    _build_entity_key,
    _dedupe_excel_headers,
    _read_tabular_file,
)
from app.server_files import resolve_supplement_upload_path


def main() -> None:
    paths: list[Path] = []
    sp = resolve_supplement_upload_path(CODE_COMPANY_PROFILE_EXCEL)
    if sp:
        paths.append(sp)
    p = (settings.supplement_company_excel_path or "").strip()
    if p:
        paths.append(Path(p))
    paths.append(Path(settings.strategy_root_dir) / "数据" / "公司资料.xlsx")
    path = next((c for c in paths if c.is_file()), None)
    if not path:
        print("未找到本地 Excel")
        return

    df = _read_tabular_file(path, sheet=0)
    df.columns = _dedupe_excel_headers(list(df.columns))
    keys = [_build_entity_key(row, ["stock_code"]) for _, row in df.iterrows()]
    excel_set = set(keys)

    url = (settings.turso_database_url or "").strip()
    token = (settings.turso_auth_token or "").strip()
    conn = libsql.connect(database=url, auth_token=token, _check_same_thread=False)
    db_codes = {r[0] for r in conn.execute("SELECT stock_code FROM supplement_company_profiles").fetchall()}

    missing = sorted(excel_set - db_codes)
    extra = sorted(db_codes - excel_set)
    print("Excel 文件:", path)
    print("Excel 行数:", len(df))
    print("Excel 唯一 stock_code:", len(excel_set))
    print("库中行数:", len(db_codes))
    print("Excel 有、库中无:", len(missing))
    print("库中有、Excel 无:", len(extra))
    if missing:
        print("缺失样例 (前20):", missing[:20])
        suf = Counter(m.split(".")[-1] if "." in m else "(none)" for m in missing)
        print("缺失代码后缀分布:", dict(suf))
        miss_set = set(missing)
        idxs = [i + 1 for i, k in enumerate(keys) if k in miss_set]
        print("缺失行在 Excel 中的行号范围:", min(idxs), "-", max(idxs))
        print("是否集中在文件末尾:", max(idxs) >= len(df) - 50)
        # 连续块
        blocks = []
        start = prev = None
        for i in idxs:
            if start is None:
                start = prev = i
            elif i == prev + 1:
                prev = i
            else:
                blocks.append((start, prev))
                start = prev = i
        if start is not None:
            blocks.append((start, prev))
        print("缺失连续区间数:", len(blocks), "最大区间:", max(blocks, key=lambda b: b[1] - b[0]))
    rows = conn.execute(
        "SELECT last_batch_id, COUNT(*) FROM supplement_company_profiles GROUP BY last_batch_id"
    ).fetchall()
    print("按 last_batch_id:", rows)
    n688 = conn.execute(
        "SELECT COUNT(*) FROM supplement_company_profiles WHERE stock_code LIKE '688%'"
    ).fetchone()[0]
    print("库中 688 开头:", n688)
    if missing:
        miss_set = set(missing)
        for label, idx in [("缺口前一行", 5349), ("缺口后一行", 5450)]:
            k = keys[idx]
            in_db = k in db_codes
            print(f"  {label} (Excel行{idx+1}): {k} -> {'在库' if in_db else '不在库'}")


if __name__ == "__main__":
    main()

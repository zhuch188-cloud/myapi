#!/usr/bin/env python3
"""手动触发 Turso 业务日期列统一为 YYYY-MM-DD（与启动时 init_database 相同逻辑）。"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text

from app.db import _apply_date_text_normalization, create_app_engine


def main() -> int:
    engine = create_app_engine()
    with engine.connect() as conn:
        conn.execute(
            text("DELETE FROM site_settings WHERE setting_key='schema_dates_iso_v1'")
        )
        conn.commit()
        _apply_date_text_normalization(conn)
        conn.commit()
    print("Done: strategy_positions / strategy_holding_daily / strategy_nav_daily dates -> YYYY-MM-DD")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

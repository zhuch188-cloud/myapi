"""全量刷新策略列表指标快照表 strategy_list_metrics。"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import SessionLocalFactory, init_database
from app.strategy_list_metrics import refresh_strategy_list_metrics_cache


def main() -> int:
    parser = argparse.ArgumentParser(description="刷新 strategy_list_metrics 快照")
    parser.add_argument(
        "strategy_ids",
        nargs="*",
        help="策略 ID；省略则刷新全部已启用且可见策略",
    )
    args = parser.parse_args()
    init_database()
    db = SessionLocalFactory()
    try:
        ids = [str(x).strip() for x in args.strategy_ids if str(x).strip()]
        n = refresh_strategy_list_metrics_cache(
            db, ids or None, do_commit=True
        )
        print(f"refreshed {n} strategies")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())

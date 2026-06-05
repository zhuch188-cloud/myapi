#!/usr/bin/env python3
"""一次性数据库连通性探测（stdout 不含 token）。"""
from __future__ import annotations

import os
import sys

from app.config import settings
from app.db import _resolve_libsql_connect_url, is_hrana_transient_error


def _ping(label: str, raw: str, token: str) -> bool:
    resolved = _resolve_libsql_connect_url(raw)
    print(f"--- {label} ---")
    print(f"raw:      {raw.split('?')[0]}")
    print(f"resolved: {resolved.split('?')[0]}")
    kw: dict = {"_check_same_thread": False}
    if token:
        kw["auth_token"] = token
    attempts = 3
    for i in range(attempts):
        try:
            import libsql

            conn = libsql.connect(resolved, **kw)
            one = conn.execute("SELECT 1 AS n").fetchone()
            tables = conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
            ).fetchone()
            users = conn.execute("SELECT COUNT(*) FROM users").fetchone()
            print(f"SELECT 1:        {one}")
            print(f"table_count:     {tables}")
            print(f"users_count:     {users}")
            print("status:          OK")
            return True
        except Exception as exc:
            transient = is_hrana_transient_error(exc)
            print(f"attempt {i + 1}/{attempts}: FAIL ({'transient' if transient else 'fatal'}) {exc!r}")
            if not transient or i + 1 >= attempts:
                print("status:          FAIL")
                return False
    return False


def main() -> int:
    ok = True
    env_url = (settings.turso_database_url or os.environ.get("TURSO_DATABASE_URL") or "").strip()
    env_token = (settings.turso_auth_token or os.environ.get("TURSO_AUTH_TOKEN") or "").strip()
    if env_url:
        ok = _ping(".env / settings", env_url, env_token) and ok
    else:
        print("skip .env: TURSO_DATABASE_URL empty")

    ngrok = (os.environ.get("PING_NGROK_URL") or "").strip()
    if not ngrok:
        ngrok = "libsql://0.tcp.jp.ngrok.io:13932?tls=0"
    ok = _ping("ngrok (Render 测试地址)", ngrok, "") and ok
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

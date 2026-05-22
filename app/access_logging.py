"""JWT 鉴权请求的访问日志（管理员在「访问日志」页查看）。"""
from __future__ import annotations

import logging
from jose import JWTError, jwt
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from sqlalchemy import text

from app.config import settings

_log = logging.getLogger(__name__)

# 不记日志的路径（无鉴权或文档）
_SKIP_PATHS: frozenset[str] = frozenset(
    {
        "/docs",
        "/redoc",
        "/openapi.json",
        "/favicon.ico",
    }
)
_SKIP_PREFIXES: tuple[str, ...] = (
    "/static/",
    "/api/public/",
    "/health",
    "/api/admin/import-jobs",
    "/api/admin/sync-jobs",
    "/api/admin/update-jobs",
)


def _client_ip(request: Request) -> str:
    xff = request.headers.get("x-forwarded-for", "")
    if xff:
        return xff.split(",")[0].strip()[:64]
    host = request.client.host if request.client else ""
    return str(host or "")[:64]


def _should_skip_path(path: str) -> bool:
    if path in _SKIP_PATHS:
        return True
    for p in _SKIP_PREFIXES:
        if path.startswith(p):
            return True
    return False


def _path_for_log(request: Request) -> str:
    path = request.url.path or ""
    q = request.url.query or ""
    if not q:
        s = path
    else:
        q = q[:500] + ("…" if len(request.url.query) > 500 else "")
        s = f"{path}?{q}"
    return s[:1024]


def _insert_access_log(
    *,
    user_id: int,
    username: str,
    role: str,
    path: str,
    method: str,
    status_code: int,
    ip: str | None,
    user_agent: str | None,
) -> None:
    from app.db import SessionLocalFactory

    if SessionLocalFactory is None:
        return
    db = SessionLocalFactory()
    try:
        db.execute(
            text(
                """
                INSERT INTO user_access_logs
                  (user_id, username, role, path, method, status_code, ip, user_agent)
                VALUES
                  (:uid, :uname, :role, :path, :method, :sc, :ip, :ua)
                """
            ),
            {
                "uid": user_id,
                "uname": username[:64],
                "role": (role or "")[:32],
                "path": path[:1024],
                "method": (method or "GET")[:16],
                "sc": int(status_code),
                "ip": (ip or "")[:64] if ip else None,
                "ua": (user_agent or "")[:512] if user_agent else None,
            },
        )
        db.commit()
    except Exception:
        _log.debug("user_access_logs insert failed", exc_info=True)
        try:
            db.rollback()
        except Exception:
            pass
    finally:
        db.close()


class UserAccessLogMiddleware(BaseHTTPMiddleware):
    """在响应返回后写入一条访问记录（需有效 Bearer JWT 且 users 表存在该用户）。"""

    async def dispatch(self, request: Request, call_next) -> Response:
        response = await call_next(request)
        try:
            if request.method == "OPTIONS":
                return response
            path = request.url.path or ""
            if _should_skip_path(path):
                return response
            auth = request.headers.get("authorization") or request.headers.get("Authorization") or ""
            if not auth.lower().startswith("bearer "):
                return response
            token = auth[7:].strip()
            if not token:
                return response
            try:
                payload = jwt.decode(token, settings.jwt_secret, algorithms=["HS256"])
            except JWTError:
                return response
            username = str(payload.get("sub") or "").strip()
            if not username:
                return response

            from app.db import SessionLocalFactory

            if SessionLocalFactory is None:
                return response
            db = SessionLocalFactory()
            try:
                row = db.execute(
                    text("SELECT id, username, role FROM users WHERE username=:u LIMIT 1"),
                    {"u": username},
                ).mappings().first()
                if not row:
                    return response
                uid = int(row["id"])
                uname = str(row.get("username") or username)[:64]
                role = str(row.get("role") or "")[:32]
            finally:
                db.close()

            _insert_access_log(
                user_id=uid,
                username=uname,
                role=role,
                path=_path_for_log(request),
                method=request.method or "GET",
                status_code=int(response.status_code),
                ip=_client_ip(request) or None,
                user_agent=(request.headers.get("user-agent") or "")[:512] or None,
            )
        except Exception:
            _log.debug("access log middleware skipped", exc_info=True)
        return response


__all__ = ["UserAccessLogMiddleware"]

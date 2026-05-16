import hashlib
from datetime import datetime, timedelta, timezone
from jose import jwt, JWTError
from fastapi import Depends, HTTPException, status, Request
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy import text
from sqlalchemy.orm import Session
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from app.config import settings
from app.db import get_session
from app.sql_dialect import sql_curdate, sql_now

# 响应头：携带 JWT 的成功请求由中间件写入新令牌，前端 fetch 包装器写入 localStorage
ACCESS_TOKEN_RENEWAL_HEADER = "X-Access-Token-Renewal"
_SLIDING_RENEW_LEEWAY_SECONDS = 120

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")


def norm_user_status(raw) -> str:
    """统一 users.status 比较（兼容 ENUM 大小写、驱动返回形态）。"""
    return str(raw or "").strip().lower()


def _client_ip(request: Request) -> str:
    xff = request.headers.get("x-forwarded-for", "")
    if xff:
        return xff.split(",")[0].strip()[:64]
    host = request.client.host if request.client else ""
    return str(host or "")[:64]


def _record_viewer_activity(db: Session, user_id: int, request: Request) -> None:
    """viewer 每次带 JWT 的请求记一次自然日用量；可选 X-Device-Token 刷新设备最近活跃。"""
    try:
        db.execute(
            text(
                f"""
                INSERT INTO user_usage_daily (user_id, usage_date, api_requests)
                VALUES (:uid, {sql_curdate()}, 1)
                ON CONFLICT(user_id, usage_date) DO UPDATE SET api_requests = api_requests + 1
                """
            ),
            {"uid": user_id},
        )
        raw_dt = (request.headers.get("x-device-token") or "").strip()
        if raw_dt:
            h = hashlib.sha256(raw_dt.encode("utf-8")).hexdigest()
            ip = _client_ip(request)
            ua = (request.headers.get("user-agent", "") or "")[:512]
            db.execute(
                text(
                    f"""
                    UPDATE user_devices
                    SET last_seen_at={sql_now()}, ip_last=:ip, ua=:ua
                    WHERE user_id=:uid AND device_token_hash=:h AND revoked_at IS NULL
                    """
                ),
                {"uid": user_id, "h": h, "ip": ip, "ua": ua},
            )
        db.commit()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass


def create_access_token(username: str, extra_claims: dict | None = None) -> str:
    expires = datetime.now(timezone.utc) + timedelta(
        minutes=settings.access_token_expire_minutes
    )
    payload = {"sub": username, "exp": expires}
    if extra_claims:
        payload.update(extra_claims)
    raw = jwt.encode(payload, settings.jwt_secret, algorithm="HS256")
    return raw.decode("ascii") if isinstance(raw, (bytes, bytearray)) else str(raw)


def get_current_user(
    request: Request, token: str = Depends(oauth2_scheme), db: Session = Depends(get_session)
):
    credential_error = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authentication"
    )
    try:
        payload = jwt.decode(
            token,
            settings.jwt_secret,
            algorithms=["HS256"],
            options={"leeway": _SLIDING_RENEW_LEEWAY_SECONDS},
        )
        username = payload.get("sub")
    except JWTError:
        raise credential_error
    if not username:
        raise credential_error

    row = db.execute(
        text(
            """
            SELECT id, username, role, org_id, status
            FROM users
            WHERE username = :username
            LIMIT 1
            """
        ),
        {"username": username},
    ).mappings().first()
    if not row:
        raise credential_error
    u = dict(row)
    st = norm_user_status(u.get("status"))
    if st == "disabled":
        raise HTTPException(status_code=403, detail="account disabled")
    if st == "locked":
        raise HTTPException(status_code=403, detail="account locked")

    must_change = bool(payload.get("must_change_password"))
    path = request.url.path or ""
    if must_change and path not in (
        "/api/auth/change-password",
        "/api/auth/me",
        "/api/auth/profile",
    ):
        raise HTTPException(
            status_code=403,
            detail="must_change_password",
        )
    u["must_change_password"] = must_change
    if u.get("role") == "viewer":
        _record_viewer_activity(db, int(u["id"]), request)
    return u


def require_roles(*roles: str):
    def checker(user=Depends(get_current_user)):
        if user["role"] not in roles:
            raise HTTPException(status_code=403, detail="Permission denied")
        return user

    return checker


def _skip_sliding_renewal_path(path: str) -> bool:
    if path in ("/docs", "/redoc", "/openapi.json", "/favicon.ico"):
        return True
    if path.startswith("/static/"):
        return True
    if path.startswith("/api/public/"):
        return True
    # 仅 JSON API 需要续期头；页面/HTML、文件流等不走此逻辑，避免反向代理下非 JSON 响应写头触发异常
    if not path.startswith("/api/"):
        return True
    return False


class SlidingJWTAccessMiddleware(BaseHTTPMiddleware):
    """
    请求已带有效 Bearer JWT 且响应为 2xx/3xx 时，在响应头附带新签发的访问令牌（自此刻起再延长 access_token_expire_minutes），
    实现滑动续期；须与前端对 fetch 的包装（读取 ACCESS_TOKEN_RENEWAL_HEADER 写回 localStorage）配合。
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        response = await call_next(request)
        try:
            if request.method == "OPTIONS":
                return response
            path = request.url.path or ""
            if _skip_sliding_renewal_path(path):
                return response
            if response.status_code < 200 or response.status_code >= 400:
                return response
            auth = request.headers.get("authorization") or request.headers.get("Authorization") or ""
            if not auth.lower().startswith("bearer "):
                return response
            raw = auth[7:].strip()
            if not raw:
                return response
            try:
                payload = jwt.decode(
                    raw,
                    settings.jwt_secret,
                    algorithms=["HS256"],
                    options={
                        "verify_signature": True,
                        "verify_exp": True,
                        "leeway": _SLIDING_RENEW_LEEWAY_SECONDS,
                    },
                )
            except JWTError:
                return response
            username = payload.get("sub")
            if not username:
                return response
            extra: dict = {}
            if bool(payload.get("must_change_password")):
                extra["must_change_password"] = True
            ct = (response.headers.get("content-type") or "").lower()
            if "application/json" not in ct:
                return response
            new_tok = create_access_token(str(username), extra_claims=extra or None)
            response.headers[ACCESS_TOKEN_RENEWAL_HEADER] = new_tok
        except Exception:
            pass
        return response

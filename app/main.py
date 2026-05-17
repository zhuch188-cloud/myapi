from pydantic import BaseModel, Field

from fastapi import BackgroundTasks, Body, FastAPI, Depends, HTTPException, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import text
from sqlalchemy.orm import Session
import re
import io
import time
import logging
import csv
import math
import statistics
import json
import hashlib
import os
import secrets
import string
from pathlib import Path
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler

from app.config import settings
from app.timeutil import now, now_naive, today as beijing_today
from app.bg_threads import spawn_daemon
from app.boot import boot_error, is_ready, start_background_boot
from app.db import DatabaseNotReadyError

_log = logging.getLogger(__name__)
from app.access_logging import UserAccessLogMiddleware
from app.db import init_database, get_session
from app.update_lock import strategy_update_mutex
from app.sql_dialect import (
    list_table_columns,
    quote_ident as _sql_quote_ident,
    sql_curdate,
    sql_hours_ago,
    sql_minutes_ago,
    sql_now,
    sql_timestampdiff_hours,
)
from app.mail import send_contact_us_message, send_password_reset_email, smtp_send_test
from app.client_messages import insert_client_submission, list_client_submissions
from app.client_safe import public_message
from app.auth import SlidingJWTAccessMiddleware, create_access_token, get_current_user, require_roles, norm_user_status
from app import ark_client, stock_trend, wind_holders, wind_income, wind_sql
from app.services import (
    execute_admin_sync_pipeline,
    import_strategy_files,
    normalize_code,
    rebuild_nav_series,
    run_admin_sync_background_task,
    run_update,
    create_strategy_import_job,
    get_strategy_import_job_row,
    run_strategy_import_background_task,
    strategy_import_job_is_resumable,
    latest_rebalance_date_by_strategy,
)
from app.supplement_import import (
    CODE_COMPANY_PROFILE_EXCEL,
    DataImportBatchNotFoundError,
    DataImportBatchNotResumableError,
    ImportDefinitionNotFoundError,
    batch_is_resumable,
    default_company_profile_xlsx_path,
    get_data_import_batch_row,
    resume_data_import_batch,
    run_import_by_code,
)

_TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
scheduler = BackgroundScheduler()


@asynccontextmanager
async def _app_lifespan(_app: FastAPI):
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )
    start_background_boot(scheduler, _scheduled_update)
    yield
    if scheduler.running:
        scheduler.shutdown()


app = FastAPI(title=settings.app_name, lifespan=_app_lifespan)
app.add_middleware(UserAccessLogMiddleware)
app.add_middleware(SlidingJWTAccessMiddleware)
templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))


@app.exception_handler(DatabaseNotReadyError)
def _database_not_ready_handler(_request: Request, exc: DatabaseNotReadyError):
    return JSONResponse(status_code=503, content={"detail": str(exc)})


_SID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
_AUTO_USERNAME_PREFIX = "8"
_AUTO_USERNAME_LEN = 9
_AUTO_PASSWORD_LEN = 10
_DEVICE_TOKEN_DAYS = 365
_LOGIN_FAIL_LOCK_THRESHOLD = 5
_LOGIN_LOCK_MINUTES = 15
_PASSWORD_RESET_TOKEN_HOURS = 1
_FORGOT_PW_WINDOW_SEC = 900
_FORGOT_PW_MAX_PER_WINDOW = 10
_FORGOT_PW_TS: dict[str, list[float]] = {}
_PUBLIC_CONTACT_WINDOW_SEC = 900
_PUBLIC_CONTACT_MAX_PER_WINDOW = 10
_PUBLIC_CONTACT_TS: dict[str, list[float]] = {}

_DETAIL_CN_MAP: dict[str, str] = {
    "email too long": "邮箱长度超限",
    "invalid email format": "邮箱格式不正确",
    "email already exists": "邮箱已存在",
    "phone already exists": "手机号已存在",
    "nickname already exists": "昵称已存在",
    "auto user generation failed": "自动创建用户失败，请稍后重试",
    "strategy not found": "策略不存在",
    "identifier not unique, please contact admin": "登录标识不唯一，请联系管理员",
    "account disabled": "账号已禁用",
    "client login gate enabled": "已开启「须先显示登录页」，请使用登录页输入账号密码登录",
    "account not active": "账号已停用或锁定，无法使用本方式登录",
    "client registration disabled": "当前未开放新用户注册",
    "superuser password incorrect": "超级用户（admin）密码错误",
    "superuser admin not found": "未找到内置超级用户 admin，无法校验",
    "account locked": "账号已锁定",
    "registration failed, retry later": "注册失败，请稍后重试",
    "password must be at least 6 chars": "密码长度至少 6 位",
    "user not found": "用户不存在",
    "old password incorrect": "原密码错误",
    "invalid strategy_id": "策略ID不合法",
    "page_size must be 20, 50, or 100": "每页条数仅支持 20、50、100",
    "invalid stock_code": "股票代码不合法",
    "stock not found": "股票不存在",
    "page must be >= 1": "页码必须大于等于 1",
    "page_size must be between 1 and 500": "每页条数必须在 1 到 500 之间",
    "limit must be between 1 and 10000": "limit 必须在 1 到 10000 之间",
    "strategy_id must not be empty": "策略ID不能为空",
    "file_name must not be empty": "文件名不能为空",
    "empty file": "文件为空",
    "only .csv is supported for config import": "仅支持导入 .csv 配置文件",
    "no rows in file": "文件中没有有效数据行",
    "strategy_ids must be a non-empty list": "strategy_ids 必须是非空列表",
    "no valid strategy_ids": "没有有效的 strategy_id",
    "strategy_ids must be a list": "strategy_ids 必须是列表",
    "import_mode must be full or incremental": "import_mode 仅支持 full 或 incremental",
    "import definition not found": "未找到该导入配置",
    "import definition disabled": "该导入已禁用",
    "import file not found": "找不到待导入文件，请检查路径",
    "import file path empty": "未指定导入文件路径",
    "unsupported import code": "暂不支持该导入类型",
    "limit must be between 1 and 200": "limit 必须在 1 到 200 之间",
    "unknown unique_source_column": "唯一键列在 Excel 中不存在，请核对表头",
    "unknown_unique_key_column": "唯一键列在导入文件中不存在，请核对表头",
    "cannot resolve unique source column": "无法确定唯一键列，请在请求或 meta_json 中配置 unique_source_column / unique_source_columns",
    "unsupported import file type": "仅支持 .xlsx / .xls / .xlsm / .csv 作为导入文件",
    "invalid status": "状态值不合法",
    "Invalid authentication": "登录状态无效，请重新登录",
    "must_change_password": "请先修改密码后再继续操作",
    "Permission denied": "没有操作权限",
}


def _detail_to_cn(detail: object) -> object:
    if isinstance(detail, str):
        s = detail.strip()
        if s in _DETAIL_CN_MAP:
            return _DETAIL_CN_MAP[s]
        if s.startswith("unknown_unique_key_column:"):
            rest = s.split(":", 1)[1].strip()
            return f"下列唯一键列在文件中不存在：{rest}"
        if s.startswith("account locked until "):
            return "账号已锁定，请稍后重试"
    return detail


def _strategy_weight_display_mode_store() -> str:
    """strategy_configs.weight_display_mode 仅存 holding；净值与持仓日快照均按持仓权重。"""
    return "holding"


@app.exception_handler(HTTPException)
async def _http_exception_cn(_, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": _detail_to_cn(exc.detail)},
        headers=getattr(exc, "headers", None),
    )


class UserProfilePayload(BaseModel):
    nickname: str = Field(default="", max_length=64)
    phone: str = Field(default="", max_length=32)
    email: str = Field(default="", max_length=255)
    bio: str = Field(default="", max_length=500)
    # 仅当账号仍为系统下发密码时由服务端采纳；与资料一并保存时同步更新登录密码。
    new_password: str = Field(default="", max_length=128)


class ClientContactPayload(BaseModel):
    title: str = Field(default="", max_length=200)
    contact: str = Field(default="", max_length=255)
    content: str = Field(default="", max_length=20000)


def _validated_contact_fields(payload: ClientContactPayload) -> tuple[str, str, str]:
    title = (payload.title or "").strip()
    content = (payload.content or "").strip()
    contact = (payload.contact or "").strip()
    if not title:
        raise HTTPException(status_code=400, detail="请填写标题")
    if not content:
        raise HTTPException(status_code=400, detail="请填写内容")
    if len(title) > 200:
        raise HTTPException(status_code=400, detail="标题过长")
    if len(content) > 20000:
        raise HTTPException(status_code=400, detail="内容过长")
    if len(contact) > 255:
        raise HTTPException(status_code=400, detail="联系方式过长")
    return title, content, contact


class AdminSystemInitPayload(BaseModel):
    """执行系统初始化前须校验内置超级用户 admin 的登录密码（与 JWT 身份无关，防误触）。"""
    admin_password: str = Field(min_length=1, max_length=128)


class RegisterPayload(BaseModel):
    password: str = Field(min_length=6, max_length=128)
    email: str = Field(min_length=3, max_length=255)
    nickname: str = Field(default="", max_length=64)
    phone: str = Field(default="", max_length=32)
    device_token: str = Field(default="", max_length=2048)


class ForgotPasswordPayload(BaseModel):
    username: str = Field(default="", max_length=64)
    email: str = Field(default="", max_length=255)


class ResetPasswordPayload(BaseModel):
    token: str = Field(min_length=8, max_length=512)
    new_password: str = Field(min_length=6, max_length=128)


class AdminSmtpTestPayload(BaseModel):
    """收件人；不传则默认发往 SMTP_FROM_ADDR / SMTP_USER。"""
    to: str = Field(default="", max_length=255)


class AdminDataImportRunPayload(BaseModel):
    """执行一条已注册的补充数据导入。"""
    code: str = Field(min_length=1, max_length=64)
    file_path: str = Field(default="", max_length=1024)
    # 与导入文件表头一致；单列或多列（多列时按顺序拼接为唯一键写入 stock_code）；不写入动态列
    unique_source_column: str = Field(default="", max_length=512)
    unique_source_columns: list[str] = Field(default_factory=list)
    # 默认后台导入，避免远程 Turso 大批量写入导致 HTTP 超时
    background: bool = True


def _norm_profile_nickname(raw: str) -> str | None:
    s = str(raw or "").strip()
    return s[:64] if s else None


def _norm_profile_phone(raw: str) -> str | None:
    s = "".join(ch for ch in str(raw or "").strip() if ch.isdigit() or ch in "+- ()")
    return s[:32] if s else None


def _norm_profile_email(raw: str) -> str | None:
    s = str(raw or "").strip()
    if not s:
        return None
    if len(s) > 255:
        raise HTTPException(status_code=400, detail="email too long")
    parts = s.split("@")
    if len(parts) != 2 or not parts[0].strip() or not parts[1].strip() or "." not in parts[1]:
        raise HTTPException(status_code=400, detail="invalid email format")
    return s


def _norm_profile_bio(raw: str) -> str | None:
    s = str(raw or "").strip()
    return s[:500] if s else None


def _ensure_unique_identity_fields(
    db: Session,
    *,
    email: str | None = None,
    phone: str | None = None,
    nickname: str | None = None,
    exclude_user_id: int | None = None,
) -> None:
    if email:
        row = db.execute(
            text(
                """
                SELECT id FROM users
                WHERE contact_email=:v
                  AND (:uid IS NULL OR id<>:uid)
                LIMIT 1
                """
            ),
            {"v": email, "uid": exclude_user_id},
        ).mappings().first()
        if row:
            raise HTTPException(status_code=400, detail="email already exists")
    if phone:
        row = db.execute(
            text(
                """
                SELECT id FROM users
                WHERE contact_phone=:v
                  AND (:uid IS NULL OR id<>:uid)
                LIMIT 1
                """
            ),
            {"v": phone, "uid": exclude_user_id},
        ).mappings().first()
        if row:
            raise HTTPException(status_code=400, detail="phone already exists")
    if nickname:
        row = db.execute(
            text(
                """
                SELECT id FROM users
                WHERE nickname=:v
                  AND (:uid IS NULL OR id<>:uid)
                LIMIT 1
                """
            ),
            {"v": nickname, "uid": exclude_user_id},
        ).mappings().first()
        if row:
            raise HTTPException(status_code=400, detail="nickname already exists")


def _find_user_for_login(db: Session, login_id: str) -> dict | None:
    ident = str(login_id or "").strip()
    if not ident:
        return None
    rows = db.execute(
        text(
            """
            SELECT
              id, username, role, org_id, password, status,
              password_is_system_generated, locked_until, failed_login_count
            FROM users
            WHERE username=:v OR contact_email=:v OR contact_phone=:v OR nickname=:v
            ORDER BY
              CASE
                WHEN username=:v THEN 0
                WHEN contact_email=:v THEN 1
                WHEN contact_phone=:v THEN 2
                WHEN nickname=:v THEN 3
                ELSE 9
              END,
              id ASC
            LIMIT 2
            """
        ),
        {"v": ident},
    ).mappings().all()
    if not rows:
        return None
    if len(rows) > 1:
        # 手机号/邮箱/昵称重复：阻止歧义登录（事件入库由调用方记录）
        return {"__login_conflict__": True}
    return dict(rows[0])


def _forgot_pw_rate_check(ip: str) -> None:
    now = time.time()
    lst = _FORGOT_PW_TS.setdefault(ip or "_", [])
    lst[:] = [t for t in lst if now - t < _FORGOT_PW_WINDOW_SEC]
    if len(lst) >= _FORGOT_PW_MAX_PER_WINDOW:
        raise HTTPException(status_code=429, detail="请求过于频繁，请稍后再试")
    lst.append(now)


def _public_contact_rate_check(ip: str) -> None:
    """未登录「联系我们」防刷：按 IP 滑动窗口限次。"""
    now = time.time()
    lst = _PUBLIC_CONTACT_TS.setdefault(ip or "_", [])
    lst[:] = [t for t in lst if now - t < _PUBLIC_CONTACT_WINDOW_SEC]
    if len(lst) >= _PUBLIC_CONTACT_MAX_PER_WINDOW:
        raise HTTPException(status_code=429, detail="请求过于频繁，请稍后再试")
    lst.append(now)


def _client_ip(request: Request | None) -> str:
    if request is None:
        return ""
    xff = request.headers.get("x-forwarded-for", "")
    if xff:
        return xff.split(",")[0].strip()[:64]
    host = request.client.host if request.client else ""
    return str(host or "")[:64]


def _device_token_hash(token: str) -> str:
    return hashlib.sha256(str(token or "").encode("utf-8")).hexdigest()


def _rand_digits(n: int) -> str:
    return "".join(secrets.choice(string.digits) for _ in range(max(1, int(n or 1))))


def _generate_auto_username() -> str:
    return _AUTO_USERNAME_PREFIX + _rand_digits(_AUTO_USERNAME_LEN - 1)


def _generate_system_password() -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(_AUTO_PASSWORD_LEN))


def _create_auto_user(db: Session) -> dict:
    for _ in range(20):
        username = _generate_auto_username()
        password = _generate_system_password()
        try:
            db.execute(
                text(
                    """
                    INSERT INTO users(
                      username, password, role, org_id, status,
                      password_is_system_generated, password_changed_at
                    ) VALUES (
                      :u, :p, 'viewer', 'org-client', 'active', 1, NULL
                    )
                    """
                ),
                {"u": username, "p": password},
            )
            db.commit()
            row = db.execute(
                text(
                    """
                    SELECT id, username, role, org_id, password_is_system_generated
                    FROM users WHERE username=:u LIMIT 1
                    """
                ),
                {"u": username},
            ).mappings().first()
            d = dict(row or {})
            d["raw_password"] = password
            return d
        except Exception:
            db.rollback()
            continue
    raise HTTPException(status_code=500, detail="auto user generation failed")


def _find_user_by_device_token(db: Session, device_token: str) -> dict | None:
    token = str(device_token or "").strip()
    if not token:
        return None
    now = now_naive()
    h = _device_token_hash(token)
    row = db.execute(
        text(
            """
            SELECT
              u.id, u.username, u.role, u.org_id, u.status,
              d.id AS device_id, d.device_token_expires_at, d.revoked_at
            FROM user_devices d
            JOIN users u ON u.id = d.user_id
            WHERE d.device_token_hash=:h
              AND d.revoked_at IS NULL
              AND d.device_token_expires_at > :now_dt
            LIMIT 1
            """
        ),
        {"h": h, "now_dt": now},
    ).mappings().first()
    return dict(row) if row else None


def _upsert_device_for_user(
    db: Session,
    user_id: int,
    device_token: str,
    request: Request | None,
    trusted: bool = True,
) -> str:
    """
    绑定或刷新浏览器 device_token 与 users 的关系。

    若请求里带来的 token 在库中已绑定「另一用户」，不得原地 UPDATE user_id（否则同一浏览器先后登录
    两账号会互相「顶掉」设备行，/api/auth/device/identify 与静默会话会指向后登录者，表现为账号错乱）。
    此时应作废旧行并为当前用户签发新 token，由前端写入 localStorage。
    """
    token = str(device_token or "").strip()
    if not token:
        token = secrets.token_urlsafe(48)
    now = now_naive()
    exp = now + timedelta(days=_DEVICE_TOKEN_DAYS)
    h = _device_token_hash(token)
    ip = _client_ip(request)
    ua = (request.headers.get("user-agent", "") if request else "")[:512]
    platform = (request.headers.get("sec-ch-ua-platform", "") if request else "")[:128]
    ex = db.execute(
        text("SELECT id, user_id FROM user_devices WHERE device_token_hash=:h LIMIT 1"),
        {"h": h},
    ).mappings().first()
    uid = int(user_id)
    if ex:
        ex_uid = int(ex["user_id"])
        if ex_uid != uid:
            db.execute(
                text("UPDATE user_devices SET revoked_at=:now_dt WHERE id=:id AND revoked_at IS NULL"),
                {"now_dt": now, "id": ex["id"]},
            )
            token = secrets.token_urlsafe(48)
            h = _device_token_hash(token)
            db.execute(
                text(
                    """
                    INSERT INTO user_devices(
                      user_id, device_token_hash, device_token_expires_at, trusted,
                      device_name, ua, platform, ip_first, ip_last, last_seen_at, revoked_at
                    ) VALUES (
                      :uid, :h, :exp, :trusted, NULL, :ua, :platform, :ip, :ip, :now_dt, NULL
                    )
                    """
                ),
                {
                    "uid": uid,
                    "h": h,
                    "exp": exp,
                    "trusted": 1 if trusted else 0,
                    "ua": ua,
                    "platform": platform,
                    "ip": ip,
                    "now_dt": now,
                },
            )
            return token
        db.execute(
            text(
                """
                UPDATE user_devices
                SET device_token_expires_at=:exp,
                    trusted=:trusted,
                    ua=:ua,
                    platform=:platform,
                    ip_last=:ip,
                    last_seen_at=:now_dt,
                    revoked_at=NULL
                WHERE id=:id
                """
            ),
            {
                "exp": exp,
                "trusted": 1 if trusted else 0,
                "ua": ua,
                "platform": platform,
                "ip": ip,
                "now_dt": now,
                "id": ex["id"],
            },
        )
    else:
        db.execute(
            text(
                """
                INSERT INTO user_devices(
                  user_id, device_token_hash, device_token_expires_at, trusted,
                  device_name, ua, platform, ip_first, ip_last, last_seen_at, revoked_at
                ) VALUES (
                  :uid, :h, :exp, :trusted, NULL, :ua, :platform, :ip, :ip, :now_dt, NULL
                )
                """
            ),
            {
                "uid": uid,
                "h": h,
                "exp": exp,
                "trusted": 1 if trusted else 0,
                "ua": ua,
                "platform": platform,
                "ip": ip,
                "now_dt": now,
            },
        )
    return token


def _audit_log(
    db: Session,
    action: str,
    actor_user_id: int | None = None,
    target_user_id: int | None = None,
    detail: dict | None = None,
    request: Request | None = None,
) -> None:
    try:
        db.execute(
            text(
                """
                INSERT INTO audit_logs(actor_user_id, action, target_user_id, detail_json, ip, ua)
                VALUES (:actor, :action, :target, :detail, :ip, :ua)
                """
            ),
            {
                "actor": actor_user_id,
                "action": str(action or "")[:128],
                "target": target_user_id,
                "detail": json.dumps(detail or {}, ensure_ascii=False),
                "ip": _client_ip(request)[:64],
                "ua": (request.headers.get("user-agent", "") if request else "")[:512],
            },
        )
    except Exception:
        pass


def _login_identifier_kind(ident: str) -> str:
    s = str(ident or "").strip()
    if not s:
        return "empty"
    if "@" in s:
        return "email"
    if s.startswith("+") or s.isdigit():
        return "phone"
    # 前台自动生成用户名：8 + 8位数字（总长 9）
    if len(s) == 9 and s.startswith("8") and s[1:].isdigit():
        return "username"
    # 昵称也可能是任意字符串：后端兜底归类，便于报表筛选
    if any("\u4e00" <= ch <= "\u9fff" for ch in s):
        return "nickname"
    return "username"


def _insert_login_event(
    db: Session,
    *,
    user_id: int | None,
    login_identifier: str,
    login_type: str,
    result: str,
    reason: str | None,
    request: Request | None,
) -> None:
    try:
        ident = str(login_identifier or "").strip()[:255]
        if not ident:
            ident = "(empty)"
        lt = str(login_type or "unknown")[:32]
        rs = "SUCCESS" if str(result or "").upper() == "SUCCESS" else "FAIL"
        rsn = (reason or None)
        if rsn is not None:
            rsn = str(rsn)[:128]
        db.execute(
            text(
                """
                INSERT INTO login_events(
                  user_id, login_identifier, login_type, result, reason, ip, ua
                ) VALUES (
                  :uid, :ident, :lt, :rs, :rsn, :ip, :ua
                )
                """
            ),
            {
                "uid": user_id,
                "ident": ident,
                "lt": lt,
                "rs": rs,
                "rsn": rsn,
                "ip": _client_ip(request)[:64],
                "ua": (request.headers.get("user-agent", "") if request else "")[:512],
            },
        )
    except Exception:
        pass


_CLIENT_LOGIN_SETTING_KEY = "client_require_login"
_CLIENT_REGISTER_SETTING_KEY = "client_allow_register"
_CLIENT_CONTACT_ENABLED_KEY = "client_contact_enabled"
_CLIENT_FEEDBACK_ENABLED_KEY = "client_feedback_enabled"
_SUPERUSER_LOGIN_USERNAME = "admin"


def _site_setting_get(db: Session, key: str, default: str) -> str:
    row = db.execute(
        text("SELECT setting_value FROM site_settings WHERE setting_key=:k LIMIT 1"),
        {"k": key},
    ).mappings().first()
    if not row:
        return default
    return str(row.get("setting_value") or default).strip() or default


def _client_require_login_enabled(db: Session) -> bool:
    """True：未登录访客打开前台会先被引导到登录页；False：允许设备静默签发 JWT（与免登录浏览一致）。"""
    v = _site_setting_get(db, _CLIENT_LOGIN_SETTING_KEY, "1").lower()
    return v not in ("0", "false", "no", "off")


def _client_register_allowed(db: Session) -> bool:
    """True：前台开放自助注册；False：隐藏注册入口并拒绝注册接口。"""
    v = _site_setting_get(db, _CLIENT_REGISTER_SETTING_KEY, "1").lower()
    return v not in ("0", "false", "no", "off")


def _site_setting_bool(db: Session, key: str, default: str) -> bool:
    v = _site_setting_get(db, key, default).lower()
    return v not in ("0", "false", "no", "off")


def _client_contact_enabled(db: Session) -> bool:
    return _site_setting_bool(db, _CLIENT_CONTACT_ENABLED_KEY, "1")


def _client_feedback_enabled(db: Session) -> bool:
    return _site_setting_bool(db, _CLIENT_FEEDBACK_ENABLED_KEY, "0")


def _submit_client_message(
    db: Session,
    *,
    kind: str,
    title: str,
    content: str,
    contact: str,
    user: dict | None,
    public_guest: bool,
    client_ip: str,
) -> dict:
    """写入数据库；联系我们另尝试发邮件（失败不影响入库成功）。"""
    uid = int(user["id"]) if user and user.get("id") is not None else None
    uname = str(user.get("username") or "").strip() if user else ""
    insert_client_submission(
        db,
        kind=kind,
        title=title,
        content=content,
        contact_info=contact,
        user_id=uid,
        username=uname,
        public_guest=public_guest,
        client_ip=client_ip,
    )
    db.commit()
    mail_ok = None
    if kind == "contact":
        mail_ok, _mail_err = send_contact_us_message(
            settings,
            title=title,
            content=content,
            contact=contact,
            from_username=uname,
            public_guest=public_guest,
            client_ip=client_ip,
        )
    out: dict = {"ok": True, "saved": True}
    if kind == "contact":
        out["mail_sent"] = bool(mail_ok)
    return out


def _site_setting_upsert(db: Session, key: str, value: str) -> None:
    db.execute(
        text(
            """
            INSERT INTO site_settings (setting_key, setting_value)
            VALUES (:k, :v)
            ON CONFLICT(setting_key) DO UPDATE SET setting_value=excluded.setting_value
            """
        ),
        {"k": key[:64], "v": str(value)[:255]},
    )


def _risk_summary_from_login_counts(*, fail24h: int, success24h: int, fail7d: int) -> dict:
    score = 0
    reasons: list[str] = []
    if fail24h >= 10:
        score += 40
        reasons.append("24小时内失败次数偏高")
    elif fail24h >= 5:
        score += 20
        reasons.append("24小时内多次登录失败")
    if fail7d >= 30:
        score += 25
        reasons.append("7日内累计失败次数偏高")
    ratio = 0.0
    denom = fail24h + success24h
    if denom > 0:
        ratio = fail24h / denom
    if ratio >= 0.75 and denom >= 6:
        score += 25
        reasons.append("近期失败占比过高")
    elif ratio >= 0.5 and denom >= 8:
        score += 15
        reasons.append("近期失败占比较高")
    score = min(100, int(score))
    level = "低"
    if score >= 70:
        level = "高"
    elif score >= 35:
        level = "中"
    return {"score": score, "level": level, "reasons": reasons}


def _normalize_strategy_file_dir(strategy_id: str, file_dir: str | None) -> str:
    from app.server_files import normalize_strategy_file_dir

    return normalize_strategy_file_dir(strategy_id, file_dir)


def _normalize_file_name(raw: str) -> str:
    """规范化 file_name：仅文件名、去空白、压缩重复片段、限制长度。"""
    s = str(raw or "").strip().replace("\\", "/")
    s = os.path.basename(s)
    s = re.sub(r"\s+", " ", s)
    m = re.search(r"\.[A-Za-z0-9]{1,8}$", s)
    ext = m.group(0) if m else ""
    stem = s[: -len(ext)] if ext else s

    # 压缩明显重复片段：A_B_C_A_B_C_A_B_C -> A_B_C
    parts = [x for x in stem.split("_") if x != ""]
    if parts:
        changed = True
        while changed:
            changed = False
            n = len(parts)
            for k in range(1, max(2, n // 2 + 1)):
                if n >= 2 * k and parts[-2 * k : -k] == parts[-k:]:
                    parts = parts[:-k]
                    changed = True
                    break
        stem = "_".join(parts)

    out = (stem + ext).strip(" .")
    if len(out) > 255:
        out = out[-255:]
    return out


def _safe_float(v):
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _fmt_pct(v) -> str:
    fv = _safe_float(v)
    if fv is None:
        return "-"
    return f"{fv * 100:.2f}%"


def _fmt_num(v, n: int = 2) -> str:
    fv = _safe_float(v)
    if fv is None:
        return "-"
    return f"{fv:.{n}f}"


def _round_nav_unit(v) -> float | None:
    """策略单位净值对外统一保留 2 位小数。"""
    fv = _safe_float(v)
    if fv is None:
        return None
    return round(fv, 2)


def _nav_metric_denominator(nav_val: float | None) -> float:
    """区间前无净值记录时，指标分母隐含为成立日单位净值 1.0。"""
    v = _safe_float(nav_val)
    if v is not None and v > 0:
        return float(v)
    return 1.0


def _to_date_key(d) -> date | None:
    if d is None:
        return None
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    if hasattr(d, "date") and callable(getattr(d, "date")):
        return d.date()
    try:
        return datetime.fromisoformat(str(d)[:10]).date()
    except (TypeError, ValueError):
        return None


def _stat_range_contains_rebalance_day(rb_day, sd: str, ed: str | None) -> bool:
    """自然日 rb_day 是否落在统计区间 [sd, ed] 内（ed 空视为无上限）。"""
    d0 = _to_date_key(rb_day)
    if d0 is None:
        return False
    try:
        lo = datetime.fromisoformat(sd[:10]).date()
    except (TypeError, ValueError):
        return False
    if d0 < lo:
        return False
    es = str(ed or "").strip()
    if not es:
        return True
    try:
        hi = datetime.fromisoformat(es[:10]).date()
    except (TypeError, ValueError):
        return True
    return d0 <= hi


def _declared_rebalance_calendar_days(db: Session, strategy_id: str) -> set[str]:
    """策略在持仓/净值中声明过的调仓自然日（含非交易日）；用于判断起点是否按调仓期锚定。"""
    rows = db.execute(
        text(
            """
            SELECT DISTINCT rebalance_date AS d
            FROM strategy_positions
            WHERE strategy_id=:sid AND rebalance_date IS NOT NULL
            UNION
            SELECT DISTINCT rebalance_date AS d
            FROM strategy_nav_daily
            WHERE strategy_id=:sid AND rebalance_date IS NOT NULL
            """
        ),
        {"sid": strategy_id},
    ).mappings().all()
    out: set[str] = set()
    for r in rows:
        d = r.get("d")
        if d is None:
            continue
        out.add(str(d)[:10])
    return out


def _start_date_is_declared_rebalance(db: Session, strategy_id: str, sd: str) -> bool:
    """起点自然日是否为声明的调仓日（可与净值表 rebalance_date 列不完全一致，如非交易日顺延）。"""
    s = str(sd or "").strip()[:10]
    if len(s) < 10:
        return False
    return s in _declared_rebalance_calendar_days(db, strategy_id)


def _resolve_nav_rebalance_period_key(db: Session, strategy_id: str, sd: str) -> str | None:
    """
    将声明的调仓自然日 sd 解析为净值表中的调仓期键 rebalance_date。
    - 若净值行已有 rebalance_date = sd，直接返回 sd。
    - 否则取「该策略各调仓期中 MIN(trade_date) 最早且 >= sd」的那一期（非交易日调仓顺延到之后首个交易日）。
    """
    s = str(sd or "").strip()[:10]
    if len(s) < 10:
        return None
    hit = db.execute(
        text(
            """
            SELECT 1 AS o
            FROM strategy_nav_daily
            WHERE strategy_id=:sid AND rebalance_date = :sd
            LIMIT 1
            """
        ),
        {"sid": strategy_id, "sd": s},
    ).mappings().first()
    if hit:
        return s
    try:
        c = datetime.fromisoformat(s).date()
    except (TypeError, ValueError):
        return None
    rows = db.execute(
        text(
            """
            SELECT rebalance_date, MIN(trade_date) AS mdt
            FROM strategy_nav_daily
            WHERE strategy_id=:sid AND rebalance_date IS NOT NULL
            GROUP BY rebalance_date
            ORDER BY mdt ASC
            """
        ),
        {"sid": strategy_id},
    ).mappings().all()
    best_rb = None
    best_mdt: date | None = None
    for row in rows:
        mdt = _to_date_key(row.get("mdt"))
        if mdt is None or mdt < c:
            continue
        if best_mdt is None or mdt < best_mdt:
            best_mdt = mdt
            best_rb = row.get("rebalance_date")
    if best_rb is None:
        return None
    return str(best_rb)[:10]


def _min_rebalance_date_nav(db: Session, strategy_id: str):
    return db.execute(
        text(
            """
            SELECT MIN(rebalance_date) AS m
            FROM strategy_nav_daily
            WHERE strategy_id=:sid AND rebalance_date IS NOT NULL
            """
        ),
        {"sid": strategy_id},
    ).mappings().first()


def _anchor_from_rebalance_period(
    db: Session, strategy_id: str, rebalance_d: str
) -> tuple[float | None, float | None]:
    """
    某一调仓期：取该期首条交易日行的净值；策略净值未录入(<=0)时按调仓单位净值 1；
    基准取该行或该期内最早 benchmark_nav>0。
    """
    first = db.execute(
        text(
            """
            SELECT nav_unit, benchmark_nav
            FROM strategy_nav_daily
            WHERE strategy_id=:sid AND rebalance_date = :rd
            ORDER BY trade_date ASC
            LIMIT 1
            """
        ),
        {"sid": strategy_id, "rd": rebalance_d},
    ).mappings().first()
    if not first:
        return None, None
    nu = _safe_float(first.get("nav_unit"))
    b0 = _safe_float(first.get("benchmark_nav"))
    if b0 is None or b0 <= 0:
        b2 = db.execute(
            text(
                """
                SELECT benchmark_nav
                FROM strategy_nav_daily
                WHERE strategy_id=:sid AND rebalance_date = :rd
                  AND benchmark_nav IS NOT NULL AND benchmark_nav > 0
                ORDER BY trade_date ASC
                LIMIT 1
                """
            ),
            {"sid": strategy_id, "rd": rebalance_d},
        ).mappings().first()
        if b2 and b2.get("benchmark_nav") is not None:
            try:
                b0 = float(b2["benchmark_nav"])
            except (TypeError, ValueError):
                b0 = None
        else:
            b0 = None
    if b0 is None or b0 <= 0:
        return None, None
    if nu is not None and nu > 0:
        return float(nu), float(b0)
    return 1.0, float(b0)


def _excess_anchor_nav_bench_for_range(
    db: Session, strategy_id: str, start_date: str | None, end_date: str | None
) -> tuple[float | None, float | None]:
    """
    超额/区间累计对齐用的 (策略净值, 基准净值)。
    - 无 start_date：区间内首条同时有效的策略/基准净值。
    - 有 start_date 且等于某调仓日（库中 rebalance_date）：以该调仓期首日为锚（本期、或年起点=调仓日）。
    - 否则：取 trade_date < start 的最近一条双正（按年/日历切分）。
    - 再无：若统计区间包含策略「最早调仓日」，则以最早调仓期净值为锚（首期分年）。
    - 最后：区间内首条双正。
    """
    sd = str(start_date or "").strip() or None
    ed = str(end_date or "").strip() or None
    if sd:
        if _start_date_is_declared_rebalance(db, strategy_id, sd):
            rk = _resolve_nav_rebalance_period_key(db, strategy_id, sd)
            if rk:
                an = _anchor_from_rebalance_period(db, strategy_id, rk)
                if an[0] is not None and an[1] is not None and an[0] > 0 and an[1] > 0:
                    return an

        pr = db.execute(
            text(
                """
                SELECT nav_unit, benchmark_nav
                FROM strategy_nav_daily
                WHERE strategy_id=:sid AND trade_date < :sd
                  AND nav_unit IS NOT NULL AND benchmark_nav IS NOT NULL
                  AND nav_unit > 0 AND benchmark_nav > 0
                ORDER BY trade_date DESC
                LIMIT 1
                """
            ),
            {"sid": strategy_id, "sd": sd},
        ).mappings().first()
        if pr:
            try:
                nv = float(pr["nav_unit"])
                bv = float(pr["benchmark_nav"])
                if nv > 0 and bv > 0:
                    return nv, bv
            except (TypeError, ValueError):
                pass

        mr = _min_rebalance_date_nav(db, strategy_id)
        min_rb = mr.get("m") if mr else None
        if min_rb is not None and _stat_range_contains_rebalance_day(min_rb, sd, ed):
            rb_s = str(min_rb)[:10]
            an = _anchor_from_rebalance_period(db, strategy_id, rb_s)
            if an[0] is not None and an[1] is not None and an[0] > 0 and an[1] > 0:
                return an

    ar0 = db.execute(
        text(
            """
            SELECT nav_unit, benchmark_nav
            FROM strategy_nav_daily
            WHERE strategy_id=:sid
              AND (:sd IS NULL OR trade_date >= :sd)
              AND (:ed IS NULL OR trade_date <= :ed)
              AND nav_unit IS NOT NULL AND benchmark_nav IS NOT NULL
              AND nav_unit > 0 AND benchmark_nav > 0
            ORDER BY trade_date ASC
            LIMIT 1
            """
        ),
        {"sid": strategy_id, "sd": sd, "ed": ed},
    ).mappings().first()
    if not ar0:
        return None, None
    nu_a = _safe_float(ar0.get("nav_unit"))
    try:
        bva = float(ar0.get("benchmark_nav"))
    except (TypeError, ValueError):
        bva = 0.0
    if nu_a is not None and nu_a > 0 and bva > 0:
        return float(nu_a), bva
    return None, None


def _prepend_nav_row_before_range_start(
    db: Session, strategy_id: str, start_date: str | None, chart_rows: list
) -> list:
    """按年/日历切分画图时，在序列前插入 start 之前最近一交易日。起点为调仓日时不再插入（锚在调仓期首日）。"""
    sd = str(start_date or "").strip() or None
    if not sd or not chart_rows:
        return list(chart_rows)
    if _start_date_is_declared_rebalance(db, strategy_id, sd):
        return list(chart_rows)
    pre = db.execute(
        text(
            """
            SELECT trade_date, nav_unit, daily_ret, benchmark_ret, benchmark_nav, rebalance_date
            FROM strategy_nav_daily
            WHERE strategy_id=:sid AND trade_date < :sd
            ORDER BY trade_date DESC
            LIMIT 1
            """
        ),
        {"sid": strategy_id, "sd": sd},
    ).mappings().first()
    if not pre:
        return list(chart_rows)
    try:
        t0 = chart_rows[0]["trade_date"]
        tp = pre["trade_date"]
        if hasattr(t0, "date"):
            t0 = t0.date()
        if hasattr(tp, "date"):
            tp = tp.date()
        if str(t0)[:10] == str(tp)[:10]:
            return list(chart_rows)
    except (TypeError, KeyError, ValueError, IndexError):
        return list(chart_rows)
    return [pre] + list(chart_rows)


_SUPPLEMENT_PROFILE_LABELS = ("公司简介", "产品及业务", "收入构成")
# 库表列名与历史 Excel 表头可能仍为旧称，读取时按序回退
_SUPPLEMENT_PROFILE_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "产品及业务": ("产品及业务", "主要产品及业务"),
    "收入构成": ("收入构成", "主营收入构成"),
}


def _supplement_profile_db_column(exist: set[str], logical: str) -> str | None:
    for cand in _SUPPLEMENT_PROFILE_COLUMN_ALIASES.get(logical, (logical,)):
        if cand in exist:
            return cand
    return None


def _fetch_supplement_company_profile(db: Session, stock_code_raw: str) -> dict[str, str | None]:
    """从 supplement_company_profiles 读取补充字段；返回键为当前展示用逻辑名（与前端一致）。"""
    out: dict[str, str | None] = {k: None for k in _SUPPLEMENT_PROFILE_LABELS}
    try:
        sc = normalize_code(stock_code_raw)
    except ValueError:
        return out
    sc_key = sc[:512]
    exist = list_table_columns(db, "supplement_company_profiles")
    pairs: list[tuple[str, str]] = []
    for logical in _SUPPLEMENT_PROFILE_LABELS:
        col = _supplement_profile_db_column(exist, logical)
        if col:
            pairs.append((logical, col))
    if not pairs:
        return out
    sel = ", ".join(_sql_quote_ident(col) for _, col in pairs)
    row = db.execute(
        text(
            f"""
            SELECT {sel}
            FROM supplement_company_profiles
            WHERE definition_code = :dc AND stock_code = :sc
            LIMIT 1
            """
        ),
        {"dc": CODE_COMPANY_PROFILE_EXCEL, "sc": sc_key},
    ).mappings().first()
    if not row:
        return out
    rd = dict(row)
    for logical, col in pairs:
        v = rd.get(col)
        if v is None:
            out[logical] = None
        else:
            s = str(v).strip()
            out[logical] = s if s else None
    return out


def _display_strategy_category(raw: object | None) -> str:
    s = str(raw or "").strip()
    return s if s else "其他"


def _batch_nav_last_date_stock_count(db: Session, strategy_ids: list[str]) -> dict[str, dict]:
    """净值表最后交易日 + 该日「当前调仓期」持仓股票数（与净值行 rebalance_date 一致，否则取该日最大 rebalance_date）。"""
    if not strategy_ids:
        return {}
    quoted = ",".join("'" + s.replace("'", "''") + "'" for s in strategy_ids)
    rows = db.execute(
        text(
            f"""
            SELECT n.strategy_id AS strategy_id, n.td AS last_trade_date,
                   COUNT(DISTINCT h.stock_code) AS stock_cnt
            FROM (
                SELECT nd.strategy_id, nd.trade_date AS td, nd.rebalance_date AS nav_rb
                FROM strategy_nav_daily nd
                INNER JOIN (
                    SELECT strategy_id, MAX(trade_date) AS mx
                    FROM strategy_nav_daily
                    WHERE strategy_id IN ({quoted})
                    GROUP BY strategy_id
                ) mm ON mm.strategy_id = nd.strategy_id AND nd.trade_date = mm.mx
            ) n
            LEFT JOIN strategy_holding_daily h
              ON h.strategy_id = n.strategy_id
             AND h.trade_date = n.td
             AND h.rebalance_date = COALESCE(
                    n.nav_rb,
                    (
                        SELECT MAX(x.rebalance_date)
                        FROM strategy_holding_daily x
                        WHERE x.strategy_id = n.strategy_id AND x.trade_date = n.td
                    )
                  )
            GROUP BY n.strategy_id, n.td
            """
        )
    ).mappings().all()
    out: dict[str, dict] = {}
    for r in rows:
        sid = str(r["strategy_id"])
        td = r.get("last_trade_date")
        out[sid] = {
            "last_trade_date": str(td)[:10] if td is not None else None,
            "stock_count": int(r["stock_cnt"] or 0),
        }
    return out


_NAV_LIST_SUMMARY_EMPTY: dict[str, Any] = {
    "latest_nav": None,
    "last_1d_return": None,
    "last_5d_return": None,
    "period_since_rebalance_return": None,
    "month_return": None,
    "year_return": None,
}


def _nav_list_trade_date_as_date(td: Any) -> date:
    if isinstance(td, datetime):
        return td.date()
    if isinstance(td, date):
        return td
    s = str(td)[:10]
    return date.fromisoformat(s)


def _nav_list_summary_from_desc_rows(
    rows_desc: list[Any], max_rb: date | None
) -> tuple[float, float | None, float | None, float | None, float | None, float | None] | None:
    """
    rows_desc: 同一 strategy 的 nav 行，按 trade_date 降序（与 SQL ORDER BY trade_date DESC 一致）。
    返回 (last_nav, last_1d, last_5d_ret, period_ret, month_ret, year_ret)；无数据返回 None。
    """
    if not rows_desc:
        return None
    top = rows_desc[0]
    last_nav = float(top["nav_unit"])
    last_1d_return = _safe_float(top.get("daily_ret"))

    last_td = _nav_list_trade_date_as_date(top["trade_date"])
    month_cut = last_td.replace(day=1)
    year_cut = date(last_td.year, 1, 1)

    anchor_m_nav = None
    anchor_y_nav = None
    for r in rows_desc:
        td = _nav_list_trade_date_as_date(r["trade_date"])
        if td < month_cut and anchor_m_nav is None:
            anchor_m_nav = float(r["nav_unit"])
        if td < year_cut and anchor_y_nav is None:
            anchor_y_nav = float(r["nav_unit"])
        if anchor_m_nav is not None and anchor_y_nav is not None:
            break

    dm = _nav_metric_denominator(anchor_m_nav)
    dy = _nav_metric_denominator(anchor_y_nav)
    month_ret = last_nav / dm - 1.0
    year_ret = last_nav / dy - 1.0

    last_5d_return = None
    if len(rows_desc) > 5:
        n5 = float(rows_desc[5]["nav_unit"])
        if n5 > 0:
            last_5d_return = last_nav / n5 - 1.0

    period_ret = None
    if max_rb is not None:
        first_nav_after = None
        for r in reversed(rows_desc):
            td = _nav_list_trade_date_as_date(r["trade_date"])
            if td >= max_rb:
                first_nav_after = float(r["nav_unit"])
                break
        if first_nav_after is not None and first_nav_after > 0:
            period_ret = last_nav / first_nav_after - 1.0
        elif last_nav > 0:
            period_ret = last_nav / 1.0 - 1.0

    return last_nav, last_1d_return, last_5d_return, period_ret, month_ret, year_ret


def _batch_strategy_nav_list_summaries(db: Session, strategy_ids: list[str]) -> dict[str, dict[str, Any]]:
    """
    与 _strategy_nav_list_summary 相同口径；供策略列表页使用。

    一次读取 strategy_nav_daily（无 ORDER BY，避免大结果集 filesort），在内存中按策略排序并聚合；
    max_rb 取净值行中 rebalance_date 的最大值（与导入后持仓期键一致，且避免再扫 strategy_positions）。
    """
    ids = [str(x).strip() for x in strategy_ids if str(x or "").strip()]
    out: dict[str, dict[str, Any]] = {sid: dict(_NAV_LIST_SUMMARY_EMPTY) for sid in ids}
    if not ids:
        return out
    quoted = ",".join("'" + s.replace("'", "''") + "'" for s in ids)

    nav_rows = db.execute(
        text(
            f"""
            SELECT strategy_id, trade_date, nav_unit, daily_ret, rebalance_date
            FROM strategy_nav_daily
            WHERE strategy_id IN ({quoted})
            """
        )
    ).mappings().all()

    by_sid: defaultdict[str, list[Any]] = defaultdict(list)
    max_rb_by: dict[str, date | None] = {}
    for row in nav_rows:
        sid = str(row["strategy_id"]).strip()
        by_sid[sid].append(row)
        rd = row.get("rebalance_date")
        if rd is not None:
            d = _nav_list_trade_date_as_date(rd)
            cur = max_rb_by.get(sid)
            if cur is None or d > cur:
                max_rb_by[sid] = d

    for sid in ids:
        rows = by_sid.get(sid, [])
        if not rows:
            continue
        rows.sort(key=lambda r: _nav_list_trade_date_as_date(r["trade_date"]), reverse=True)
        pack = _nav_list_summary_from_desc_rows(rows, max_rb_by.get(sid))
        if pack is None:
            continue
        last_nav, last_1d_return, last_5d_return, period_ret, month_ret, year_ret = pack
        out[sid] = {
            "latest_nav": _round_nav_unit(last_nav),
            "last_1d_return": last_1d_return,
            "last_5d_return": last_5d_return,
            "period_since_rebalance_return": period_ret,
            "month_return": month_ret,
            "year_return": year_ret,
        }
    return out


def _strategy_nav_list_summary(db: Session, strategy_id: str) -> dict:
    """最新净值、本期（最近调仓日以来）、1日/5日（净值序列相邻交易日）、本月、本年收益；口径与净值页 nav-metrics 中月/年一致。"""
    m = _batch_strategy_nav_list_summaries(db, [strategy_id])
    return m.get(str(strategy_id).strip(), dict(_NAV_LIST_SUMMARY_EMPTY))


def _require_visible_strategy(db: Session, strategy_id: str) -> dict:
    row = db.execute(
        text(
            """
            SELECT strategy_id, strategy_name, benchmark_code, benchmark_name, strategy_intro
            FROM strategy_configs
            WHERE strategy_id=:sid AND is_visible=1 AND status='enabled'
            LIMIT 1
            """
        ),
        {"sid": strategy_id},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="strategy not found")
    return dict(row)


_SCHEDULED_UPDATE_MAX_ATTEMPTS = 5
"""每日定时任务内，单次调度最多执行更新次数（含首次）；失败后间隔几秒再试，避免瞬时故障。"""
_SCHEDULED_UPDATE_RETRY_SLEEP_SEC = 8


def _scheduled_update():
    from app.db import SessionLocalFactory
    import logging

    import app.services as _svc

    log = logging.getLogger(__name__)
    if not wind_sql.use_remote_sqlserver():
        log.warning("Skip scheduled update: Wind SQL Server not configured or unavailable")
        return
    if _svc._job_running:
        log.info("Skip scheduled update: run_update already active (manual sync/update in progress)")
        return

    last_exc: BaseException | None = None
    for attempt in range(1, _SCHEDULED_UPDATE_MAX_ATTEMPTS + 1):
        db = SessionLocalFactory()
        try:
            run_update(db, "SCHEDULED", "system")
            if attempt > 1:
                log.info("Scheduled update succeeded on attempt %s/%s", attempt, _SCHEDULED_UPDATE_MAX_ATTEMPTS)
            return
        except Exception as ex:
            last_exc = ex
            log.warning(
                "Scheduled update attempt %s/%s failed: %s",
                attempt,
                _SCHEDULED_UPDATE_MAX_ATTEMPTS,
                ex,
                exc_info=attempt >= _SCHEDULED_UPDATE_MAX_ATTEMPTS,
            )
        finally:
            db.close()
        if attempt < _SCHEDULED_UPDATE_MAX_ATTEMPTS:
            time.sleep(_SCHEDULED_UPDATE_RETRY_SLEEP_SEC)

    log.error(
        "Scheduled update exhausted %s attempts; last error: %s",
        _SCHEDULED_UPDATE_MAX_ATTEMPTS,
        last_exc,
    )


@app.get("/health")
def health():
    wind_ok = wind_sql.use_remote_sqlserver()
    wind_st = wind_sql.wind_status()
    turso_url = (settings.turso_database_url or "").strip()
    db_ready = is_ready()
    err = boot_error()
    return {
        "ok": db_ready and not err and (not wind_st.get("configured") or wind_ok),
        "db_ready": db_ready,
        "db_error": err,
        "service": "strategy-showcase-python",
        "turso_configured": bool(turso_url and (settings.turso_auth_token or "").strip()),
        "wind_data_source": "sqlserver" if wind_ok else "disabled",
        "wind_sqlserver_ready": wind_ok,
        "wind_sqlserver": wind_st,
    }


@app.post("/api/auth/login")
def login(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    remember: bool = Form(True),
    device_token: str | None = Form(None),
    db: Session = Depends(get_session),
):
    login_ident = str(username or "").strip()
    ident_kind = _login_identifier_kind(login_ident)

    user = _find_user_for_login(db, login_ident)
    if user and user.get("__login_conflict__"):
        _insert_login_event(
            db,
            user_id=None,
            login_identifier=login_ident,
            login_type=ident_kind,
            result="FAIL",
            reason="identifier_conflict",
            request=request,
        )
        db.commit()
        raise HTTPException(status_code=409, detail="identifier not unique, please contact admin")
    if not user:
        _insert_login_event(
            db,
            user_id=None,
            login_identifier=login_ident,
            login_type=ident_kind,
            result="FAIL",
            reason="user_not_found",
            request=request,
        )
        db.commit()
        raise HTTPException(status_code=401, detail="用户名或密码错误")
    if norm_user_status(user.get("status")) == "disabled":
        _insert_login_event(
            db,
            user_id=int(user["id"]),
            login_identifier=login_ident,
            login_type=ident_kind,
            result="FAIL",
            reason="account_disabled",
            request=request,
        )
        db.commit()
        raise HTTPException(status_code=403, detail="account disabled")
    if norm_user_status(user.get("status")) == "locked":
        lock_to = user.get("locked_until")
        # 与北京时间 now() 比较 epoch：若 locked_until 为 timezone-aware 亦可用 timestamp 比较
        lock_active = False
        if lock_to and isinstance(lock_to, datetime):
            try:
                lock_active = lock_to.timestamp() > now().timestamp()
            except (TypeError, ValueError, OSError):
                lock_active = False
        if lock_active:
            _insert_login_event(
                db,
                user_id=int(user["id"]),
                login_identifier=login_ident,
                login_type=ident_kind,
                result="FAIL",
                reason="account_locked",
                request=request,
            )
            db.commit()
            raise HTTPException(status_code=403, detail="account locked")
        db.execute(
            text(
                "UPDATE users SET status='active', locked_until=NULL, failed_login_count=0 WHERE id=:id"
            ),
            {"id": user["id"]},
        )
        db.commit()
        user["status"] = "active"
    if str(user.get("password") or "") != str(password or ""):
        fail_cnt = int(user.get("failed_login_count") or 0) + 1
        lock_until = None
        new_status = user.get("status") or "active"
        if fail_cnt >= _LOGIN_FAIL_LOCK_THRESHOLD:
            lock_until = now_naive() + timedelta(minutes=_LOGIN_LOCK_MINUTES)
            new_status = "locked"
            fail_cnt = 0
        _insert_login_event(
            db,
            user_id=int(user["id"]),
            login_identifier=login_ident,
            login_type=ident_kind,
            result="FAIL",
            reason="bad_password",
            request=request,
        )
        db.execute(
            text(
                """
                UPDATE users
                SET failed_login_count=:c,
                    locked_until=:lu,
                    status=:st
                WHERE id=:id
                """
            ),
            {"c": fail_cnt, "lu": lock_until, "st": new_status, "id": user["id"]},
        )
        db.commit()
        if lock_until is not None:
            raise HTTPException(
                status_code=403,
                detail=f"account locked until {lock_until.strftime('%Y-%m-%d %H:%M:%S')}",
            )
        raise HTTPException(status_code=401, detail="用户名或密码错误")

    must_change = (not bool(remember)) and bool(user.get("password_is_system_generated"))
    token = create_access_token(
        user["username"],
        {"must_change_password": must_change, "client_auth": "password"},
    )
    out_device_token = _upsert_device_for_user(
        db,
        int(user["id"]),
        str(device_token or "").strip(),
        request,
        trusted=bool(remember),
    )
    db.execute(
        text(
            """
            UPDATE users
            SET failed_login_count=0,
                locked_until=NULL,
                last_login_at=:now_dt,
                last_login_ip=:ip
            WHERE id=:id
            """
        ),
        {"now_dt": now_naive(), "ip": _client_ip(request), "id": user["id"]},
    )
    _insert_login_event(
        db,
        user_id=int(user["id"]),
        login_identifier=login_ident,
        login_type=ident_kind,
        result="SUCCESS",
        reason=None,
        request=request,
    )
    db.commit()
    prow = db.execute(
        text(
            """
            SELECT nickname, contact_phone, contact_email, profile_bio,
                   password_is_system_generated
            FROM users WHERE id=:id LIMIT 1
            """
        ),
        {"id": user["id"]},
    ).mappings().first()
    pd = dict(prow) if prow else {}
    profile = {
        "id": user["id"],
        "username": user["username"],
        "role": user["role"],
        "org_id": user["org_id"],
        "must_change_password": must_change,
        "password_is_system_generated": bool(int(pd.get("password_is_system_generated") or 0)),
        "nickname": pd.get("nickname"),
        "phone": pd.get("contact_phone"),
        "email": pd.get("contact_email"),
        "bio": pd.get("profile_bio"),
    }
    return {
        "access_token": token,
        "token_type": "bearer",
        "profile": profile,
        "device_token": out_device_token,
        "must_change_password": must_change,
    }


@app.get("/api/auth/device/identify")
def identify_by_device(device_token: str, db: Session = Depends(get_session)):
    rec = _find_user_by_device_token(db, device_token)
    if not rec:
        return {"ok": False}
    if norm_user_status(rec.get("status")) in ("disabled", "locked"):
        return {"ok": False}
    return {
        "ok": True,
        "username": rec.get("username"),
        "role": rec.get("role"),
    }


@app.post("/api/auth/client-bootstrap")
def client_bootstrap(request: Request, device_token: str | None = Form(None), db: Session = Depends(get_session)):
    rec = _find_user_by_device_token(db, str(device_token or "").strip())
    if rec and norm_user_status(rec.get("status")) == "active":
        return {
            "ok": True,
            "existing": True,
            "username": rec.get("username"),
        }
    created = _create_auto_user(db)
    fresh_device_token = _upsert_device_for_user(
        db,
        int(created["id"]),
        str(device_token or "").strip(),
        request,
        trusted=True,
    )
    db.commit()
    return {
        "ok": True,
        "existing": False,
        "username": created["username"],
        "password": created["raw_password"],
        "device_token": fresh_device_token,
    }


@app.get("/api/public/client-access-mode")
def public_client_access_mode(db: Session = Depends(get_session)):
    """未鉴权：前台访客是否须先登录、是否开放自助注册。"""
    return {
        "require_login": _client_require_login_enabled(db),
        "allow_register": _client_register_allowed(db),
        "contact_enabled": _client_contact_enabled(db),
        "feedback_enabled": _client_feedback_enabled(db),
    }


@app.post("/api/auth/client-session-from-device")
def client_session_from_device(
    request: Request,
    device_token: str | None = Form(None),
    db: Session = Depends(get_session),
):
    """
    仅当后台关闭「访客须先登录」时可用：用 device_token 绑定或自动创建 viewer 并签发 JWT，
    使策略展示页在无需密码的情况下调用现有接口。
    """
    if _client_require_login_enabled(db):
        raise HTTPException(
            status_code=403,
            detail="client login gate enabled",
        )
    dt = str(device_token or "").strip()
    if not dt:
        raise HTTPException(status_code=400, detail="device_token required")

    def _finish(uid: int, uname: str, role: str, org_id: str) -> dict:
        must_change = False
        token = create_access_token(
            uname,
            {"must_change_password": must_change, "client_auth": "device_silent"},
        )
        out_device_token = _upsert_device_for_user(db, uid, dt, request, trusted=True)
        db.execute(
            text(
                """
                UPDATE users
                SET failed_login_count=0,
                    locked_until=NULL,
                    last_login_at=:now_dt,
                    last_login_ip=:ip
                WHERE id=:id
                """
            ),
            {"now_dt": now_naive(), "ip": _client_ip(request), "id": uid},
        )
        _insert_login_event(
            db,
            user_id=uid,
            login_identifier=uname,
            login_type="device_silent",
            result="SUCCESS",
            reason=None,
            request=request,
        )
        db.commit()
        prow = db.execute(
            text(
                """
                SELECT nickname, contact_phone, contact_email, profile_bio
                FROM users WHERE id=:id LIMIT 1
                """
            ),
            {"id": uid},
        ).mappings().first()
        pd = dict(prow) if prow else {}
        profile = {
            "id": uid,
            "username": uname,
            "role": role,
            "org_id": org_id,
            "must_change_password": must_change,
            "nickname": pd.get("nickname"),
            "phone": pd.get("contact_phone"),
            "email": pd.get("contact_email"),
            "bio": pd.get("profile_bio"),
        }
        return {
            "access_token": token,
            "token_type": "bearer",
            "profile": profile,
            "device_token": out_device_token,
            "must_change_password": must_change,
        }

    rec = _find_user_by_device_token(db, dt)
    if rec:
        if norm_user_status(rec.get("status")) != "active":
            raise HTTPException(status_code=403, detail="account not active")
        uid = int(rec["id"])
        row = db.execute(
            text("SELECT username, role, org_id FROM users WHERE id=:id LIMIT 1"),
            {"id": uid},
        ).mappings().first()
        if not row:
            raise HTTPException(status_code=500, detail="user missing")
        return _finish(
            uid,
            str(row["username"]),
            str(row.get("role") or "viewer"),
            str(row.get("org_id") or ""),
        )

    created = _create_auto_user(db)
    uid = int(created["id"])
    uname = str(created["username"])
    return _finish(uid, uname, "viewer", "org-client")


@app.post("/api/auth/register")
def auth_register(request: Request, payload: RegisterPayload, db: Session = Depends(get_session)):
    if not _client_register_allowed(db):
        raise HTTPException(status_code=403, detail="client registration disabled")
    email = _norm_profile_email(payload.email)
    nk = _norm_profile_nickname(payload.nickname)
    ph = _norm_profile_phone(payload.phone)
    pwd = payload.password
    _ensure_unique_identity_fields(db, email=email, phone=ph, nickname=nk)
    user_rec: dict | None = None
    for _ in range(25):
        username = _generate_auto_username()
        try:
            db.execute(
                text(
                    """
                    INSERT INTO users(
                      username, password, role, org_id, status,
                      password_is_system_generated, password_changed_at,
                      nickname, contact_phone, contact_email
                    ) VALUES (
                      :u, :p, 'viewer', 'org-client', 'active', 0, datetime('now', '+8 hours'),
                      :nk, :ph, :em
                    )
                    """
                ),
                {"u": username, "p": pwd, "nk": nk, "ph": ph, "em": email},
            )
            db.commit()
            row = db.execute(
                text("SELECT id, username, role, org_id FROM users WHERE username=:u LIMIT 1"),
                {"u": username},
            ).mappings().first()
            user_rec = dict(row or {})
            break
        except HTTPException:
            db.rollback()
            raise
        except Exception as e:
            db.rollback()
            msg = str(e)
            if "Duplicate" in msg and "username" in msg:
                continue
            if "contact_email" in msg:
                raise HTTPException(status_code=400, detail="email already exists")
            if "contact_phone" in msg:
                raise HTTPException(status_code=400, detail="phone already exists")
            if "nickname" in msg:
                raise HTTPException(status_code=400, detail="nickname already exists")
            continue
    if not user_rec:
        raise HTTPException(status_code=500, detail="registration failed, retry later")
    must_change = False
    access_token = create_access_token(
        user_rec["username"],
        {"must_change_password": must_change, "client_auth": "password"},
    )
    out_device = _upsert_device_for_user(
        db,
        int(user_rec["id"]),
        str(payload.device_token or "").strip(),
        request,
        trusted=True,
    )
    db.execute(
        text(
            """
            UPDATE users
            SET failed_login_count=0,
                locked_until=NULL,
                last_login_at=:now_dt,
                last_login_ip=:ip
            WHERE id=:id
            """
        ),
        {"now_dt": now_naive(), "ip": _client_ip(request), "id": user_rec["id"]},
    )
    db.commit()
    prow = db.execute(
        text(
            """
            SELECT nickname, contact_phone, contact_email, profile_bio
            FROM users WHERE id=:id LIMIT 1
            """
        ),
        {"id": user_rec["id"]},
    ).mappings().first()
    pd = dict(prow) if prow else {}
    profile = {
        "id": user_rec["id"],
        "username": user_rec["username"],
        "role": user_rec["role"],
        "org_id": user_rec["org_id"],
        "must_change_password": must_change,
        "password_is_system_generated": False,
        "nickname": pd.get("nickname"),
        "phone": pd.get("contact_phone"),
        "email": pd.get("contact_email"),
        "bio": pd.get("profile_bio"),
    }
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "profile": profile,
        "device_token": out_device,
        "must_change_password": must_change,
    }


@app.post("/api/auth/forgot-password")
def auth_forgot_password(request: Request, payload: ForgotPasswordPayload, db: Session = Depends(get_session)):
    _forgot_pw_rate_check(_client_ip(request))
    username = str(payload.username or "").strip()
    email_try = str(payload.email or "").strip()
    generic = {
        "ok": True,
        "message": "若用户名与预留邮箱一致，系统将发送重置邮件，请检查收件箱与垃圾箱。",
    }
    if not username or not email_try:
        return generic
    try:
        email_norm = _norm_profile_email(email_try)
    except HTTPException:
        return generic
    row = db.execute(
        text(
            """
            SELECT id, username, contact_email FROM users
            WHERE username=:u AND status='active'
            LIMIT 1
            """
        ),
        {"u": username},
    ).mappings().first()
    if not row:
        return generic
    db_em = row.get("contact_email")
    if db_em is None or str(db_em).strip().lower() != email_norm.lower():
        return generic
    raw_token = secrets.token_urlsafe(32)
    th = hashlib.sha256(raw_token.encode("utf-8")).hexdigest()
    exp = now_naive() + timedelta(hours=_PASSWORD_RESET_TOKEN_HOURS)
    db.execute(
        text(
            """
            INSERT INTO password_reset_tokens (user_id, token_hash, expires_at)
            VALUES (:uid, :h, :exp)
            """
        ),
        {"uid": row["id"], "h": th, "exp": exp},
    )
    db.commit()
    base = (settings.password_reset_public_base_url or "").strip().rstrip("/")
    if not base:
        base = str(request.base_url).rstrip("/")
    reset_url = f"{base}/client-reset-password?token={raw_token}"
    sent = send_password_reset_email(settings, email_norm, reset_url)
    out = dict(generic)
    if settings.password_reset_return_link_in_json:
        out["dev_reset_url"] = reset_url
    elif not sent and not (settings.smtp_host or "").strip():
        out["hint"] = "邮件服务未配置时无法自动发信，请联系管理员配置 SMTP，或使用后台重置密码。"
    return out


@app.post("/api/auth/reset-password")
def auth_reset_password(payload: ResetPasswordPayload, db: Session = Depends(get_session)):
    raw = str(payload.token or "").strip()
    np = payload.new_password
    th = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    row = db.execute(
        text(
            """
            SELECT id, user_id FROM password_reset_tokens
            WHERE token_hash=:h AND used_at IS NULL AND expires_at > datetime('now', '+8 hours')
            LIMIT 1
            """
        ),
        {"h": th},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=400, detail="链接无效或已过期")
    uid = int(row["user_id"])
    now_dt = now_naive()
    db.execute(
        text(
            """
            UPDATE users
            SET password=:p,
                password_is_system_generated=0,
                password_changed_at=:now_dt,
                updated_at=datetime('now', '+8 hours')
            WHERE id=:id
            """
        ),
        {"p": np, "now_dt": now_dt, "id": uid},
    )
    db.execute(
        text("UPDATE password_reset_tokens SET used_at=:now_dt WHERE id=:tid"),
        {"now_dt": now_dt, "tid": row["id"]},
    )
    db.execute(
        text(
            """
            UPDATE user_devices
            SET revoked_at=:now_dt
            WHERE user_id=:uid AND revoked_at IS NULL
            """
        ),
        {"now_dt": now_dt, "uid": uid},
    )
    db.commit()
    return {"ok": True}


@app.post("/api/auth/change-password")
def change_password(
    payload: dict,
    user=Depends(get_current_user),
    db: Session = Depends(get_session),
):
    old_pwd = str((payload or {}).get("old_password") or "")
    new_pwd = str((payload or {}).get("new_password") or "")
    kick_others = bool((payload or {}).get("kick_other_devices") or False)
    if len(new_pwd) < 6:
        raise HTTPException(status_code=400, detail="password must be at least 6 chars")
    row = db.execute(
        text(
            "SELECT id, password, password_is_system_generated FROM users WHERE username=:u LIMIT 1"
        ),
        {"u": user["username"]},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="user not found")
    system_gen = bool(int(row.get("password_is_system_generated") or 0))
    if not system_gen and str(row["password"] or "") != old_pwd:
        raise HTTPException(status_code=400, detail="old password incorrect")
    now_dt = now_naive()
    db.execute(
        text(
            """
            UPDATE users
            SET password=:p,
                password_is_system_generated=0,
                password_changed_at=:now_dt
            WHERE id=:id
            """
        ),
        {"p": new_pwd, "now_dt": now_dt, "id": row["id"]},
    )
    if kick_others:
        db.execute(
            text(
                """
                UPDATE user_devices
                SET revoked_at=:now_dt
                WHERE user_id=:uid
                """
            ),
            {"now_dt": now_dt, "uid": row["id"]},
        )
    db.commit()
    token = create_access_token(
        user["username"],
        {"must_change_password": False, "client_auth": "password"},
    )
    return {"ok": True, "access_token": token, "must_change_password": False}


@app.get("/api/auth/me")
def auth_me(user=Depends(get_current_user), db: Session = Depends(get_session)):
    prow = db.execute(
        text(
            """
            SELECT nickname, contact_phone, contact_email, profile_bio,
                   password_is_system_generated
            FROM users WHERE id=:id LIMIT 1
            """
        ),
        {"id": user["id"]},
    ).mappings().first()
    pd = dict(prow) if prow else {}
    return {
        "username": user.get("username"),
        "role": user.get("role"),
        "org_id": user.get("org_id"),
        "must_change_password": bool(user.get("must_change_password")),
        "password_is_system_generated": bool(int(pd.get("password_is_system_generated") or 0)),
        "nickname": pd.get("nickname"),
        "phone": pd.get("contact_phone"),
        "email": pd.get("contact_email"),
        "bio": pd.get("profile_bio"),
    }


@app.put("/api/auth/profile")
def auth_profile_put(
    payload: UserProfilePayload,
    user=Depends(get_current_user),
    db: Session = Depends(get_session),
):
    nickname = _norm_profile_nickname(payload.nickname)
    phone = _norm_profile_phone(payload.phone)
    email = _norm_profile_email(payload.email)
    bio = _norm_profile_bio(payload.bio)
    _ensure_unique_identity_fields(db, email=email, phone=phone, nickname=nickname, exclude_user_id=int(user["id"]))
    gen_row = db.execute(
        text("SELECT password_is_system_generated FROM users WHERE id=:id LIMIT 1"),
        {"id": user["id"]},
    ).mappings().first()
    sys_gen = bool(int((gen_row or {}).get("password_is_system_generated") or 0))
    new_pw = str(payload.new_password or "").strip()
    pwd_update = False
    if sys_gen and new_pw:
        if len(new_pw) < 6:
            raise HTTPException(status_code=400, detail="password must be at least 6 chars")
        pwd_update = True
    if pwd_update:
        db.execute(
            text(
                """
                UPDATE users
                SET nickname=:nk,
                    contact_phone=:ph,
                    contact_email=:em,
                    profile_bio=:bio,
                    password=:pwd,
                    password_is_system_generated=0,
                    password_changed_at=datetime('now', '+8 hours'),
                    updated_at=datetime('now', '+8 hours')
                WHERE id=:id
                """
            ),
            {
                "nk": nickname,
                "ph": phone,
                "em": email,
                "bio": bio,
                "pwd": new_pw,
                "id": user["id"],
            },
        )
    else:
        db.execute(
            text(
                """
                UPDATE users
                SET nickname=:nk,
                    contact_phone=:ph,
                    contact_email=:em,
                    profile_bio=:bio,
                    updated_at=datetime('now', '+8 hours')
                WHERE id=:id
                """
            ),
            {"nk": nickname, "ph": phone, "em": email, "bio": bio, "id": user["id"]},
        )
    db.commit()
    out: dict = {
        "ok": True,
        "nickname": nickname,
        "phone": phone,
        "email": email,
        "bio": bio,
        "password_is_system_generated": False if pwd_update else sys_gen,
    }
    if pwd_update:
        out["access_token"] = create_access_token(
            user["username"],
            {"must_change_password": False, "client_auth": "password"},
        )
    return out


@app.post("/api/client/contact")
def client_contact_submit(
    payload: ClientContactPayload,
    user=Depends(get_current_user),
    db: Session = Depends(get_session),
):
    if not _client_contact_enabled(db):
        raise HTTPException(status_code=403, detail=public_message("forbidden"))
    title, content, contact = _validated_contact_fields(payload)
    return _submit_client_message(
        db,
        kind="contact",
        title=title,
        content=content,
        contact=contact,
        user=user,
        public_guest=False,
        client_ip="",
    )


@app.post("/api/public/contact")
def public_contact_submit(
    payload: ClientContactPayload,
    request: Request,
    db: Session = Depends(get_session),
):
    if not _client_contact_enabled(db):
        raise HTTPException(status_code=403, detail=public_message("forbidden"))
    _public_contact_rate_check(_client_ip(request))
    title, content, contact = _validated_contact_fields(payload)
    return _submit_client_message(
        db,
        kind="contact",
        title=title,
        content=content,
        contact=contact,
        user=None,
        public_guest=True,
        client_ip=_client_ip(request),
    )


@app.post("/api/client/feedback")
def client_feedback_submit(
    payload: ClientContactPayload,
    user=Depends(get_current_user),
    db: Session = Depends(get_session),
):
    if not _client_feedback_enabled(db):
        raise HTTPException(status_code=403, detail=public_message("forbidden"))
    title, content, contact = _validated_contact_fields(payload)
    return _submit_client_message(
        db,
        kind="feedback",
        title=title,
        content=content,
        contact=contact,
        user=user,
        public_guest=False,
        client_ip="",
    )


@app.post("/api/public/feedback")
def public_feedback_submit(
    payload: ClientContactPayload,
    request: Request,
    db: Session = Depends(get_session),
):
    if not _client_feedback_enabled(db):
        raise HTTPException(status_code=403, detail=public_message("forbidden"))
    _public_contact_rate_check(_client_ip(request))
    title, content, contact = _validated_contact_fields(payload)
    return _submit_client_message(
        db,
        kind="feedback",
        title=title,
        content=content,
        contact=contact,
        user=None,
        public_guest=True,
        client_ip=_client_ip(request),
    )


@app.get("/api/strategies")
def list_strategies(user=Depends(get_current_user), db: Session = Depends(get_session)):
    rows = db.execute(
        text(
            """
            SELECT strategy_id, strategy_name, benchmark_code, benchmark_name, strategy_intro,
                   strategy_category, rebalance_frequency
            FROM strategy_configs
            WHERE is_visible=1 AND status='enabled'
            ORDER BY updated_at DESC
            """
        )
    ).mappings().all()
    username = user["username"]
    follow_rows = db.execute(
        text(
            """
            SELECT strategy_id FROM user_strategy_follows WHERE username=:u
            """
        ),
        {"u": username},
    ).mappings().all()
    followed_set = {str(r["strategy_id"]) for r in follow_rows}
    sids = [str(r["strategy_id"]) for r in rows]
    summaries = _batch_strategy_nav_list_summaries(db, sids)
    items = []
    for r in rows:
        d = dict(r)
        sid = d["strategy_id"]
        d["is_followed"] = sid in followed_set
        d["display_category"] = _display_strategy_category(d.get("strategy_category"))
        d.update(summaries.get(str(sid), dict(_NAV_LIST_SUMMARY_EMPTY)))
        items.append(d)
    nav_meta = _batch_nav_last_date_stock_count(db, [str(x["strategy_id"]) for x in items])
    for d in items:
        sid = str(d["strategy_id"])
        m = nav_meta.get(sid) or {}
        d["last_trade_date"] = m.get("last_trade_date")
        d["stock_count_on_last_date"] = m.get("stock_count")
    followed = [x for x in items if x["is_followed"]]
    return {"items": items, "followed": followed, "role": user["role"]}


@app.get("/api/strategies/{strategy_id}")
def get_strategy_public(
    strategy_id: str, user=Depends(get_current_user), db: Session = Depends(get_session)
):
    if not _SID_PATTERN.match(strategy_id):
        raise HTTPException(status_code=400, detail="invalid strategy_id")
    cfg = _require_visible_strategy(db, strategy_id)
    row = db.execute(
        text(
            """
            SELECT 1 FROM user_strategy_follows
            WHERE username=:u AND strategy_id=:sid
            LIMIT 1
            """
        ),
        {"u": user["username"], "sid": strategy_id},
    ).first()
    cfg["is_followed"] = bool(row)
    return cfg


@app.get("/api/stock-leaderboard")
@app.get("/api/strategies/stock-leaderboard")
def stock_leaderboard(user=Depends(get_current_user), db: Session = Depends(get_session)):
    username = user["username"]
    all_strategy_rows = db.execute(
        text(
            """
            SELECT strategy_id, strategy_name, strategy_category
            FROM strategy_configs
            WHERE is_visible=1 AND status='enabled'
            ORDER BY updated_at DESC
            """
        )
    ).mappings().all()
    all_strategy_map = {str(r["strategy_id"]): str(r["strategy_name"]) for r in all_strategy_rows}
    if not all_strategy_map:
        return {"latest_trade_date": None, "followed_top": [], "categories": [], "category_tops": {}}

    follow_rows = db.execute(
        text("SELECT strategy_id FROM user_strategy_follows WHERE username=:u"),
        {"u": username},
    ).mappings().all()
    followed_ids = [str(r["strategy_id"]) for r in follow_rows if str(r["strategy_id"]) in all_strategy_map]

    # 展示用「更新日期」：各可见策略各自最新持仓交易日的最大值（与下面排行榜口径一致）
    quoted_visible = ",".join("'" + s.replace("'", "''") + "'" for s in all_strategy_map.keys())
    td_row = db.execute(
        text(
            f"""
            SELECT MAX(z.td) AS d
            FROM (
                SELECT MAX(trade_date) AS td
                FROM strategy_holding_daily
                WHERE strategy_id IN ({quoted_visible})
                GROUP BY strategy_id
            ) z
            """
        )
    ).mappings().first()
    latest_td = td_row["d"] if td_row else None
    if latest_td is None:
        return {"latest_trade_date": None, "followed_top": [], "categories": [], "category_tops": {}}

    def _build_top(strategy_ids: list[str], lim: int) -> list[dict]:
        """每个策略：最新 trade_date + 该日 MAX(rebalance_date) 的持仓（与 strategy_holdings 默认本期一致）。"""
        if not strategy_ids:
            return []
        ph = []
        binds: dict = {}
        for i, sid in enumerate(strategy_ids):
            k = f"sid{i}"
            ph.append(f":{k}")
            binds[k] = sid
        in_clause = ",".join(ph)
        rows = db.execute(
            text(
                f"""
                SELECT h.strategy_id, h.stock_code, h.stock_name, h.last_1d_pct, h.period_return, h.ret_5d
                FROM strategy_holding_daily h
                INNER JOIN (
                    SELECT strategy_id, MAX(trade_date) AS td
                    FROM strategy_holding_daily
                    WHERE strategy_id IN ({in_clause})
                    GROUP BY strategy_id
                ) lt ON lt.strategy_id = h.strategy_id AND h.trade_date = lt.td
                INNER JOIN (
                    SELECT strategy_id, trade_date, MAX(rebalance_date) AS rb
                    FROM strategy_holding_daily
                    WHERE strategy_id IN ({in_clause})
                    GROUP BY strategy_id, trade_date
                ) lr ON lr.strategy_id = h.strategy_id
                    AND lr.trade_date = h.trade_date
                    AND h.rebalance_date = lr.rb
                WHERE h.strategy_id IN ({in_clause})
                """
            ),
            binds,
        ).mappings().all()
        bucket = {}
        for r in rows:
            code = str(r["stock_code"] or "").strip()
            if not code:
                continue
            name = str(r.get("stock_name") or "").strip() or code
            sid = str(r["strategy_id"])
            sname = all_strategy_map.get(sid, sid)
            item = bucket.get(code)
            if not item:
                item = {
                    "stock_code": code,
                    "stock_name": name,
                    "last_1d_pct": r.get("last_1d_pct"),
                    "period_return": r.get("period_return"),
                    "ret_5d": r.get("ret_5d"),
                    "_strategies": {sname},
                    "_nav_strategy_id": sid,
                }
                bucket[code] = item
            else:
                item["_strategies"].add(sname)
                cur = item.get("period_return")
                nv = r.get("period_return")
                curf = float(cur) if cur is not None else float("-inf")
                nvf = float(nv) if nv is not None else float("-inf")
                if nvf > curf:
                    item["last_1d_pct"] = r.get("last_1d_pct")
                    item["period_return"] = r.get("period_return")
                    item["ret_5d"] = r.get("ret_5d")
                    item["_nav_strategy_id"] = sid
                    if r.get("stock_name"):
                        item["stock_name"] = str(r.get("stock_name"))
        items = list(bucket.values())
        items.sort(key=lambda x: (float(x["period_return"]) if x["period_return"] is not None else float("-inf")), reverse=True)
        out = []
        for x in items[:lim]:
            out.append(
                {
                    "stock_code": x["stock_code"],
                    "stock_name": x["stock_name"],
                    "last_1d_pct": x["last_1d_pct"],
                    "period_return": x["period_return"],
                    "ret_5d": x["ret_5d"],
                    "holding_strategies": ",".join(sorted(x["_strategies"])),
                    "nav_strategy_id": x.get("_nav_strategy_id") or "",
                }
            )
        return out

    by_category: dict[str, list[str]] = defaultdict(list)
    for r in all_strategy_rows:
        sid = str(r["strategy_id"])
        cat = _display_strategy_category(r.get("strategy_category"))
        by_category[cat].append(sid)
    category_order = sorted(by_category.keys(), key=lambda x: (x == "其他", x))
    category_tops: dict[str, list[dict]] = {}
    for cat in category_order:
        category_tops[cat] = _build_top(by_category[cat], 10)

    return {
        "latest_trade_date": str(latest_td),
        "followed_top": _build_top(followed_ids, 10),
        "categories": category_order,
        "category_tops": category_tops,
    }


@app.post("/api/strategies/{strategy_id}/follow")
def follow_strategy(
    strategy_id: str, user=Depends(get_current_user), db: Session = Depends(get_session)
):
    if not _SID_PATTERN.match(strategy_id):
        raise HTTPException(status_code=400, detail="invalid strategy_id")
    _require_visible_strategy(db, strategy_id)
    db.execute(
        text(
            """
            INSERT OR IGNORE INTO user_strategy_follows (username, strategy_id)
            VALUES (:u, :sid)
            """
        ),
        {"u": user["username"], "sid": strategy_id},
    )
    db.commit()
    return {"ok": True, "is_followed": True}


@app.delete("/api/strategies/{strategy_id}/follow")
def unfollow_strategy(
    strategy_id: str, user=Depends(get_current_user), db: Session = Depends(get_session)
):
    if not _SID_PATTERN.match(strategy_id):
        raise HTTPException(status_code=400, detail="invalid strategy_id")
    db.execute(
        text(
            """
            DELETE FROM user_strategy_follows
            WHERE username=:u AND strategy_id=:sid
            """
        ),
        {"u": user["username"], "sid": strategy_id},
    )
    db.commit()
    return {"ok": True, "is_followed": False}


@app.get("/api/strategies/{strategy_id}/holdings")
def strategy_holdings(
    strategy_id: str,
    page: int = 1,
    page_size: int = 50,
    rebalance_date: str | None = None,
    all_rows: int = 0,
    user=Depends(get_current_user),
    db: Session = Depends(get_session),
):
    _ = user
    if int(all_rows or 0) != 1 and page_size not in (20, 50, 100):
        raise HTTPException(status_code=400, detail="page_size must be 20, 50, or 100")

    latest_td_row = db.execute(
        text("SELECT MAX(trade_date) AS d FROM strategy_holding_daily WHERE strategy_id=:sid"),
        {"sid": strategy_id},
    ).mappings().first()
    latest_trade_date = latest_td_row["d"] if latest_td_row else None
    if latest_trade_date is None:
        return {
            "page": page,
            "page_size": page_size,
            "total": 0,
            "meta": {
                "latest_trade_date": None,
                "rebalance_periods": 0,
                "current_rebalance_date": None,
                "wind_data_source": "sqlserver",
            },
            "items": [],
        }

    selected_rb = rebalance_date.strip() if rebalance_date else None
    if selected_rb == "":
        selected_rb = None
    if selected_rb is None:
        rb_row = db.execute(
            text(
                """
                SELECT MAX(rebalance_date) AS rb
                FROM strategy_holding_daily
                WHERE strategy_id=:sid AND trade_date=:td
                """
            ),
            {"sid": strategy_id, "td": latest_trade_date},
        ).mappings().first()
        selected_rb = str(rb_row["rb"]) if rb_row and rb_row.get("rb") is not None else None

    total = db.execute(
        text(
            """
            SELECT COUNT(*) AS c FROM strategy_holding_daily h
            WHERE h.strategy_id=:sid
              AND h.trade_date=:td
              AND (:rb IS NULL OR h.rebalance_date=:rb)
            """
        ),
        {"sid": strategy_id, "td": latest_trade_date, "rb": selected_rb},
    ).mappings().first()["c"]
    meta_row = db.execute(
        text(
            """
            SELECT
              MAX(trade_date) AS latest_trade_date,
              COUNT(DISTINCT rebalance_date) AS rebalance_periods
            FROM strategy_holding_daily h
            WHERE h.strategy_id=:sid
              AND h.trade_date=:td
            """
        ),
        {"sid": strategy_id, "td": latest_trade_date},
    ).mappings().first()
    if int(all_rows or 0) == 1:
        rows = db.execute(
            text(
                """
                SELECT
                  trade_date, stock_code, stock_name, period_weight, latest_weight, latest_price,
                  last_1d_pct, period_return, ret_5d, ret_20d, ret_60d, ret_ytd,
                  market_cap, industry_name, pe, pb, rebalance_date
                FROM strategy_holding_daily
                WHERE strategy_id=:sid
                  AND trade_date=:td
                  AND (:rb IS NULL OR rebalance_date=:rb)
                ORDER BY latest_weight DESC, stock_code
                """
            ),
            {"sid": strategy_id, "td": latest_trade_date, "rb": selected_rb},
        ).mappings().all()
    else:
        offset = (page - 1) * page_size
        rows = db.execute(
            text(
                """
                SELECT
                  trade_date, stock_code, stock_name, period_weight, latest_weight, latest_price,
                  last_1d_pct, period_return, ret_5d, ret_20d, ret_60d, ret_ytd,
                  market_cap, industry_name, pe, pb, rebalance_date
                FROM strategy_holding_daily
                WHERE strategy_id=:sid
                  AND trade_date=:td
                  AND (:rb IS NULL OR rebalance_date=:rb)
                ORDER BY latest_weight DESC, stock_code
                LIMIT :limit OFFSET :offset
                """
            ),
            {"sid": strategy_id, "td": latest_trade_date, "rb": selected_rb, "limit": page_size, "offset": offset},
        ).mappings().all()
    return {
        "page": page,
        "page_size": page_size,
        "total": int(total),
        "meta": {
            "latest_trade_date": str(meta_row["latest_trade_date"] or "") or None,
            "rebalance_periods": int(meta_row["rebalance_periods"] or 0),
            "current_rebalance_date": selected_rb,
            "wind_data_source": "sqlserver",
        },
        "items": [dict(r) for r in rows],
    }


@app.get("/api/strategies/{strategy_id}/rebalance-dates")
def strategy_rebalance_dates(
    strategy_id: str,
    latest_only: int = 0,
    user=Depends(get_current_user),
    db: Session = Depends(get_session),
):
    _ = user
    if int(latest_only or 0) == 1:
        rows = db.execute(
            text(
                """
                SELECT DISTINCT rebalance_date
                FROM strategy_holding_daily
                WHERE strategy_id=:sid
                  AND trade_date = (
                    SELECT MAX(trade_date) FROM strategy_holding_daily WHERE strategy_id=:sid
                  )
                ORDER BY rebalance_date DESC
                """
            ),
            {"sid": strategy_id},
        ).mappings().all()
    else:
        rows = db.execute(
            text(
                """
                SELECT DISTINCT rebalance_date
                FROM strategy_positions
                WHERE strategy_id=:sid
                ORDER BY rebalance_date DESC
                """
            ),
            {"sid": strategy_id},
        ).mappings().all()
    return {"items": [str(r["rebalance_date"]) for r in rows]}


@app.get("/api/strategies/{strategy_id}/stocks/{stock_code}")
def strategy_stock_profile(
    strategy_id: str,
    stock_code: str,
    user=Depends(get_current_user),
    db: Session = Depends(get_session),
):
    _ = user
    code = (stock_code or "").strip()
    if not code or len(code) > 32:
        raise HTTPException(status_code=400, detail="invalid stock_code")

    latest_td_row = db.execute(
        text("SELECT MAX(trade_date) AS d FROM strategy_holding_daily WHERE strategy_id=:sid"),
        {"sid": strategy_id},
    ).mappings().first()
    latest_trade_date = latest_td_row["d"] if latest_td_row else None
    if latest_trade_date is None:
        raise HTTPException(status_code=404, detail="stock not found")

    latest = db.execute(
        text(
            """
            SELECT
              trade_date, rebalance_date, stock_code, stock_name, period_weight, latest_weight,
              latest_price, last_1d_pct, period_return, ret_5d, ret_20d, ret_60d, ret_ytd,
              market_cap, industry_name, pe, pb
            FROM strategy_holding_daily
            WHERE strategy_id=:sid AND trade_date=:td AND stock_code=:code
            LIMIT 1
            """
        ),
        {"sid": strategy_id, "td": latest_trade_date, "code": code},
    ).mappings().first()
    if not latest:
        raise HTTPException(status_code=404, detail="stock not found")

    hist = db.execute(
        text(
            """
            SELECT
              p.rebalance_date AS snapshot_date,
              p.holding_weight AS period_weight,
              d.period_return
            FROM strategy_positions p
            LEFT JOIN (
              SELECT x.rebalance_date, x.period_return
              FROM strategy_holding_daily x
              INNER JOIN (
                SELECT rebalance_date, MAX(trade_date) AS max_td
                FROM strategy_holding_daily
                WHERE strategy_id=:sid AND stock_code=:code
                GROUP BY rebalance_date
              ) m
                ON x.rebalance_date = m.rebalance_date
               AND x.trade_date = m.max_td
               AND x.strategy_id = :sid
               AND x.stock_code = :code
            ) d
              ON d.rebalance_date = p.rebalance_date
            WHERE p.strategy_id=:sid AND p.stock_code=:code
            ORDER BY p.rebalance_date DESC
            LIMIT 60
            """
        ),
        {"sid": strategy_id, "code": code},
    ).mappings().all()
    hist_items = [dict(r) for r in hist]
    company_profile = _fetch_supplement_company_profile(db, code)

    trend_payload: dict | None = None
    income_series: dict | None = None
    top10_holders: dict | None = None
    if wind_sql.use_remote_sqlserver():
        wind = None
        trend_payload = {"error": "Wind 连接未建立"}
        income_series = {"error": "Wind 连接未建立"}
        top10_holders = {"error": "Wind 连接未建立", "items": []}
        try:
            wind = wind_sql.open_wind(db)
            try:
                trend_payload = stock_trend.compute_stock_index_year_trend(wind, code)
            except Exception as e:
                trend_payload = {"error": str(e)}
            try:
                income_series = wind_income.build_income_series_for_stock(wind, code)
            except Exception as e:
                income_series = {"error": str(e)}
            try:
                top10_holders = wind_holders.fetch_top10_holders(wind, code)
            except Exception as e:
                top10_holders = {"error": str(e), "items": []}
        except Exception as e:
            trend_payload = {"error": str(e)}
            income_series = {"error": str(e)}
            top10_holders = {"error": str(e), "items": []}
        finally:
            wind_sql.close_wind(wind, db)
    else:
        trend_payload = {"error": "Wind 未配置"}
        income_series = {"error": "Wind 未配置"}
        top10_holders = {"error": "Wind 未配置", "items": []}

    return {
        "latest": dict(latest),
        "history": hist_items,
        "company_profile": company_profile,
        "trend": trend_payload,
        "income_series": income_series,
        "top10_holders": top10_holders,
    }


@app.get("/api/strategies/{strategy_id}/stocks/{stock_code}/ai-brief")
def strategy_stock_ai_brief(
    strategy_id: str,
    stock_code: str,
    user=Depends(get_current_user),
    db: Session = Depends(get_session),
):
    """个股页「AI查询信息」：证券名、近若干自然日窗口（默认 30 天）+ 摘要，含新闻与研报要点（方舟 Responses + web_search）。"""
    _ = user
    code = (stock_code or "").strip()
    if not code or len(code) > 32:
        return {"text": None, "error": "invalid stock_code", "context": None}

    row = db.execute(
        text(
            """
            SELECT stock_name
            FROM strategy_holding_daily
            WHERE strategy_id=:sid AND stock_code=:code
            ORDER BY trade_date DESC
            LIMIT 1
            """
        ),
        {"sid": strategy_id, "code": code},
    ).mappings().first()
    if not row:
        return {"text": None, "error": "未找到该证券在策略持仓中的记录", "context": None}

    name = (str(row.get("stock_name") or "").strip()) or code

    end_d = beijing_today()
    ai_brief_lookback_days = 30
    start_d = end_d - timedelta(days=ai_brief_lookback_days)
    w_start = start_d.strftime("%Y%m%d")
    w_end = end_d.strftime("%Y%m%d")

    instructions = (
        f"系统时钟（Asia/Shanghai）的「当前自然日」为 {end_d.isoformat()}，仅此日期可称为「今天」「当前」「截至今日」。\n"
        f"检索与摘要的时间窗口为最近约 {ai_brief_lookback_days} 个自然日：自 {start_d.isoformat()} 起至 {end_d.isoformat()}（含端点）。\n"
        "你必须先使用联网搜索工具（web_search）检索后再写正文；不得用训练数据中的虚构「当前真实时间」或旧年份冒充今天。\n"
        "除公告与经营信息外，须刻意检索并归纳「与公司相关的新闻报道要点」和「卖方/机构研报要点」（观点、评级、目标价等如有请注明来源与日期）。\n"
        "正文中的事实与数据须来自检索结果或可核对的公开来源，并尽量标注报道日期、报告日期或公告日期；检索未命中时如实说明，禁止编造未来财报或未披露数据。\n"
        "若检索结果与训练记忆冲突，一律以检索结果为准。"
    )
    user_prompt = (
        f"标的：{name}（证券代码上下文：{code}）\n"
        f"时间窗口：最近约 {ai_brief_lookback_days} 个自然日（自 {start_d.isoformat()} 至 {end_d.isoformat()}，上海日期）。\n"
        "请先使用联网搜索，尽量多轮、换关键词，覆盖但不限于：\n"
        f"- 公告与经营：公司名+最新公告、投资者关系、{end_d.year} 年报/季报、交易所+公司简称+公告；\n"
        "- 新闻与舆情：公司名+新闻、公司名+媒体报道、公司名+近况、公司名+舆情；\n"
        "- 研报与观点：公司名+研报、券商+公司名、卖方+公司名、公司名+评级、公司名+目标价、公司名+深度报告。\n"
        "再基于检索结果撰写中文摘要，建议分节：一、所属行业；二、经营与业务要点；三、近期新闻与舆情要点（每条尽量带日期或媒体/来源类型）；"
        "四、研报与机构观点要点（机构、结论、评级/目标价如有、报告日期）；五、风险要点；六、信息时效与「检索未命中」说明。\n"
        "不要输出「当前真实时间为某年某月」这类与上述系统当前日矛盾的表述。"
    )
    messages = [{"role": "user", "content": user_prompt}]
    text_out, err = ark_client.ark_chat_completion(
        messages, temperature=0.35, instructions=instructions
    )
    return {
        "text": text_out,
        "error": err,
        "context": {
            "server_today": end_d.isoformat(),
            "window_start": w_start,
            "window_end": w_end,
            "window_days": ai_brief_lookback_days,
        },
    }


@app.get("/api/strategies/{strategy_id}/nav")
def strategy_nav(
    strategy_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int | None = 2000,
    page: int | None = None,
    page_size: int = 20,
    chart: int = 0,
    user=Depends(get_current_user),
    db: Session = Depends(get_session),
):
    _ = user
    cfg = db.execute(
        text(
            """
            SELECT strategy_id, benchmark_code, benchmark_name
            FROM strategy_configs
            WHERE strategy_id=:sid
            LIMIT 1
            """
        ),
        {"sid": strategy_id},
    ).mappings().first()
    if not cfg:
        raise HTTPException(status_code=404, detail="strategy not found")

    excess_anchor_nav, excess_anchor_bench = _excess_anchor_nav_bench_for_range(
        db, strategy_id, start_date, end_date
    )
    nav_response_meta = {
        "strategy_id": strategy_id,
        "benchmark_code": cfg.get("benchmark_code"),
        "benchmark_name": cfg.get("benchmark_name"),
        "excess_anchor_nav": excess_anchor_nav,
        "excess_anchor_bench": excess_anchor_bench,
    }

    def _nav_item_from_row(r) -> dict:
        d = dict(r)
        nu_raw = d.get("nav_unit")
        nu = _safe_float(nu_raw)
        bn = d.get("benchmark_nav")
        excess_cum = None
        if (
            nu is not None
            and excess_anchor_nav is not None
            and excess_anchor_bench is not None
            and excess_anchor_nav > 0
            and excess_anchor_bench > 0
        ):
            try:
                bnv = float(bn)
                nuv = float(nu)
                if bnv > 0 and nuv > 0:
                    excess_cum = (nuv / excess_anchor_nav) / (bnv / excess_anchor_bench) - 1.0
            except (TypeError, ValueError):
                excess_cum = None
        dr = d.get("daily_ret")
        br = d.get("benchmark_ret")
        excess_day = None
        if dr is not None and br is not None:
            try:
                excess_day = float(dr) - float(br)
            except (TypeError, ValueError):
                excess_day = None
        d["excess_cumulative"] = excess_cum
        d["excess_daily"] = excess_day
        d["nav_unit"] = _round_nav_unit(nu_raw)
        return d

    want_chart = int(chart or 0) == 1
    use_page = page is not None
    if use_page:
        p = int(page or 1)
        ps = int(page_size or 30)
        if p < 1:
            raise HTTPException(status_code=400, detail="page must be >= 1")
        if ps < 1 or ps > 500:
            raise HTTPException(status_code=400, detail="page_size must be between 1 and 500")
        offset = (p - 1) * ps
        cnt_row = db.execute(
            text(
                """
                SELECT COUNT(*) AS c
                FROM strategy_nav_daily
                WHERE strategy_id=:sid
                  AND (:sd IS NULL OR trade_date >= :sd)
                  AND (:ed IS NULL OR trade_date <= :ed)
                """
            ),
            {"sid": strategy_id, "sd": start_date, "ed": end_date},
        ).mappings().first()
        total = int(cnt_row["c"] or 0) if cnt_row else 0
        rows = db.execute(
            text(
                """
                SELECT trade_date, nav_unit, daily_ret, benchmark_ret, benchmark_nav, rebalance_date
                FROM strategy_nav_daily
                WHERE strategy_id=:sid
                  AND (:sd IS NULL OR trade_date >= :sd)
                  AND (:ed IS NULL OR trade_date <= :ed)
                ORDER BY trade_date DESC
                LIMIT :lim OFFSET :off
                """
            ),
            {"sid": strategy_id, "sd": start_date, "ed": end_date, "lim": ps, "off": offset},
        ).mappings().all()
        chart_rows = []
        if want_chart:
            chart_rows = db.execute(
                text(
                    """
                    SELECT trade_date, nav_unit, daily_ret, benchmark_ret, benchmark_nav, rebalance_date
                    FROM strategy_nav_daily
                    WHERE strategy_id=:sid
                      AND (:sd IS NULL OR trade_date >= :sd)
                      AND (:ed IS NULL OR trade_date <= :ed)
                    ORDER BY trade_date ASC
                    """
                ),
                {"sid": strategy_id, "sd": start_date, "ed": end_date},
            ).mappings().all()
            chart_rows = _prepend_nav_row_before_range_start(db, strategy_id, start_date, chart_rows)
        out = {
            "items": [_nav_item_from_row(r) for r in rows],
            "total": total,
            "page": p,
            "page_size": ps,
            "meta": nav_response_meta,
        }
        if want_chart:
            chart_series = [_nav_item_from_row(r) for r in chart_rows]
            # 前端曾用 chart_series || items；空数组为真值会导致无图。若全量查询异常为空则退回本页数据。
            if not chart_series and rows and total > 0:
                chart_series = [_nav_item_from_row(r) for r in rows]
            out["chart_series"] = chart_series
        return out

    lim = int(limit if limit is not None else 2000)
    if lim < 1 or lim > 10000:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 10000")
    rows = db.execute(
        text(
            """
            SELECT trade_date, nav_unit, daily_ret, benchmark_ret, benchmark_nav, rebalance_date
            FROM strategy_nav_daily
            WHERE strategy_id=:sid
              AND (:sd IS NULL OR trade_date >= :sd)
              AND (:ed IS NULL OR trade_date <= :ed)
            ORDER BY trade_date DESC
            LIMIT :limit
            """
        ),
        {"sid": strategy_id, "sd": start_date, "ed": end_date, "limit": lim},
    ).mappings().all()
    items = [_nav_item_from_row(r) for r in rows]
    out = {
        "items": items,
        "total": len(items),
        "meta": nav_response_meta,
    }
    if want_chart:
        chart_rows = db.execute(
            text(
                """
                SELECT trade_date, nav_unit, daily_ret, benchmark_ret, benchmark_nav, rebalance_date
                FROM strategy_nav_daily
                WHERE strategy_id=:sid
                  AND (:sd IS NULL OR trade_date >= :sd)
                  AND (:ed IS NULL OR trade_date <= :ed)
                ORDER BY trade_date ASC
                LIMIT :limit
                """
            ),
            {"sid": strategy_id, "sd": start_date, "ed": end_date, "limit": lim},
        ).mappings().all()
        chart_rows = _prepend_nav_row_before_range_start(db, strategy_id, start_date, chart_rows)
        out["chart_series"] = [_nav_item_from_row(r) for r in chart_rows]
    return out


@app.get("/api/strategies/{strategy_id}/nav-metrics")
def strategy_nav_metrics(
    strategy_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
    user=Depends(get_current_user),
    db: Session = Depends(get_session),
):
    _ = user

    def _to_date(v) -> date | None:
        if v is None:
            return None
        if isinstance(v, date):
            return v
        s = str(v).strip()
        if not s:
            return None
        return datetime.fromisoformat(s[:10]).date()

    rows = db.execute(
        text(
            """
            SELECT trade_date, nav_unit, daily_ret, benchmark_ret, benchmark_nav
            FROM strategy_nav_daily
            WHERE strategy_id=:sid
              AND (:sd IS NULL OR trade_date >= :sd)
              AND (:ed IS NULL OR trade_date <= :ed)
            ORDER BY trade_date ASC
            """
        ),
        {"sid": strategy_id, "sd": start_date, "ed": end_date},
    ).mappings().all()
    if not rows:
        znull = {
            "cum_return": 0.0,
            "annual_return": None,
            "annual_volatility": None,
            "max_drawdown": None,
            "week_return": None,
            "month_return": None,
            "year_return": None,
            "win_rate": None,
            "sharpe": None,
            "calmar": None,
            "excess_cum_return": None,
            "information_ratio": None,
            "tracking_error": None,
            "annual_excess_return": None,
        }
        return {"ok": True, "items_count": 0, "metrics": znull}

    ds = [dict(r) for r in rows]
    first = ds[0]
    last = ds[-1]
    first_nav = float(first["nav_unit"]) if first.get("nav_unit") is not None else None
    last_nav = float(last["nav_unit"]) if last.get("nav_unit") is not None else None
    n0_ex, b0_ex = _excess_anchor_nav_bench_for_range(db, strategy_id, start_date, end_date)
    cum_anchor_nav = first_nav
    if n0_ex is not None and n0_ex > 0:
        cum_anchor_nav = float(n0_ex)
    n_days = len(ds)

    daily_rets = [float(x["daily_ret"]) for x in ds if x.get("daily_ret") is not None]
    daily_excess = [
        float(x["daily_ret"]) - float(x["benchmark_ret"])
        for x in ds
        if x.get("daily_ret") is not None and x.get("benchmark_ret") is not None
    ]
    cum_ret = None
    if last_nav is not None and last_nav > 0:
        cum_ret = last_nav / _nav_metric_denominator(cum_anchor_nav) - 1.0

    ann_ret = None
    periods = max(1, n_days - 1)
    if cum_ret is not None and periods > 0 and 1.0 + cum_ret > 0:
        ann_ret = (1.0 + cum_ret) ** (252.0 / periods) - 1.0

    ann_vol = None
    if len(daily_rets) >= 2:
        sd = statistics.stdev(daily_rets)
        ann_vol = sd * math.sqrt(252.0)

    max_dd = None
    peak = None
    mdd = 0.0
    for x in ds:
        nv = x.get("nav_unit")
        if nv is None:
            continue
        v = float(nv)
        if peak is None or v > peak:
            peak = v
        if peak and peak > 0:
            dd = v / peak - 1.0
            if dd < mdd:
                mdd = dd
    if peak is not None:
        max_dd = mdd

    rf = 0.0
    sharpe = None
    if ann_ret is not None and ann_vol is not None and ann_vol > 0:
        sharpe = (ann_ret - rf) / ann_vol

    calmar = None
    if ann_ret is not None and max_dd is not None and abs(max_dd) > 0:
        calmar = ann_ret / abs(max_dd)

    excess_cum = None
    end_idx = None
    for i in range(len(ds) - 1, -1, -1):
        x = ds[i]
        nv = x.get("nav_unit")
        bv = x.get("benchmark_nav")
        if nv is None or bv is None:
            continue
        nvv = float(nv)
        bvv = float(bv)
        if nvv > 0 and bvv > 0:
            end_idx = i
            break
    if end_idx is not None:
        n1 = float(ds[end_idx]["nav_unit"])
        b1 = float(ds[end_idx]["benchmark_nav"])
        if n0_ex is not None and b0_ex is not None and n0_ex > 0 and b0_ex > 0 and n1 > 0 and b1 > 0:
            excess_cum = (n1 / n0_ex) / (b1 / b0_ex) - 1.0

    te = None
    ann_excess = None
    ir = None
    if len(daily_excess) >= 2:
        te = statistics.stdev(daily_excess) * math.sqrt(252.0)
        ann_excess = statistics.mean(daily_excess) * 252.0
        if te > 0:
            ir = ann_excess / te

    win_rate = None
    win_day_pairs = [
        (float(x["daily_ret"]), float(x["benchmark_ret"]))
        for x in ds
        if x.get("daily_ret") is not None and x.get("benchmark_ret") is not None
    ]
    if win_day_pairs:
        win_rate = sum(1 for dr, br in win_day_pairs if dr >= br) / len(win_day_pairs)

    end_td = _to_date(last.get("trade_date"))

    def _anchor_nav(anchor_dt: date) -> float | None:
        row = db.execute(
            text(
                """
                SELECT nav_unit
                FROM strategy_nav_daily
                WHERE strategy_id=:sid
                  AND trade_date < :anchor
                ORDER BY trade_date DESC
                LIMIT 1
                """
            ),
            {"sid": strategy_id, "anchor": anchor_dt},
        ).mappings().first()
        if not row or row.get("nav_unit") is None:
            return None
        return float(row["nav_unit"])

    week_ret = None
    month_ret = None
    year_ret = None
    if end_td is not None and last_nav is not None and last_nav > 0:
        week_start = end_td - timedelta(days=end_td.weekday())
        m_start = end_td.replace(day=1)
        y_start = end_td.replace(month=1, day=1)
        base_w = _anchor_nav(week_start)
        base_m = _anchor_nav(m_start)
        base_y = _anchor_nav(y_start)
        dw = _nav_metric_denominator(base_w)
        dm = _nav_metric_denominator(base_m)
        dy = _nav_metric_denominator(base_y)
        week_ret = last_nav / dw - 1.0
        month_ret = last_nav / dm - 1.0
        year_ret = last_nav / dy - 1.0

    return {
        "ok": True,
        "items_count": n_days,
        "metrics": {
            "cum_return": cum_ret,
            "annual_return": ann_ret,
            "annual_volatility": ann_vol,
            "max_drawdown": max_dd,
            "week_return": week_ret,
            "month_return": month_ret,
            "year_return": year_ret,
            "win_rate": win_rate,
            "sharpe": sharpe,
            "calmar": calmar,
            "excess_cum_return": excess_cum,
            "information_ratio": ir,
            "tracking_error": te,
            "annual_excess_return": ann_excess,
        },
    }


@app.get("/api/strategies/{strategy_id}/nav-years")
def strategy_nav_years(
    strategy_id: str,
    user=Depends(get_current_user),
    db: Session = Depends(get_session),
):
    _ = user
    rows = db.execute(
        text(
            """
            SELECT DISTINCT YEAR(trade_date) AS y
            FROM strategy_nav_daily
            WHERE strategy_id=:sid
            ORDER BY y ASC
            """
        ),
        {"sid": strategy_id},
    ).mappings().all()
    years = [str(r["y"]) for r in rows if r.get("y") is not None]
    return {"items": years}


@app.post("/api/admin/strategies")
def upsert_strategy(
    payload: dict,
    user=Depends(require_roles("admin", "editor")),
    db: Session = Depends(get_session),
):
    _ = user
    required = [
        "strategy_id",
        "strategy_name",
        "file_name",
        "benchmark_code",
        "benchmark_name",
        "strategy_intro",
    ]
    for key in required:
        if key not in payload:
            raise HTTPException(status_code=400, detail=f"missing field: {key}")
    strategy_id = str(payload.get("strategy_id") or "").strip()
    if not strategy_id:
        raise HTTPException(status_code=400, detail="strategy_id must not be empty")
    if not _SID_PATTERN.match(strategy_id):
        raise HTTPException(
            status_code=400,
            detail="strategy_id invalid: only letters/digits/_/- and must start with letter or digit (max 64)",
        )
    normalized_file_name = _normalize_file_name(payload.get("file_name", ""))
    if not normalized_file_name:
        raise HTTPException(status_code=400, detail="file_name must not be empty")
    db.execute(
        text(
            """
            INSERT INTO strategy_configs (
              strategy_id,is_visible,strategy_name,source,remark,file_dir,file_name,weight_display_mode,
              benchmark_code,benchmark_name,strategy_intro,strategy_category,rebalance_frequency,status
            ) VALUES (
              :sid,:visible,:name,:source,:remark,:dir,:file,:mode,:bcode,:bname,:intro,:scat,:rfreq,'enabled'
            )
            ON CONFLICT(strategy_id) DO UPDATE SET
              is_visible=excluded.is_visible,
              strategy_name=excluded.strategy_name,
              source=excluded.source,
              remark=excluded.remark,
              file_dir=excluded.file_dir,
              file_name=excluded.file_name,
              weight_display_mode=excluded.weight_display_mode,
              benchmark_code=excluded.benchmark_code,
              benchmark_name=excluded.benchmark_name,
              strategy_intro=excluded.strategy_intro,
              strategy_category=excluded.strategy_category,
              rebalance_frequency=excluded.rebalance_frequency
            """
        ),
        {
            "sid": strategy_id,
            "visible": 1 if payload.get("is_visible", True) else 0,
            "name": payload["strategy_name"],
            "source": payload.get("source", ""),
            "remark": payload.get("remark"),
            "dir": _normalize_strategy_file_dir(strategy_id, payload.get("file_dir", "")),
            "file": normalized_file_name,
            "mode": _strategy_weight_display_mode_store(),
            "bcode": payload["benchmark_code"],
            "bname": payload["benchmark_name"],
            "intro": payload["strategy_intro"],
            "scat": str(payload.get("strategy_category") or "").strip(),
            "rfreq": str(payload.get("rebalance_frequency") or "").strip(),
        },
    )
    db.commit()
    return {"ok": True}


@app.post("/api/admin/strategies/normalize-upload-paths")
def admin_normalize_strategy_upload_paths(
    user=Depends(require_roles("admin", "editor")),
    db: Session = Depends(get_session),
):
    """将 file_dir 统一为 strategies/{strategy_id}（修正 CSV 误填 ./server-data 等）。"""
    _ = user
    rows = db.execute(text("SELECT strategy_id, file_dir FROM strategy_configs")).mappings().all()
    updated = 0
    for r in rows:
        sid = str(r.get("strategy_id") or "").strip()
        if not sid:
            continue
        new_dir = _normalize_strategy_file_dir(sid, r.get("file_dir"))
        old_dir = str(r.get("file_dir") or "").strip()
        if new_dir != old_dir:
            db.execute(
                text(
                    "UPDATE strategy_configs SET file_dir=:fd, updated_at=datetime('now', '+8 hours') WHERE strategy_id=:sid"
                ),
                {"fd": new_dir, "sid": sid},
            )
            updated += 1
    db.commit()
    return {"ok": True, "updated": updated}


@app.get("/api/admin/strategies")
def list_admin_strategies(user=Depends(require_roles("admin", "editor")), db: Session = Depends(get_session)):
    _ = user
    rows = db.execute(
        text(
            """
            SELECT
              strategy_id, is_visible, strategy_name, source, remark, file_dir, file_name,
              weight_display_mode, benchmark_code, benchmark_name,
              strategy_intro, strategy_category, rebalance_frequency, status, updated_at
            FROM strategy_configs
            ORDER BY updated_at DESC, strategy_id ASC
            """
        )
    ).mappings().all()
    from app.server_files import strategy_excel_file_status

    latest_rb_map = latest_rebalance_date_by_strategy(db)
    items: list[dict] = []
    for r in rows:
        d = dict(r)
        sid = str(d.get("strategy_id") or "").strip()
        st = strategy_excel_file_status(d.get("file_dir"), d.get("file_name"))
        d["file_exists"] = bool(st.get("file_exists"))
        d["file_mtime"] = st.get("file_mtime")
        d["file_resolved_path"] = st.get("file_path") or ""
        d["latest_rebalance_date"] = latest_rb_map.get(sid) or ""
        items.append(d)
    return {"items": items}


@app.get("/api/admin/strategies/export")
def export_admin_strategies(user=Depends(require_roles("admin", "editor")), db: Session = Depends(get_session)):
    _ = user
    rows = db.execute(
        text(
            """
            SELECT
              strategy_id, is_visible, strategy_name, source, remark, file_dir, file_name,
              weight_display_mode, benchmark_code, benchmark_name,
              strategy_intro, strategy_category, rebalance_frequency
            FROM strategy_configs
            ORDER BY strategy_id ASC
            """
        )
    ).mappings().all()
    cols = [
        "strategy_id",
        "is_visible",
        "strategy_name",
        "source",
        "remark",
        "file_dir",
        "file_name",
        "weight_display_mode",
        "benchmark_code",
        "benchmark_name",
        "strategy_intro",
        "strategy_category",
        "rebalance_frequency",
    ]
    sio = io.StringIO()
    writer = csv.DictWriter(sio, fieldnames=cols)
    writer.writeheader()
    for r in rows:
        d = dict(r)
        out = {k: (d.get(k) if d.get(k) is not None else "") for k in cols}
        out["is_visible"] = 1 if str(out.get("is_visible", "1")).strip() in ("1", "true", "True") else 0
        writer.writerow(out)
    data = sio.getvalue().encode("utf-8-sig")
    headers = {"Content-Disposition": 'attachment; filename="strategy_configs_template.csv"'}
    return StreamingResponse(io.BytesIO(data), media_type="text/csv; charset=utf-8", headers=headers)


@app.post("/api/admin/strategies/import")
async def import_admin_strategies(
    file: UploadFile = File(...),
    user=Depends(require_roles("admin", "editor")),
    db: Session = Depends(get_session),
):
    _ = user
    fname = str(file.filename or "").lower()
    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="empty file")

    if fname.endswith(".csv"):
        text_data = raw.decode("utf-8-sig", errors="ignore")
        reader = csv.DictReader(io.StringIO(text_data))
        rows = [dict(r) for r in reader]
    else:
        raise HTTPException(status_code=400, detail="only .csv is supported for config import")

    required = ["strategy_id", "strategy_name", "file_name", "benchmark_code", "benchmark_name", "strategy_intro"]
    if not rows:
        raise HTTPException(status_code=400, detail="no rows in file")

    def _bool01(v) -> int:
        s = str(v or "").strip().lower()
        return 0 if s in ("0", "false", "no", "n", "") else 1

    errs: list[str] = []
    upserted = 0
    for i, r in enumerate(rows, start=2):
        sid = str(r.get("strategy_id") or "").strip()
        if not sid:
            errs.append(f"line {i}: strategy_id empty")
            continue
        if not _SID_PATTERN.match(sid):
            errs.append(f"line {i}: invalid strategy_id={sid}")
            continue
        miss = [k for k in required if not str(r.get(k) or "").strip()]
        if miss:
            errs.append(f"line {i}: missing {','.join(miss)}")
            continue
        db.execute(
            text(
                """
                INSERT INTO strategy_configs (
                  strategy_id,is_visible,strategy_name,source,remark,file_dir,file_name,weight_display_mode,
                  benchmark_code,benchmark_name,strategy_intro,strategy_category,rebalance_frequency,status
                ) VALUES (
                  :sid,:visible,:name,:source,:remark,:dir,:file,:mode,:bcode,:bname,:intro,:scat,:rfreq,'enabled'
                )
                ON CONFLICT(strategy_id) DO UPDATE SET
                  is_visible=excluded.is_visible,
                  strategy_name=excluded.strategy_name,
                  source=excluded.source,
                  remark=excluded.remark,
                  file_dir=excluded.file_dir,
                  file_name=excluded.file_name,
                  weight_display_mode=excluded.weight_display_mode,
                  benchmark_code=excluded.benchmark_code,
                  benchmark_name=excluded.benchmark_name,
                  strategy_intro=excluded.strategy_intro,
                  strategy_category=excluded.strategy_category,
                  rebalance_frequency=excluded.rebalance_frequency
                """
            ),
            {
                "sid": sid,
                "visible": _bool01(r.get("is_visible", 1)),
                "name": str(r.get("strategy_name") or "").strip(),
                "source": str(r.get("source") or "").strip(),
                "remark": str(r.get("remark") or "").strip(),
                "dir": _normalize_strategy_file_dir(sid, str(r.get("file_dir") or "")),
                "file": _normalize_file_name(str(r.get("file_name") or "").strip()),
                "mode": _strategy_weight_display_mode_store(),
                "bcode": str(r.get("benchmark_code") or "").strip(),
                "bname": str(r.get("benchmark_name") or "").strip(),
                "intro": str(r.get("strategy_intro") or "").strip(),
                "scat": str(r.get("strategy_category") or "").strip(),
                "rfreq": str(r.get("rebalance_frequency") or "").strip(),
            },
        )
        upserted += 1
    if errs:
        db.rollback()
        raise HTTPException(status_code=400, detail="; ".join(errs[:20]))
    db.commit()
    return {"ok": True, "upserted": upserted}


@app.delete("/api/admin/strategies/{strategy_id}")
def delete_strategy(
    strategy_id: str,
    user=Depends(require_roles("admin", "editor")),
    db: Session = Depends(get_session),
):
    _ = user
    row = db.execute(
        text("SELECT strategy_id FROM strategy_configs WHERE strategy_id=:sid LIMIT 1"),
        {"sid": strategy_id},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail=f"strategy_id not found: {strategy_id}")
    db.execute(text("DELETE FROM user_strategy_follows WHERE strategy_id=:sid"), {"sid": strategy_id})
    db.execute(text("DELETE FROM strategy_positions WHERE strategy_id=:sid"), {"sid": strategy_id})
    db.execute(text("DELETE FROM strategy_holding_daily WHERE strategy_id=:sid"), {"sid": strategy_id})
    db.execute(text("DELETE FROM strategy_nav_daily WHERE strategy_id=:sid"), {"sid": strategy_id})
    db.execute(text("DELETE FROM strategy_configs WHERE strategy_id=:sid"), {"sid": strategy_id})
    db.commit()
    return {"ok": True, "deleted_strategy_id": strategy_id}


@app.post("/api/admin/strategies/delete")
def delete_strategies(
    payload: dict,
    user=Depends(require_roles("admin", "editor")),
    db: Session = Depends(get_session),
):
    _ = user
    ids = payload.get("strategy_ids") or []
    if not isinstance(ids, list) or not ids:
        raise HTTPException(status_code=400, detail="strategy_ids must be a non-empty list")
    clean_ids = sorted({str(x).strip() for x in ids if str(x).strip()})
    if not clean_ids:
        raise HTTPException(status_code=400, detail="no valid strategy_ids")
    quoted = ",".join("'" + s.replace("'", "''") + "'" for s in clean_ids)
    hit = db.execute(
        text(f"SELECT strategy_id FROM strategy_configs WHERE strategy_id IN ({quoted})")
    ).mappings().all()
    hit_ids = [str(r["strategy_id"]) for r in hit]
    if not hit_ids:
        return {"ok": True, "deleted_count": 0, "deleted_strategy_ids": []}
    q2 = ",".join("'" + s.replace("'", "''") + "'" for s in hit_ids)
    db.execute(text(f"DELETE FROM user_strategy_follows WHERE strategy_id IN ({q2})"))
    db.execute(text(f"DELETE FROM strategy_positions WHERE strategy_id IN ({q2})"))
    db.execute(text(f"DELETE FROM strategy_holding_daily WHERE strategy_id IN ({q2})"))
    db.execute(text(f"DELETE FROM strategy_nav_daily WHERE strategy_id IN ({q2})"))
    db.execute(text(f"DELETE FROM strategy_configs WHERE strategy_id IN ({q2})"))
    db.commit()
    return {"ok": True, "deleted_count": len(hit_ids), "deleted_strategy_ids": hit_ids}


@app.post("/api/admin/import")
def admin_import(
    background_tasks: BackgroundTasks,
    payload: dict | None = None,
    user=Depends(require_roles("admin", "editor")),
    db: Session = Depends(get_session),
):
    body = payload or {}
    selected_ids = body.get("strategy_ids") or []
    if selected_ids and not isinstance(selected_ids, list):
        raise HTTPException(status_code=400, detail="strategy_ids must be a list")
    mode = str(body.get("import_mode") or "full").strip().lower()
    if mode not in ("full", "incremental"):
        raise HTTPException(status_code=400, detail="import_mode must be full or incremental")
    ids = [str(x).strip() for x in selected_ids if str(x or "").strip()]
    resume_job_id = body.get("resume_job_id")
    if resume_job_id is not None:
        job_id = int(resume_job_id)
        job = get_strategy_import_job_row(db, job_id)
        if not job:
            raise HTTPException(status_code=404, detail="strategy import job not found")
        if not strategy_import_job_is_resumable(job):
            raise HTTPException(status_code=400, detail="该导入任务不可续传")
        db.execute(
            text(
                "UPDATE strategy_import_jobs SET status='QUEUED', message='续传已入队' WHERE id=:id"
            ),
            {"id": job_id},
        )
        db.commit()
        background_tasks.add_task(run_strategy_import_background_task, job_id, resume=True)
        return {
            "ok": True,
            "queued": True,
            "resumed": True,
            "import_job_id": job_id,
            "message": f"已续传策略导入任务 #{job_id}，请轮询 GET /api/admin/import-jobs/{job_id}",
        }
    if bool(body.get("background")):
        if not ids:
            raise HTTPException(
                status_code=400,
                detail="background import requires non-empty strategy_ids",
            )
        job_id = create_strategy_import_job(
            db,
            strategy_ids=ids,
            import_mode=mode,
            triggered_by=str(user.get("username") or "admin"),
        )
        db.commit()
        background_tasks.add_task(run_strategy_import_background_task, job_id, resume=False)
        return {
            "ok": True,
            "queued": True,
            "import_job_id": job_id,
            "message": f"已入队策略导入任务 #{job_id}，请轮询 GET /api/admin/import-jobs/{job_id}",
        }
    return import_strategy_files(db, selected_strategy_ids=ids, import_mode=mode)


@app.post("/api/admin/sync")
def admin_sync(
    background_tasks: BackgroundTasks,
    payload: dict | None = None,
    user=Depends(require_roles("admin", "editor")),
    db: Session = Depends(get_session),
):
    """
    一体化：导入 Excel → 重建净值 → 全量更新持仓快照。

    **默认异步**：立即返回 `sync_job_id`，实际工作在 BackgroundTasks 中执行，避免浏览器/代理在
    长时间无响应体时断开连接（表现为 Failed to fetch，但服务端可能仍在写库）。

    进度与结果：`GET /api/admin/sync-jobs/{sync_job_id}` 轮询字段 `status`、`stage`、`message`；
    结束后 `result_json` 与原先同步接口返回体一致。

    可选 `payload.synchronous=true`：在请求线程内跑完全程（仅适合极小数据量或排障，仍可能超时）。
    """
    selected_ids = (payload or {}).get("strategy_ids") or []
    if not isinstance(selected_ids, list) or not selected_ids:
        raise HTTPException(status_code=400, detail="strategy_ids must be a non-empty list")
    import_mode = str((payload or {}).get("import_mode") or "incremental").strip().lower()
    if import_mode not in ("full", "incremental"):
        raise HTTPException(status_code=400, detail="import_mode must be full or incremental")
    want_sync = bool((payload or {}).get("synchronous"))

    ids = [str(x).strip() for x in selected_ids if str(x or "").strip()]
    username = user["username"]
    stale_mins = max(1, int(getattr(settings, "stale_running_update_job_minutes", 240)))
    sync_stale_mins = max(stale_mins, 120)

    if want_sync:
        return execute_admin_sync_pipeline(username, ids, import_mode, sync_job_id=None)

    # 清理僵尸：长时间仍为 RUNNING 的同步任务（进程崩溃等）
    db.execute(
        text(
            """
            UPDATE admin_sync_jobs
            SET status='FAILED', finished_at=datetime('now', '+8 hours'),
                message=COALESCE(message, '') || '（僵尸RUNNING：后台未正常结束，已自动标记失败）'
            WHERE status='RUNNING'
              AND started_at IS NOT NULL
              AND started_at < datetime('now', printf('-%d minutes', :mins))
            """
        ),
        {"mins": sync_stale_mins},
    )
    # 排队过久仍未被 worker 接起的任务
    db.execute(
        text(
            """
            UPDATE admin_sync_jobs
            SET status='FAILED', finished_at=datetime('now', '+8 hours'), message='排队超时（未在 2 分钟内启动），请重试'
            WHERE status='QUEUED' AND created_at < datetime('now', '-2 minutes')
            """
        )
    )
    db.commit()

    row_run = db.execute(text("SELECT id FROM admin_sync_jobs WHERE status='RUNNING' LIMIT 1")).first()
    if row_run:
        raise HTTPException(
            status_code=409,
            detail=f"已有「导入并提取」同步任务正在执行（admin_sync_jobs id={row_run[0]}），请待完成后再试。",
        )
    row_q = db.execute(text("SELECT id FROM admin_sync_jobs WHERE status='QUEUED' LIMIT 1")).first()
    if row_q:
        raise HTTPException(
            status_code=409,
            detail=f"已有同步任务在排队（admin_sync_jobs id={row_q[0]}），请稍后再试。",
        )

    running_u = db.execute(
        text("SELECT id, started_at FROM strategy_update_jobs WHERE status='RUNNING' ORDER BY id DESC LIMIT 1")
    ).first()
    if running_u:
        rid, rst = running_u[0], running_u[1]
        raise HTTPException(
            status_code=409,
            detail=(
                f"已有进行中的数据更新任务 strategy_update_jobs id={rid}（开始于 {rst}），请待其完成后再执行同步。"
            ),
        )

    sj = json.dumps(ids, ensure_ascii=False)
    res = db.execute(
        text(
            """
            INSERT INTO admin_sync_jobs (status, stage, message, strategy_ids_json, import_mode, triggered_by)
            VALUES ('QUEUED', 'queued', :msg, :sj, :im, :by)
            """
        ),
        {"msg": "任务已入队，即将在后台执行", "sj": sj, "im": import_mode, "by": username},
    )
    job_id = int(res.lastrowid or 0)
    if not job_id:
        raise HTTPException(status_code=500, detail="创建同步任务失败")
    db.commit()

    background_tasks.add_task(run_admin_sync_background_task, job_id, username, ids, import_mode)
    return {
        "ok": True,
        "queued": True,
        "sync_job_id": job_id,
        "message": "已入队。请轮询 GET /api/admin/sync-jobs/{sync_job_id} 查看进度与最终结果。",
    }


@app.get("/api/admin/sync-jobs")
def admin_list_sync_jobs(
    limit: int = 20,
    user=Depends(require_roles("admin", "editor")),
    db: Session = Depends(get_session),
):
    _ = user
    if limit < 1 or limit > 100:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 100")
    rows = db.execute(
        text(
            """
            SELECT id, status, stage, message, import_mode, triggered_by,
                   created_at, finished_at, result_json, checkpoint_json
            FROM admin_sync_jobs
            ORDER BY id DESC
            LIMIT :lim
            """
        ),
        {"lim": int(limit)},
    ).mappings().all()
    items = []
    for r in rows:
        d = dict(r)
        try:
            rj = d.get("result_json")
            rj_obj = json.loads(rj) if isinstance(rj, str) and rj.strip() else {}
            d["resumable"] = bool(
                str(d.get("status") or "").upper() == "FAILED"
                and (rj_obj.get("resumable") or d.get("checkpoint_json"))
            )
        except json.JSONDecodeError:
            d["resumable"] = False
        items.append(d)
    return {"items": items}


@app.get("/api/admin/sync-jobs/{job_id}")
def admin_get_sync_job(
    job_id: int,
    user=Depends(require_roles("admin", "editor")),
    db: Session = Depends(get_session),
):
    _ = user
    row = db.execute(
        text(
            """
            SELECT id, status, stage, message, import_mode, triggered_by,
                   created_at, started_at, finished_at, result_json, strategy_ids_json,
                   checkpoint_json
            FROM admin_sync_jobs
            WHERE id=:id
            """
        ),
        {"id": job_id},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="sync job not found")
    d = dict(row)
    try:
        rj = d.get("result_json")
        if isinstance(rj, str) and rj.strip():
            rj_obj = json.loads(rj)
            d["resumable"] = bool(
                not rj_obj.get("ok")
                and (rj_obj.get("resumable") or d.get("checkpoint_json"))
            )
        else:
            d["resumable"] = str(d.get("status") or "").upper() == "FAILED" and bool(
                d.get("checkpoint_json")
            )
    except json.JSONDecodeError:
        d["resumable"] = False
    return d


@app.post("/api/admin/sync-jobs/{job_id}/resume")
def admin_sync_job_resume(
    job_id: int,
    background_tasks: BackgroundTasks,
    user=Depends(require_roles("admin", "editor")),
    db: Session = Depends(get_session),
):
    row = db.execute(
        text(
            """
            SELECT id, status, strategy_ids_json, import_mode, checkpoint_json
            FROM admin_sync_jobs
            WHERE id=:id
            """
        ),
        {"id": job_id},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="sync job not found")
    st = str(row.get("status") or "").upper()
    if st not in ("FAILED",):
        raise HTTPException(status_code=400, detail=f"仅 FAILED 任务可续传（当前 {st}）")
    if not row.get("checkpoint_json"):
        raise HTTPException(status_code=400, detail="无断点记录，请重新发起同步")
    row_run = db.execute(text("SELECT id FROM admin_sync_jobs WHERE status='RUNNING' LIMIT 1")).first()
    if row_run and int(row_run[0]) != job_id:
        raise HTTPException(status_code=409, detail=f"已有同步任务 RUNNING id={row_run[0]}")
    ids = json.loads(str(row.get("strategy_ids_json") or "[]"))
    import_mode = str(row.get("import_mode") or "incremental")
    username = str(user.get("username") or "admin")
    db.execute(
        text(
            "UPDATE admin_sync_jobs SET status='QUEUED', message='续传已入队', finished_at=NULL WHERE id=:id"
        ),
        {"id": job_id},
    )
    db.commit()
    background_tasks.add_task(
        run_admin_sync_background_task, job_id, username, ids, import_mode, resume=True
    )
    return {
        "ok": True,
        "queued": True,
        "resumed": True,
        "sync_job_id": job_id,
        "message": f"已续传同步任务 #{job_id}，请轮询 GET /api/admin/sync-jobs/{job_id}",
    }


@app.get("/api/admin/import-jobs/{job_id}")
def admin_get_import_job(
    job_id: int,
    user=Depends(require_roles("admin", "editor")),
    db: Session = Depends(get_session),
):
    _ = user
    job = get_strategy_import_job_row(db, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="import job not found")
    job["resumable"] = strategy_import_job_is_resumable(job)
    return job


@app.get("/api/admin/import-jobs")
def admin_list_import_jobs(
    limit: int = 30,
    user=Depends(require_roles("admin", "editor")),
    db: Session = Depends(get_session),
):
    _ = user
    if limit < 1 or limit > 100:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 100")
    rows = db.execute(
        text(
            """
            SELECT id, status, import_mode, strategy_ids_json, completed_strategy_ids_json,
                   imported_count, failed_count, message, triggered_by, created_at, finished_at
            FROM strategy_import_jobs
            ORDER BY id DESC
            LIMIT :lim
            """
        ),
        {"lim": int(limit)},
    ).mappings().all()
    items = []
    for r in rows:
        d = dict(r)
        d["resumable"] = strategy_import_job_is_resumable(d)
        items.append(d)
    return {"items": items}


@app.post("/api/admin/update")
def admin_update(
    background_tasks: BackgroundTasks,
    full_refresh: bool = False,
    payload: dict | None = None,
    user=Depends(require_roles("admin", "editor")),
    db: Session = Depends(get_session),
):
    """
    在返回前同步插入 RUNNING 任务并返回 job_id，避免前端在后台线程尚未 INSERT 时
    把历史 SUCCESS/FAILED 误判为本次更新结果；进程内互斥避免并发下重复插入。
    """
    username = user["username"]
    selected_ids = (payload or {}).get("strategy_ids") or []
    if selected_ids and not isinstance(selected_ids, list):
        raise HTTPException(status_code=400, detail="strategy_ids must be a list")

    job_id: int | None = None
    with strategy_update_mutex():
        row = db.execute(
            text("SELECT id FROM strategy_update_jobs WHERE status='RUNNING' LIMIT 1")
        ).first()
        if row:
            raise HTTPException(
                status_code=409,
                detail=f"已有进行中的更新任务 id={row[0]}，请稍候或刷新页面查看进度",
            )
        started = now_naive()
        job_id = db.execute(
            text(
                """
                INSERT INTO strategy_update_jobs(job_type,status,triggered_by,started_at)
                VALUES ('MANUAL','RUNNING',:by,:st)
                """
            ),
            {"by": username, "st": started},
        ).lastrowid
        db.commit()

    def _run_update_task(jid: int) -> None:
        from app.db import SessionLocalFactory
        import logging

        db2 = SessionLocalFactory()
        try:
            run_update(
                db2,
                "MANUAL",
                username,
                full_refresh=full_refresh,
                selected_strategy_ids=selected_ids,
                existing_job_id=jid,
            )
        except Exception:
            logging.exception("Background run_update failed")
        finally:
            db2.close()

    background_tasks.add_task(_run_update_task, job_id)
    return {
        "ok": True,
        "queued": True,
        "job_id": job_id,
        "full_refresh": full_refresh,
        "selected_count": len(selected_ids),
    }


@app.get("/api/admin/update-jobs")
def update_jobs(user=Depends(require_roles("admin", "editor")), db: Session = Depends(get_session)):
    _ = user
    rows = db.execute(
        text(
            """
            SELECT id, job_type, status, triggered_by, started_at, finished_at, message
            FROM strategy_update_jobs
            ORDER BY id DESC
            LIMIT 50
            """
        )
    ).mappings().all()
    return {"items": [dict(r) for r in rows]}


@app.post("/api/admin/smtp-test")
def admin_smtp_test(
    payload: AdminSmtpTestPayload | None = Body(default=None),
    user=Depends(require_roles("admin")),
):
    """发送一封测试邮件；仅 admin。可选 JSON：{\"to\":\"收件邮箱\"}，不传则发往 SMTP_FROM_ADDR / SMTP_USER。"""
    _ = user
    raw_to = (payload.to if payload else "") or ""
    to = str(raw_to).strip()
    if not to:
        to = (settings.smtp_from_addr or settings.smtp_user or "").strip()
    if not to:
        raise HTTPException(
            status_code=400,
            detail='请传入 JSON {"to":"收件邮箱"}，或在 .env 中配置 SMTP_FROM_ADDR / SMTP_USER',
        )
    ok, msg = smtp_send_test(settings, to)
    return {
        "ok": ok,
        "message": msg,
        "to": to,
        "smtp_host": (settings.smtp_host or "").strip(),
        "smtp_port": int(settings.smtp_port or 0),
        "smtp_use_ssl": bool(settings.smtp_use_ssl),
        "smtp_use_tls": bool(settings.smtp_use_tls),
    }


@app.get("/api/admin/data-import/definitions")
def admin_data_import_definitions(
    user=Depends(require_roles("admin", "editor")),
    db: Session = Depends(get_session),
):
    _ = user
    rows = db.execute(
        text(
            """
            SELECT code, display_name, default_file_path, description,
                   enabled, sort_order, meta_json, created_at, updated_at
            FROM data_import_definitions
            ORDER BY sort_order ASC, id ASC
            """
        )
    ).mappings().all()
    items: list[dict] = []
    for r in rows:
        d = dict(r)
        mj = d.get("meta_json")
        if isinstance(mj, str):
            try:
                d["meta_json"] = json.loads(mj) if str(mj).strip() else {}
            except json.JSONDecodeError:
                d["meta_json"] = {}
        elif mj is None:
            d["meta_json"] = {}
        raw_fp = (d.get("default_file_path") or "").strip()
        from app.server_files import file_stat, resolve_supplement_upload_path, server_upload_enabled

        server_p = resolve_supplement_upload_path(str(d.get("code") or ""))
        d["server_upload_enabled"] = server_upload_enabled()
        d["server_file"] = file_stat(server_p) if server_p else None
        if server_p and server_p.is_file():
            d["effective_default_path"] = str(server_p)
        elif not raw_fp and d.get("code") == CODE_COMPANY_PROFILE_EXCEL:
            d["effective_default_path"] = default_company_profile_xlsx_path()
        else:
            d["effective_default_path"] = raw_fp or None
        items.append(d)
    return {"items": items, "server_upload_enabled": server_upload_enabled()}


@app.get("/api/admin/data-import/server-file")
def admin_data_import_server_file(
    code: str,
    user=Depends(require_roles("admin", "editor")),
):
    _ = user
    from app.server_files import file_stat, resolve_supplement_upload_path, server_upload_enabled, upload_root

    c = (code or "").strip()
    if not c:
        raise HTTPException(status_code=400, detail="code required")
    p = resolve_supplement_upload_path(c)
    return {
        "code": c,
        "server_upload_enabled": server_upload_enabled(),
        "upload_root": str(upload_root()) if upload_root() else None,
        "file": file_stat(p) if p else None,
    }


@app.post("/api/admin/data-import/upload")
async def admin_data_import_upload(
    request: Request,
    code: str = Form(...),
    file: UploadFile = File(...),
    user=Depends(require_roles("admin", "editor")),
    db: Session = Depends(get_session),
):
    """上传补充数据文件到服务器（有则覆盖）；导入时优先读该文件。"""
    from app.server_files import upload_supplement_file

    c = (code or "").strip()
    if not c:
        raise HTTPException(status_code=400, detail="code required")
    ret = await upload_supplement_file(c, file)
    path = str(ret.get("path") or "")
    db.execute(
        text(
            """
            UPDATE data_import_definitions
            SET default_file_path=:p, updated_at=datetime('now', '+8 hours')
            WHERE code=:c
            """
        ),
        {"p": path[:1024], "c": c},
    )
    _audit_log(
        db,
        action="admin_data_import_upload",
        actor_user_id=user.get("id"),
        detail={"code": c, "path": path, "size_bytes": (ret.get("file") or {}).get("size_bytes")},
        request=request,
    )
    db.commit()
    ret["default_file_path_updated"] = True
    return ret


@app.post("/api/admin/strategies/{strategy_id}/upload-data-file")
async def admin_strategy_upload_data_file(
    strategy_id: str,
    request: Request,
    file: UploadFile = File(...),
    user=Depends(require_roles("admin", "editor")),
    db: Session = Depends(get_session),
):
    """上传策略 Excel 到服务器（有则覆盖），并更新 strategy_configs 的 file_dir/file_name。"""
    from app.server_files import upload_strategy_data_file

    sid = (strategy_id or "").strip()
    row = db.execute(
        text("SELECT strategy_id FROM strategy_configs WHERE strategy_id=:sid LIMIT 1"),
        {"sid": sid},
    ).first()
    if not row:
        raise HTTPException(status_code=404, detail="strategy not found")
    ret = await upload_strategy_data_file(sid, file)
    db.execute(
        text(
            """
            UPDATE strategy_configs
            SET file_dir=:fd, file_name=:fn, updated_at=datetime('now', '+8 hours')
            WHERE strategy_id=:sid
            """
        ),
        {
            "fd": ret["file_dir"],
            "fn": ret["file_name"],
            "sid": sid,
        },
    )
    _audit_log(
        db,
        action="admin_strategy_upload_data_file",
        actor_user_id=user.get("id"),
        detail={"strategy_id": sid, "path": ret.get("path")},
        request=request,
    )
    db.commit()
    return ret


@app.post("/api/admin/strategies/batch-upload-data-files")
async def admin_batch_strategy_upload_data_files(
    request: Request,
    files: list[UploadFile] = File(...),
    user=Depends(require_roles("admin", "editor")),
    db: Session = Depends(get_session),
):
    """
    批量上传策略 Excel：按配置表 file_name 匹配 strategy_id（或文件名主干等于 strategy_id）。
    """
    from app.server_files import batch_upload_strategy_data_files

    _ = user
    if not files:
        raise HTTPException(status_code=400, detail="no files")
    configs = [
        dict(r)
        for r in db.execute(
            text("SELECT strategy_id, file_name FROM strategy_configs")
        ).mappings().all()
    ]
    ret = await batch_upload_strategy_data_files(files, configs)
    for item in ret.get("results") or []:
        if not item.get("ok"):
            continue
        sid = str(item.get("strategy_id") or "").strip()
        if not sid:
            continue
        db.execute(
            text(
                """
                UPDATE strategy_configs
                SET file_dir=:fd, file_name=:fn, updated_at=datetime('now', '+8 hours')
                WHERE strategy_id=:sid
                """
            ),
            {
                "fd": item.get("file_dir") or _normalize_strategy_file_dir(sid, ""),
                "fn": item.get("file_name") or "",
                "sid": sid,
            },
        )
    _audit_log(
        db,
        action="admin_strategy_batch_upload_data_files",
        actor_user_id=user.get("id"),
        detail={"uploaded": ret.get("uploaded"), "failed": ret.get("failed")},
        request=request,
    )
    db.commit()
    return ret


@app.get("/api/admin/data-import/batches")
def admin_data_import_batches(
    limit: int = 40,
    user=Depends(require_roles("admin", "editor")),
    db: Session = Depends(get_session),
):
    _ = user
    if limit < 1 or limit > 200:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 200")
    stale_mins = max(5, int(getattr(settings, "supplement_import_stale_running_minutes", 12)))
    db.execute(
        text(
            """
            UPDATE data_import_batches
            SET status='FAILED',
                message=COALESCE(message, '') || '（超过5分钟仍为QUEUED：后台未启动，请重启服务后重新导入或续传）'
            WHERE status='QUEUED'
              AND created_at < datetime('now', '-5 minutes')
            """
        )
    )
    db.execute(
        text(
            """
            UPDATE data_import_batches
            SET status='FAILED',
                message=COALESCE(message, '') || '（超过 ' || CAST(:mins AS TEXT) || ' 分钟无进度更新，可能远程写入卡住；请点续传或配置 TURSO_LOCAL_REPLICA）'
            WHERE status='RUNNING'
              AND COALESCE(progress_at, created_at) < datetime('now', printf('-%d minutes', :mins))
            """
        ),
        {"mins": stale_mins},
    )
    db.commit()
    rows = db.execute(
        text(
            """
            SELECT id, definition_code, source_file_path, status, rows_ok, rows_fail,
                   rows_total, resume_from_row, message, actor_user_id, created_at, progress_at
            FROM data_import_batches
            ORDER BY id DESC
            LIMIT :lim
            """
        ),
        {"lim": int(limit)},
    ).mappings().all()
    items = []
    for x in rows:
        d = dict(x)
        d["resumable"] = batch_is_resumable(d)
        items.append(d)
    return {"items": items}


@app.get("/api/admin/data-import/batches/{batch_id}")
def admin_data_import_batch_detail(
    batch_id: int,
    user=Depends(require_roles("admin", "editor")),
    db: Session = Depends(get_session),
):
    _ = user
    batch = get_data_import_batch_row(db, batch_id)
    if not batch:
        raise HTTPException(status_code=404, detail="batch not found")
    batch["resumable"] = batch_is_resumable(batch)
    return batch


_active_data_import_batches: set[int] = set()


def _mark_data_import_batch_running(db: Session, batch_id: int, message: str) -> None:
    db.execute(
        text(
            """
            UPDATE data_import_batches
            SET status='RUNNING', message=:m, progress_at=datetime('now', '+8 hours')
            WHERE id=:id
            """
        ),
        {"m": message[:65000], "id": batch_id},
    )
    db.commit()


def _background_data_import_task(
    batch_id: int,
    code: str,
    file_path: str,
    actor_user_id: int | None,
    unique_source_column: str | None,
    unique_source_columns: list[str],
) -> None:
    from app.db import SessionLocalFactory

    if batch_id in _active_data_import_batches:
        _log.warning("data import batch %s skipped: already running in this process", batch_id)
        return
    _active_data_import_batches.add(batch_id)
    if SessionLocalFactory is None:
        _active_data_import_batches.discard(batch_id)
        raise RuntimeError("数据库未初始化（SessionLocalFactory 为空）")
    db = SessionLocalFactory()
    try:
        _mark_data_import_batch_running(db, batch_id, "后台线程已启动，正在读取文件并准备导入…")
        _log.info("data import batch %s: running import code=%s path=%s", batch_id, code, file_path)
        run_import_by_code(
            db,
            code=code,
            file_path=file_path,
            actor_user_id=actor_user_id,
            unique_source_column=unique_source_column,
            unique_source_columns=unique_source_columns,
            existing_batch_id=batch_id,
        )
        _log.info("data import batch %s: finished ok", batch_id)
    except Exception as e:
        _log.exception("data import batch %s failed", batch_id)
        try:
            batch = get_data_import_batch_row(db, batch_id)
            rr = int((batch or {}).get("resume_from_row") or 0)
            db.execute(
                text(
                    """
                    UPDATE data_import_batches
                    SET status='FAILED', message=:m
                    WHERE id=:id AND status IN ('QUEUED', 'RUNNING')
                    """
                ),
                {
                    "m": (
                        f"导入失败（可从第 {rr + 1} 行续传）：{str(e)[:64000]}"
                        if rr > 0
                        else str(e)[:65000]
                    ),
                    "id": batch_id,
                },
            )
            db.commit()
        except Exception:
            db.rollback()
        raise
    finally:
        db.close()
        _active_data_import_batches.discard(batch_id)


def _background_data_import_resume_task(batch_id: int, actor_user_id: int | None) -> None:
    from app.db import SessionLocalFactory

    if batch_id in _active_data_import_batches:
        _log.warning("data import resume batch %s skipped: already running", batch_id)
        return
    _active_data_import_batches.add(batch_id)
    if SessionLocalFactory is None:
        _active_data_import_batches.discard(batch_id)
        raise RuntimeError("数据库未初始化（SessionLocalFactory 为空）")
    db = SessionLocalFactory()
    try:
        _mark_data_import_batch_running(db, batch_id, "续传线程已启动…")
        resume_data_import_batch(db, batch_id, actor_user_id)
    except Exception as e:
        try:
            batch = get_data_import_batch_row(db, batch_id)
            rr = int((batch or {}).get("resume_from_row") or 0)
            db.execute(
                text(
                    """
                    UPDATE data_import_batches
                    SET status='FAILED', message=:m
                    WHERE id=:id AND status IN ('QUEUED', 'RUNNING')
                    """
                ),
                {
                    "m": (
                        f"续传失败（可从第 {rr + 1} 行再试）：{str(e)[:64000]}"
                        if rr > 0
                        else str(e)[:65000]
                    ),
                    "id": batch_id,
                },
            )
            db.commit()
        except Exception:
            db.rollback()
    finally:
        db.close()
        _active_data_import_batches.discard(batch_id)


@app.post("/api/admin/data-import/batches/{batch_id}/resume")
def admin_data_import_resume(
    batch_id: int,
    background_tasks: BackgroundTasks,
    user=Depends(require_roles("admin", "editor")),
    db: Session = Depends(get_session),
):
    batch = get_data_import_batch_row(db, batch_id)
    if not batch:
        raise HTTPException(status_code=404, detail="batch not found")
    if not batch_is_resumable(batch):
        raise HTTPException(status_code=400, detail="该批次不可续传（已成功、无进度或源文件不可用）")
    actor_id = int(user["id"]) if user.get("id") is not None else None
    rr = int(batch.get("resume_from_row") or 0)
    db.execute(
        text(
            """
            UPDATE data_import_batches
            SET status='QUEUED', message=:m
            WHERE id=:id
            """
        ),
        {
            "m": f"续传已入队，将从第 {rr + 1} 行继续",
            "id": batch_id,
        },
    )
    db.commit()
    spawn_daemon(
        f"data-import-resume-{batch_id}",
        _background_data_import_resume_task,
        batch_id,
        actor_id,
    )
    return {
        "ok": True,
        "queued": True,
        "resumed": True,
        "batch_id": batch_id,
        "resume_from_row": rr,
        "message": f"已续传批次 #{batch_id}，将从第 {rr + 1} 行继续",
    }


def _enqueue_data_import_batch(
    db: Session,
    *,
    code: str,
    file_path: str | None,
    actor_user_id: int | None,
) -> tuple[int, str]:
    """校验定义与本地文件，创建 QUEUED 批次，返回 (batch_id, 解析后的路径)。"""
    row = db.execute(
        text(
            """
            SELECT code, display_name, default_file_path, enabled
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

    from app.server_files import resolve_supplement_import_path
    from app.supplement_import import CODE_COMPANY_PROFILE_EXCEL, default_company_profile_xlsx_path

    fallback = default_company_profile_xlsx_path() if code == CODE_COMPANY_PROFILE_EXCEL else ""
    path = resolve_supplement_import_path(
        definition_code=code,
        explicit_path=file_path,
        default_file_path=(row.get("default_file_path") or ""),
        fallback_path=fallback,
    )
    if not path:
        raise ValueError("import file path empty")
    if not Path(path).is_file():
        raise FileNotFoundError(path)

    db.execute(
        text(
            """
            INSERT INTO data_import_batches
            (definition_code, source_file_path, status, rows_ok, rows_fail, message, actor_user_id)
            VALUES (:dc, :fp, 'QUEUED', 0, 0, :m, :uid)
            """
        ),
        {
            "dc": code,
            "fp": str(path)[:1024],
            "m": "任务已入队，后台线程即将启动",
            "uid": actor_user_id,
        },
    )
    bid_row = db.execute(text("SELECT last_insert_rowid() AS id")).mappings().first()
    batch_id = int(bid_row["id"]) if bid_row and bid_row.get("id") is not None else 0
    if not batch_id:
        raise RuntimeError("创建导入批次失败")
    return batch_id, path


@app.post("/api/admin/data-import/run")
def admin_data_import_run(
    request: Request,
    background_tasks: BackgroundTasks,
    payload: AdminDataImportRunPayload,
    user=Depends(require_roles("admin", "editor")),
    db: Session = Depends(get_session),
):
    code = (payload.code or "").strip()
    fp = (payload.file_path or "").strip() or None
    usc = [str(x).strip() for x in (payload.unique_source_columns or []) if str(x).strip()]
    ucol = (payload.unique_source_column or "").strip() or None
    actor_id = int(user["id"]) if user.get("id") is not None else None

    try:
        if payload.background:
            batch_id, resolved_path = _enqueue_data_import_batch(
                db, code=code, file_path=fp, actor_user_id=actor_id
            )
            _audit_log(
                db,
                action="admin_data_import_run",
                actor_user_id=user.get("id"),
                detail={"code": code, "file_path": resolved_path, "batch_id": batch_id, "queued": True},
                request=request,
            )
            db.commit()
            spawn_daemon(
                f"data-import-{batch_id}",
                _background_data_import_task,
                batch_id,
                code,
                resolved_path,
                actor_id,
                ucol,
                usc,
            )
            return {
                "ok": True,
                "queued": True,
                "batch_id": batch_id,
                "message": "已提交后台导入。请在本页「最近批次」查看 RUNNING/SUCCESS；完成后刷新列表。",
            }

        out = run_import_by_code(
            db,
            code=code,
            file_path=fp,
            actor_user_id=actor_id,
            unique_source_column=ucol,
            unique_source_columns=usc if usc else None,
        )
    except ImportDefinitionNotFoundError:
        raise HTTPException(status_code=404, detail="import definition not found")
    except FileNotFoundError:
        raise HTTPException(status_code=400, detail="import file not found")
    except ValueError as e:
        msg = str(e).strip()
        if msg == "import definition disabled":
            raise HTTPException(status_code=400, detail="import definition disabled")
        if msg == "unsupported import code":
            raise HTTPException(status_code=400, detail="unsupported import code")
        if msg == "import file path empty":
            raise HTTPException(status_code=400, detail="import file path empty")
        raise HTTPException(status_code=400, detail=msg or "import failed")
    _audit_log(
        db,
        action="admin_data_import_run",
        actor_user_id=user.get("id"),
        detail={
            "code": code,
            "file_path": fp,
            "unique_source_column": out.get("unique_source_column"),
            "unique_key_columns": out.get("unique_key_columns"),
            "batch_id": out.get("batch_id"),
            "rows_ok": out.get("rows_ok"),
            "rows_fail": out.get("rows_fail"),
        },
        request=request,
    )
    db.commit()
    return out


@app.get("/api/admin/users")
def admin_users_list(
    q: str | None = None,
    status_filter: str | None = None,
    user=Depends(require_roles("admin")),
    db: Session = Depends(get_session),
):
    kw = "%" + str(q or "").strip() + "%"
    rows = db.execute(
        text(
            """
            SELECT
              u.id, u.username, u.role, u.org_id, u.status,
              u.password_is_system_generated, u.password_changed_at,
              u.last_login_at, u.last_login_ip, u.created_at, u.updated_at,
              u.nickname, u.contact_phone, u.contact_email
            FROM users u
            WHERE (
              :kw='%%'
              OR u.username LIKE :kw
              OR u.nickname LIKE :kw
              OR u.contact_phone LIKE :kw
              OR u.contact_email LIKE :kw
            )
              AND (:st IS NULL OR :st='' OR u.status=:st)
            ORDER BY u.updated_at DESC, u.id DESC
            LIMIT 500
            """
        ),
        {"kw": kw, "st": status_filter},
    ).mappings().all()
    _ = user
    items = [dict(r) for r in rows]
    if items:
        ids = [int(r["id"]) for r in items]
        in_list = ",".join(str(i) for i in ids)
        crow = db.execute(
            text(
                f"""
                SELECT user_id, COUNT(*) AS page_request_count
                FROM user_access_logs
                WHERE user_id IN ({in_list})
                GROUP BY user_id
                """
            )
        ).mappings().all()
        cmap = {int(r["user_id"]): int(r.get("page_request_count") or 0) for r in crow}
        for it in items:
            it["page_request_count"] = cmap.get(int(it["id"]), 0)
    return {"items": items}


@app.get("/api/admin/access-overview")
def admin_access_overview(
    user=Depends(require_roles("admin")),
    db: Session = Depends(get_session),
):
    """首页仪表盘：用户与登录访问汇总（基于 users / user_devices / login_events）。"""
    _ = user
    urow = db.execute(
        text(
            """
            SELECT
              COUNT(*) AS total,
              SUM(CASE WHEN status='active' THEN 1 ELSE 0 END) AS active_cnt,
              SUM(CASE WHEN status='locked' THEN 1 ELSE 0 END) AS locked_cnt,
              SUM(CASE WHEN status='disabled' THEN 1 ELSE 0 END) AS disabled_cnt
            FROM users
            """
        )
    ).mappings().first()
    ud = dict(urow or {})

    role_rows = db.execute(
        text(
            """
            SELECT role, COUNT(*) AS cnt
            FROM users
            GROUP BY role
            ORDER BY cnt DESC
            """
        )
    ).mappings().all()
    by_role = [{"role": str(r.get("role") or ""), "count": int(r.get("cnt") or 0)} for r in role_rows]

    dev_row = db.execute(
        text(
            """
            SELECT COUNT(*) AS c
            FROM user_devices
            WHERE revoked_at IS NULL AND device_token_expires_at > datetime('now', '+8 hours')
            """
        )
    ).mappings().first()
    devices_active = int((dev_row or {}).get("c") or 0)

    def _login_bucket(hours: int) -> dict:
        agg = db.execute(
            text(
                f"""
                SELECT
                  SUM(CASE WHEN result='SUCCESS' THEN 1 ELSE 0 END) AS ok_n,
                  SUM(CASE WHEN result='FAIL' THEN 1 ELSE 0 END) AS fail_n
                FROM login_events
                WHERE created_at >= {sql_hours_ago(':hrs')}
                """
            ),
            {"hrs": hours},
        ).mappings().first()
        ad = dict(agg or {})
        du = db.execute(
            text(
                f"""
                SELECT COUNT(DISTINCT user_id) AS c
                FROM login_events
                WHERE result='SUCCESS'
                  AND user_id IS NOT NULL
                  AND created_at >= {sql_hours_ago(':hrs')}
                """
            ),
            {"hrs": hours},
        ).mappings().first()
        return {
            "success": int(ad.get("ok_n") or 0),
            "fail": int(ad.get("fail_n") or 0),
            "distinct_users_success": int((du or {}).get("c") or 0),
        }

    login_windows = {
        "24h": _login_bucket(24),
        "7d": _login_bucket(24 * 7),
        "30d": _login_bucket(24 * 30),
    }

    top_ip_rows = db.execute(
        text(
            """
            SELECT ip, COUNT(*) AS cnt
            FROM login_events
            WHERE result='FAIL'
              AND created_at >= datetime('now', '-24 hours')
              AND ip IS NOT NULL
              AND TRIM(ip) <> ''
            GROUP BY ip
            ORDER BY cnt DESC
            LIMIT 8
            """
        )
    ).mappings().all()
    top_fail_ips = [{"ip": str(r.get("ip") or ""), "count": int(r.get("cnt") or 0)} for r in top_ip_rows]

    reason_rows = db.execute(
        text(
            """
            SELECT reason, COUNT(*) AS cnt
            FROM login_events
            WHERE result='FAIL'
              AND created_at >= datetime('now', '-24 hours')
            GROUP BY reason
            ORDER BY cnt DESC
            LIMIT 10
            """
        )
    ).mappings().all()
    fail_reasons_24h = [
        {"reason": str(r.get("reason") or "(empty)"), "count": int(r.get("cnt") or 0)} for r in reason_rows
    ]

    usage_today = db.execute(
        text(
            """
            SELECT COALESCE(SUM(u.api_requests), 0) AS c
            FROM user_usage_daily u
            INNER JOIN users usr ON usr.id = u.user_id AND usr.role = 'viewer'
            WHERE u.usage_date = date('now')
            """
        )
    ).mappings().first()
    usage_7d = db.execute(
        text(
            """
            SELECT COALESCE(SUM(u.api_requests), 0) AS c
            FROM user_usage_daily u
            INNER JOIN users usr ON usr.id = u.user_id AND usr.role = 'viewer'
            WHERE u.usage_date >= date('now', '-6 days')
            """
        )
    ).mappings().first()
    usage_30d = db.execute(
        text(
            """
            SELECT COALESCE(SUM(u.api_requests), 0) AS c
            FROM user_usage_daily u
            INNER JOIN users usr ON usr.id = u.user_id AND usr.role = 'viewer'
            WHERE u.usage_date >= date('now', '-29 days')
            """
        )
    ).mappings().first()
    api_today = int((usage_today or {}).get("c") or 0)
    api_7d = int((usage_7d or {}).get("c") or 0)
    api_30d = int((usage_30d or {}).get("c") or 0)

    dv7 = db.execute(
        text(
            """
            SELECT COUNT(DISTINCT u.user_id) AS c
            FROM user_usage_daily u
            INNER JOIN users usr ON usr.id = u.user_id AND usr.role = 'viewer'
            WHERE u.usage_date >= date('now', '-6 days')
            """
        )
    ).mappings().first()
    dv30 = db.execute(
        text(
            """
            SELECT COUNT(DISTINCT u.user_id) AS c
            FROM user_usage_daily u
            INNER JOIN users usr ON usr.id = u.user_id AND usr.role = 'viewer'
            WHERE u.usage_date >= date('now', '-29 days')
            """
        )
    ).mappings().first()
    distinct_viewers_7d = int((dv7 or {}).get("c") or 0)
    distinct_viewers_30d = int((dv30 or {}).get("c") or 0)
    avg_req_per_active_7d = round(api_7d / distinct_viewers_7d, 2) if distinct_viewers_7d else 0.0

    lv7 = db.execute(
        text(
            """
            SELECT
              COUNT(*) AS n_ok,
              COUNT(DISTINCT le.user_id) AS n_u
            FROM login_events le
            INNER JOIN users usr ON usr.id = le.user_id AND usr.role = 'viewer'
            WHERE le.result = 'SUCCESS'
              AND le.user_id IS NOT NULL
              AND le.created_at >= datetime('now', '-7 days')
            """
        )
    ).mappings().first()
    lv7d = dict(lv7 or {})
    login_ok_7d = int(lv7d.get("n_ok") or 0)
    login_du_7d = int(lv7d.get("n_u") or 0)
    avg_login_ok_per_user_7d = round(login_ok_7d / login_du_7d, 2) if login_du_7d else 0.0

    vdev_cnt_row = db.execute(
        text(
            """
            SELECT COUNT(*) AS c
            FROM user_devices d
            INNER JOIN users u ON u.id = d.user_id AND u.role = 'viewer'
            WHERE d.revoked_at IS NULL AND d.device_token_expires_at > datetime('now', '+8 hours')
            """
        )
    ).mappings().first()
    viewer_active_devices = int((vdev_cnt_row or {}).get("c") or 0)

    idle_row = db.execute(
        text(
            f"""
            SELECT AVG({sql_timestampdiff_hours('d.last_seen_at')}) AS h
            FROM user_devices d
            INNER JOIN users u ON u.id = d.user_id AND u.role = 'viewer'
            WHERE d.revoked_at IS NULL
              AND d.device_token_expires_at > datetime('now', '+8 hours')
              AND d.last_seen_at IS NOT NULL
            """
        )
    ).mappings().first()
    idle_h = (idle_row or {}).get("h")
    try:
        viewer_devices_avg_idle_hours = round(float(idle_h), 2) if idle_h is not None else None
    except (TypeError, ValueError):
        viewer_devices_avg_idle_hours = None

    viewer_engagement = {
        "api_requests_calendar": {
            "today": api_today,
            "last_7_days": api_7d,
            "last_30_days": api_30d,
        },
        "distinct_active_viewers_by_requests": {
            "last_7_days": distinct_viewers_7d,
            "last_30_days": distinct_viewers_30d,
        },
        "avg_api_requests_per_active_viewer_7d": avg_req_per_active_7d,
        "login_success_viewer": {
            "last_7d_total": login_ok_7d,
            "last_7d_distinct_users": login_du_7d,
            "avg_success_per_user_7d": avg_login_ok_per_user_7d,
        },
        "viewer_active_devices": viewer_active_devices,
        "viewer_devices_avg_idle_hours": viewer_devices_avg_idle_hours,
        "access_token_expire_minutes": int(settings.access_token_expire_minutes),
        "notes": [
            "API 请求按自然日累计，仅统计 viewer：每次携带 JWT 并成功鉴权的接口调用记 1 次（同一页面多次接口会计多次）。",
            "前端若在请求头附带 X-Device-Token，会同步刷新对应设备的最近活跃时间，便于观察黏性。",
            "访问令牌有效期为单次登录可持有的最长时间，不等于实际在线时长；真实活跃强度可看上方 API 请求量。",
            "各角色按请求路径的访问明细在用户管理 → 用户详情中查看（表 user_access_logs）。",
        ],
    }

    return {
        "users": {
            "total": int(ud.get("total") or 0),
            "active": int(ud.get("active_cnt") or 0),
            "locked": int(ud.get("locked_cnt") or 0),
            "disabled": int(ud.get("disabled_cnt") or 0),
            "by_role": by_role,
        },
        "devices_active": devices_active,
        "login_events": login_windows,
        "fail_top_ips_24h": top_fail_ips,
        "fail_reasons_24h": fail_reasons_24h,
        "viewer_engagement": viewer_engagement,
        "client_require_login": _client_require_login_enabled(db),
        "client_allow_register": _client_register_allowed(db),
        "client_contact_enabled": _client_contact_enabled(db),
        "client_feedback_enabled": _client_feedback_enabled(db),
    }


@app.post("/api/admin/client-access-settings")
def admin_client_access_settings(
    payload: dict,
    request: Request,
    user=Depends(require_roles("admin")),
    db: Session = Depends(get_session),
):
    """前台访客：须先登录、是否开放注册等（请求体可只包含需要修改的项）。"""
    p = payload or {}
    detail_audit: dict = {}
    if "client_require_login" in p:
        raw = p.get("client_require_login")
        if isinstance(raw, str):
            require = raw.strip().lower() not in ("0", "false", "no", "off", "")
        else:
            require = bool(raw)
        _site_setting_upsert(db, _CLIENT_LOGIN_SETTING_KEY, "1" if require else "0")
        detail_audit["client_require_login"] = require
    if "client_allow_register" in p:
        raw_r = p.get("client_allow_register")
        if isinstance(raw_r, str):
            allow_reg = raw_r.strip().lower() not in ("0", "false", "no", "off", "")
        else:
            allow_reg = bool(raw_r)
        _site_setting_upsert(db, _CLIENT_REGISTER_SETTING_KEY, "1" if allow_reg else "0")
        detail_audit["client_allow_register"] = allow_reg
    if "client_contact_enabled" in p:
        raw_c = p.get("client_contact_enabled")
        if isinstance(raw_c, str):
            contact_on = raw_c.strip().lower() not in ("0", "false", "no", "off", "")
        else:
            contact_on = bool(raw_c)
        _site_setting_upsert(db, _CLIENT_CONTACT_ENABLED_KEY, "1" if contact_on else "0")
        detail_audit["client_contact_enabled"] = contact_on
    if "client_feedback_enabled" in p:
        raw_f = p.get("client_feedback_enabled")
        if isinstance(raw_f, str):
            feedback_on = raw_f.strip().lower() not in ("0", "false", "no", "off", "")
        else:
            feedback_on = bool(raw_f)
        _site_setting_upsert(db, _CLIENT_FEEDBACK_ENABLED_KEY, "1" if feedback_on else "0")
        detail_audit["client_feedback_enabled"] = feedback_on
    if detail_audit:
        _audit_log(
            db,
            action="admin_client_access_gate",
            actor_user_id=user.get("id"),
            detail=detail_audit,
            request=request,
        )
        db.commit()
    return {
        "ok": True,
        "client_require_login": _client_require_login_enabled(db),
        "client_allow_register": _client_register_allowed(db),
        "client_contact_enabled": _client_contact_enabled(db),
        "client_feedback_enabled": _client_feedback_enabled(db),
    }


@app.get("/api/admin/client-messages")
def admin_list_client_messages(
    kind: str | None = None,
    limit: int = 80,
    offset: int = 0,
    user=Depends(require_roles("admin", "editor")),
    db: Session = Depends(get_session),
):
    _ = user
    k = (kind or "").strip().lower()
    if k and k not in ("contact", "feedback"):
        raise HTTPException(status_code=400, detail="kind must be contact or feedback")
    try:
        items, total = list_client_submissions(
            db, kind=k or None, limit=limit, offset=offset
        )
    except Exception as exc:
        _log.warning("client-messages list failed: %s", exc)
        raise HTTPException(
            status_code=503,
            detail="留言表尚未初始化，请在仪表盘执行「上线环境初始化」后重试",
        ) from exc
    return {"items": items, "total": total, "limit": limit, "offset": offset}


@app.post("/api/admin/system-init")
def admin_system_init(
    payload: AdminSystemInitPayload,
    request: Request,
    user=Depends(require_roles("admin")),
    db: Session = Depends(get_session),
):
    """
    正式上线前环境对齐：在系统测试结束、准备投产时，由管理员再次校验内置超级用户 admin 密码后，
    执行与进程启动时相同的 init_database（建表/补列、INSERT IGNORE 默认配置与种子账号等）。
    用于保证生产库结构、索引与默认站点开关齐全；不主动删除已有业务数据（策略、持仓、用户业务数据等保留）。
    """
    ad_row = db.execute(
        text(
            """
            SELECT id, password
            FROM users
            WHERE username=:u AND role='admin'
            LIMIT 1
            """
        ),
        {"u": _SUPERUSER_LOGIN_USERNAME},
    ).mappings().first()
    if not ad_row:
        raise HTTPException(status_code=404, detail="superuser admin not found")
    if str(ad_row.get("password") or "") != str(payload.admin_password or "").strip():
        raise HTTPException(status_code=401, detail="superuser password incorrect")
    actor_id = user.get("id")
    init_database()
    from app.db import SessionLocalFactory as _SessionLocalFactory

    adb = _SessionLocalFactory()
    try:
        _audit_log(
            adb,
            action="admin_system_init",
            actor_user_id=int(actor_id) if actor_id is not None else None,
            detail={"trigger": "manual", "context": "production_go_live"},
            request=request,
        )
        adb.commit()
    finally:
        adb.close()
    return {
        "ok": True,
        "message": "正式上线环境初始化已完成：库结构、索引与默认配置已对齐，业务数据未做清空。",
    }


@app.get("/api/admin/users/{user_id}/detail")
def admin_user_detail(
    user_id: int,
    user=Depends(require_roles("admin")),
    db: Session = Depends(get_session),
):
    _ = user
    urow = db.execute(
        text(
            """
            SELECT
              u.id, u.username, u.role, u.org_id, u.status,
              u.password_is_system_generated, u.password_changed_at,
              u.last_login_at, u.last_login_ip, u.created_at, u.updated_at,
              u.failed_login_count, u.locked_until,
              u.nickname, u.contact_phone, u.contact_email, u.profile_bio
            FROM users u
            WHERE u.id=:id
            LIMIT 1
            """
        ),
        {"id": user_id},
    ).mappings().first()
    if not urow:
        raise HTTPException(status_code=404, detail="user not found")
    base = dict(urow)

    dev_rows = db.execute(
        text(
            """
            SELECT
              id,
              trusted,
              ua,
              platform,
              ip_first,
              ip_last,
              last_seen_at,
              created_at,
              revoked_at,
              device_token_expires_at,
              CASE
                WHEN revoked_at IS NOT NULL THEN 'revoked'
                WHEN device_token_expires_at <= datetime('now', '+8 hours') THEN 'expired'
                ELSE 'active'
              END AS session_state
            FROM user_devices
            WHERE user_id=:uid
            ORDER BY last_seen_at DESC, id DESC
            LIMIT 50
            """
        ),
        {"uid": user_id},
    ).mappings().all()
    devices = [dict(r) for r in dev_rows]

    ev_rows = db.execute(
        text(
            """
            SELECT id, login_identifier, login_type, result, reason, ip, ua, created_at
            FROM login_events
            WHERE user_id=:uid
            ORDER BY id DESC
            LIMIT 80
            """
        ),
        {"uid": user_id},
    ).mappings().all()
    login_events = [dict(r) for r in ev_rows]

    agg = db.execute(
        text(
            """
            SELECT
              SUM(CASE WHEN created_at >= datetime('now', '-24 hours') AND result='FAIL' THEN 1 ELSE 0 END) AS fail_24h,
              SUM(CASE WHEN created_at >= datetime('now', '-24 hours') AND result='SUCCESS' THEN 1 ELSE 0 END) AS ok_24h,
              SUM(CASE WHEN created_at >= datetime('now', '-7 days') AND result='FAIL' THEN 1 ELSE 0 END) AS fail_7d,
              SUM(CASE WHEN created_at >= datetime('now', '-7 days') AND result='SUCCESS' THEN 1 ELSE 0 END) AS ok_7d,
              SUM(CASE WHEN created_at >= datetime('now', '-30 days') AND result='FAIL' THEN 1 ELSE 0 END) AS fail_30d,
              MAX(CASE WHEN result='SUCCESS' THEN created_at END) AS last_ok_at,
              MAX(CASE WHEN result='FAIL' THEN created_at END) AS last_fail_at
            FROM login_events
            WHERE user_id=:uid
            """
        ),
        {"uid": user_id},
    ).mappings().first()
    agg_d = dict(agg or {})
    risk = _risk_summary_from_login_counts(
        fail24h=int(agg_d.get("fail_24h") or 0),
        success24h=int(agg_d.get("ok_24h") or 0),
        fail7d=int(agg_d.get("fail_7d") or 0),
    )

    aud_rows = db.execute(
        text(
            """
            SELECT id, actor_user_id, action, detail_json, ip, ua, created_at
            FROM audit_logs
            WHERE target_user_id=:uid
            ORDER BY id DESC
            LIMIT 50
            """
        ),
        {"uid": user_id},
    ).mappings().all()
    audits = []
    for r in aud_rows:
        d = dict(r)
        dj = d.get("detail_json")
        if isinstance(dj, (bytes, bytearray)):
            try:
                dj = dj.decode("utf-8", errors="ignore")
            except Exception:
                dj = None
        if isinstance(dj, str):
            try:
                dj = json.loads(dj)
            except Exception:
                pass
        d["detail_json"] = dj
        audits.append(d)

    acc_rows = db.execute(
        text(
            """
            SELECT created_at, path, method, status_code
            FROM user_access_logs
            WHERE user_id=:uid
            ORDER BY id DESC
            LIMIT 100
            """
        ),
        {"uid": user_id},
    ).mappings().all()
    access_logs = [dict(r) for r in acc_rows]

    return {
        "user": base,
        "devices": devices,
        "access_logs": access_logs,
        "login_events": login_events,
        "login_stats": {
            "fail_24h": int(agg_d.get("fail_24h") or 0),
            "success_24h": int(agg_d.get("ok_24h") or 0),
            "fail_7d": int(agg_d.get("fail_7d") or 0),
            "success_7d": int(agg_d.get("ok_7d") or 0),
            "fail_30d": int(agg_d.get("fail_30d") or 0),
            "last_success_login_event_at": agg_d.get("last_ok_at"),
            "last_fail_login_event_at": agg_d.get("last_fail_at"),
        },
        "risk": risk,
        "audit_logs": audits,
    }


@app.post("/api/admin/users/{user_id}/status")
def admin_user_status(
    user_id: int,
    payload: dict,
    request: Request,
    user=Depends(require_roles("admin")),
    db: Session = Depends(get_session),
):
    target_status = str((payload or {}).get("status") or "").strip().lower()
    if target_status not in ("active", "disabled", "locked"):
        raise HTTPException(status_code=400, detail="invalid status")
    db.execute(
        text("UPDATE users SET status=:st, updated_at=datetime('now', '+8 hours') WHERE id=:id"),
        {"st": target_status, "id": user_id},
    )
    _audit_log(
        db,
        action="admin_user_status",
        actor_user_id=user.get("id"),
        target_user_id=user_id,
        detail={"status": target_status},
        request=request,
    )
    db.commit()
    return {"ok": True}


@app.post("/api/admin/users/{user_id}/reset-password")
def admin_reset_password(
    user_id: int,
    request: Request,
    user=Depends(require_roles("admin")),
    db: Session = Depends(get_session),
):
    new_pwd = _generate_system_password()
    db.execute(
        text(
            """
            UPDATE users
            SET password=:p,
                password_is_system_generated=1,
                password_changed_at=NULL,
                updated_at=datetime('now', '+8 hours')
            WHERE id=:id
            """
        ),
        {"p": new_pwd, "id": user_id},
    )
    _audit_log(
        db,
        action="admin_reset_password",
        actor_user_id=user.get("id"),
        target_user_id=user_id,
        request=request,
    )
    db.commit()
    return {"ok": True, "password": new_pwd}


@app.post("/api/admin/users/{user_id}/revoke-devices")
def admin_revoke_devices(
    user_id: int,
    request: Request,
    user=Depends(require_roles("admin")),
    db: Session = Depends(get_session),
):
    db.execute(
        text("UPDATE user_devices SET revoked_at=datetime('now', '+8 hours') WHERE user_id=:id AND revoked_at IS NULL"),
        {"id": user_id},
    )
    _audit_log(
        db,
        action="admin_revoke_devices",
        actor_user_id=user.get("id"),
        target_user_id=user_id,
        request=request,
    )
    db.commit()
    return {"ok": True}


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    """站点根路径：策略前台用户登录。"""
    return templates.TemplateResponse("client_login.html", {"request": request, "app_name": settings.app_name})


@app.get("/admin-login", response_class=HTMLResponse)
def admin_login_page(request: Request):
    """后台管理登录（不在根路径，避免与前台入口混淆）。"""
    return templates.TemplateResponse("login.html", {"request": request, "app_name": settings.app_name})


@app.get("/admin/holdings", response_class=HTMLResponse)
def admin_holdings_page(request: Request):
    return templates.TemplateResponse("admin_holdings.html", {"request": request, "app_name": settings.app_name})


@app.get("/admin", response_class=HTMLResponse)
def admin_dashboard_page(request: Request):
    return templates.TemplateResponse("admin_dashboard.html", {"request": request, "app_name": settings.app_name})


@app.get("/admin/strategies", response_class=HTMLResponse)
def admin_strategies_page(request: Request):
    return templates.TemplateResponse("admin_strategies.html", {"request": request, "app_name": settings.app_name})


@app.get("/admin/nav-check", response_class=HTMLResponse)
def admin_nav_page(request: Request):
    return templates.TemplateResponse("admin_nav.html", {"request": request, "app_name": settings.app_name})


@app.get("/admin/users", response_class=HTMLResponse)
def admin_users_page(request: Request):
    return templates.TemplateResponse("admin_users.html", {"request": request, "app_name": settings.app_name})


@app.get("/admin/data-import", response_class=HTMLResponse)
def admin_data_import_page(request: Request):
    return templates.TemplateResponse(
        "admin_data_import.html",
        {"request": request, "app_name": settings.app_name},
    )


def _client_strategy_context(db: Session, strategy_id: str) -> dict:
    if not _SID_PATTERN.match(strategy_id):
        raise HTTPException(status_code=400, detail="invalid strategy_id")
    cfg = db.execute(
        text(
            """
            SELECT strategy_id, strategy_name
            FROM strategy_configs
            WHERE strategy_id=:sid AND is_visible=1 AND status='enabled'
            LIMIT 1
            """
        ),
        {"sid": strategy_id},
    ).mappings().first()
    if not cfg:
        raise HTTPException(status_code=404, detail="strategy not found")
    return {"strategy_id": strategy_id, "strategy_name": cfg["strategy_name"]}


@app.get("/client", response_class=HTMLResponse)
def client_strategies_page(request: Request):
    return templates.TemplateResponse("client_strategies.html", {"request": request, "app_name": settings.app_name})


@app.get("/client-login", response_class=HTMLResponse)
def client_login_page(request: Request):
    return templates.TemplateResponse("client_login.html", {"request": request, "app_name": settings.app_name})


@app.get("/client-register", response_class=HTMLResponse)
def client_register_page(request: Request, db: Session = Depends(get_session)):
    if not _client_register_allowed(db):
        return HTMLResponse(
            content=(
                "<!doctype html><html lang=\"zh-CN\"><head><meta charset=\"utf-8\"/>"
                "<title>注册已关闭</title></head><body style=\"font-family:sans-serif;padding:24px;\">"
                "<p>当前未开放新用户注册。</p>"
                "<p><a href=\"/client-login\">返回登录</a></p></body></html>"
            ),
            status_code=403,
        )
    return templates.TemplateResponse("client_register.html", {"request": request, "app_name": settings.app_name})


@app.get("/client-forgot-password", response_class=HTMLResponse)
def client_forgot_password_page(request: Request):
    return templates.TemplateResponse(
        "client_forgot_password.html",
        {"request": request, "app_name": settings.app_name},
    )


@app.get("/client-reset-password", response_class=HTMLResponse)
def client_reset_password_page(request: Request):
    return templates.TemplateResponse(
        "client_reset_password.html",
        {"request": request, "app_name": settings.app_name},
    )


@app.get("/client-change-password", response_class=HTMLResponse)
def client_change_password_page(request: Request):
    return templates.TemplateResponse(
        "client_change_password.html",
        {"request": request, "app_name": settings.app_name},
    )


@app.get("/client-profile", response_class=HTMLResponse)
def client_profile_page(request: Request):
    return templates.TemplateResponse(
        "client_profile.html",
        {"request": request, "app_name": settings.app_name},
    )


@app.get("/client/contact", response_class=HTMLResponse)
def client_contact_page(request: Request, db: Session = Depends(get_session)):
    if not _client_contact_enabled(db):
        return templates.TemplateResponse(
            "client_feature_disabled.html",
            {"request": request, "app_name": settings.app_name, "feature_name": "联系我们"},
        )
    return templates.TemplateResponse(
        "client_contact.html",
        {"request": request, "app_name": settings.app_name},
    )


@app.get("/client/feedback", response_class=HTMLResponse)
def client_feedback_page(request: Request, db: Session = Depends(get_session)):
    if not _client_feedback_enabled(db):
        return templates.TemplateResponse(
            "client_feature_disabled.html",
            {"request": request, "app_name": settings.app_name, "feature_name": "意见建议"},
        )
    return templates.TemplateResponse(
        "client_feedback.html",
        {"request": request, "app_name": settings.app_name},
    )


@app.get("/admin/client-messages", response_class=HTMLResponse)
def admin_client_messages_page(request: Request):
    return templates.TemplateResponse(
        "admin_client_messages.html",
        {"request": request, "app_name": settings.app_name},
    )


@app.get("/client/strategy/{strategy_id}/intro", response_class=HTMLResponse)
def client_strategy_intro(request: Request, strategy_id: str, db: Session = Depends(get_session)):
    ctx = _client_strategy_context(db, strategy_id)
    return templates.TemplateResponse(
        "client_intro.html",
        {"request": request, "app_name": settings.app_name, **ctx},
    )


@app.get("/client/strategy/{strategy_id}/nav", response_class=HTMLResponse)
def client_strategy_nav(request: Request, strategy_id: str, db: Session = Depends(get_session)):
    ctx = _client_strategy_context(db, strategy_id)
    return templates.TemplateResponse(
        "client_nav.html",
        {"request": request, "app_name": settings.app_name, **ctx},
    )


@app.get("/client/strategy/{strategy_id}/holdings", response_class=HTMLResponse)
def client_strategy_holdings(request: Request, strategy_id: str, db: Session = Depends(get_session)):
    ctx = _client_strategy_context(db, strategy_id)
    return templates.TemplateResponse(
        "client_holdings.html",
        {"request": request, "app_name": settings.app_name, **ctx},
    )


@app.get("/client/strategy/{strategy_id}/stock/{stock_code}", response_class=HTMLResponse)
def client_strategy_stock_profile(
    request: Request, strategy_id: str, stock_code: str, db: Session = Depends(get_session)
):
    ctx = _client_strategy_context(db, strategy_id)
    return templates.TemplateResponse(
        "client_stock.html",
        {
            "request": request,
            "app_name": settings.app_name,
            "stock_code": stock_code,
            **ctx,
        },
    )

"""前台「联系我们 / 意见建议」入库与查询。"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.sql_dialect import sql_now


def insert_client_submission(
    db: Session,
    *,
    kind: str,
    title: str,
    content: str,
    contact_info: str = "",
    user_id: int | None = None,
    username: str = "",
    public_guest: bool = False,
    client_ip: str = "",
) -> int:
    row = db.execute(
        text(
            f"""
            INSERT INTO client_feedback_submissions (
              kind, title, content, contact_info,
              user_id, username, is_public_guest, client_ip, created_at
            ) VALUES (
              :kind, :title, :content, :contact,
              :uid, :uname, :pg, :ip, {sql_now()}
            )
            """
        ),
        {
            "kind": kind[:16],
            "title": title[:200],
            "content": content[:20000],
            "contact": (contact_info or "")[:255],
            "uid": user_id,
            "uname": (username or "")[:64],
            "pg": 1 if public_guest else 0,
            "ip": (client_ip or "")[:64],
        },
    )
    return int(row.lastrowid or 0)


def list_client_submissions(
    db: Session,
    *,
    kind: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[dict], int]:
    lim = max(1, min(int(limit), 500))
    off = max(0, int(offset))
    params: dict = {"lim": lim, "off": off}
    where = ""
    if kind:
        where = " WHERE kind=:kind "
        params["kind"] = kind.strip()[:16]
    total_row = db.execute(
        text(f"SELECT COUNT(*) AS c FROM client_feedback_submissions{where}"),
        params,
    ).mappings().first()
    total = int((total_row or {}).get("c") or 0)
    rows = db.execute(
        text(
            f"""
            SELECT id, kind, title, content, contact_info, user_id, username,
                   is_public_guest, client_ip, created_at
            FROM client_feedback_submissions
            {where}
            ORDER BY id DESC
            LIMIT :lim OFFSET :off
            """
        ),
        params,
    ).mappings().all()
    return [dict(r) for r in rows], total

"""可选 SMTP：发送忘记密码邮件与管理端连通测试。"""
from __future__ import annotations

import logging
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr

from app.config import Settings
from app.timeutil import now_naive

logger = logging.getLogger(__name__)


def _smtp_deliver(settings: Settings, from_addr: str, recipients: list[str], msg_as_string: str) -> None:
    host = (settings.smtp_host or "").strip()
    if not host:
        raise ValueError("SMTP_HOST 未配置")
    port = int(settings.smtp_port or 587)
    user = (settings.smtp_user or "").strip()
    password = settings.smtp_password or ""
    if settings.smtp_use_ssl:
        with smtplib.SMTP_SSL(host, port, timeout=30) as smtp:
            if user:
                smtp.login(user, password)
            smtp.sendmail(from_addr, recipients, msg_as_string)
    elif settings.smtp_use_tls:
        with smtplib.SMTP(host, port, timeout=30) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
            if user:
                smtp.login(user, password)
            smtp.sendmail(from_addr, recipients, msg_as_string)
    else:
        with smtplib.SMTP(host, port, timeout=30) as smtp:
            if user:
                smtp.login(user, password)
            smtp.sendmail(from_addr, recipients, msg_as_string)


def send_password_reset_email(settings: Settings, to_addr: str, reset_url: str) -> bool:
    from_addr = (settings.smtp_from_addr or settings.smtp_user or "").strip()
    if not from_addr:
        logger.warning("SMTP host set but smtp_from_addr/smtp_user empty; skip send")
        return False
    subject = "重置密码链接"
    body = (
        "您正在重置本平台登录密码。\n\n"
        f"请点击链接（1 小时内有效）：\n{reset_url}\n\n"
        "如非本人操作，请忽略本邮件。\n"
    )
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = formataddr(("Strategy Showcase", from_addr))
    msg["To"] = to_addr
    try:
        _smtp_deliver(settings, from_addr, [to_addr], msg.as_string())
        return True
    except Exception:
        logger.exception("send_password_reset_email failed to=%s", to_addr)
        return False


def send_contact_us_message(
    settings: Settings,
    *,
    title: str,
    content: str,
    contact: str = "",
    from_username: str = "",
    public_guest: bool = False,
    client_ip: str = "",
) -> tuple[bool, str]:
    """
    前台「联系我们」：将标题与正文发到配置的收件箱（默认 zh1111@88.com）。
    public_guest 为 True 时表示未登录用户通过公开入口提交。
    成功返回 (True, 说明)；失败返回 (False, 错误信息)。
    """
    to_addr = (settings.contact_us_inbox_email or "").strip()
    if not to_addr:
        return False, "未配置联系收件邮箱"
    from_addr = (settings.smtp_from_addr or settings.smtp_user or "").strip()
    if not from_addr:
        return False, "未配置 SMTP 发件人（SMTP_FROM_ADDR 或 SMTP_USER）"
    host = (settings.smtp_host or "").strip()
    if not host:
        return False, "未配置 SMTP（SMTP_HOST），无法发送邮件"
    safe_title = (title or "").strip().replace("\r", " ").replace("\n", " ")[:200]
    safe_content = (content or "").strip()
    safe_contact = (contact or "").strip()[:255]
    safe_user = (from_username or "").strip() or "（未知用户）"
    cip = (client_ip or "").strip()[:64]
    ip_line = f"客户端 IP：{cip}\n" if (public_guest and cip) else ""
    if public_guest:
        head = f"登录状态：未登录（公开「联系我们」入口）\n{ip_line}"
    else:
        head = f"登录用户名：{safe_user}\n"
    subject = f"[联系我们] {safe_title}"[:900]
    body = (
        f"{head}"
        f"提交时间：{now_naive().strftime('%Y-%m-%d %H:%M:%S')} (北京时间)\n\n"
        f"标题：\n{safe_title}\n\n"
        f"联系方式：\n{safe_contact if safe_contact else '（未填写）'}\n\n"
        f"内容：\n{safe_content}\n"
    )
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = formataddr(("Strategy Showcase 联系我们", from_addr))
    msg["To"] = to_addr
    try:
        _smtp_deliver(settings, from_addr, [to_addr], msg.as_string())
        return True, "发送成功"
    except Exception as e:
        logger.exception("send_contact_us_message failed")
        return False, str(e) or type(e).__name__


def smtp_send_test(settings: Settings, to_addr: str) -> tuple[bool, str]:
    """
    管理端连通测试：成功返回 (True, 简短说明)，失败返回 (False, 错误信息字符串)。
    """
    to_addr = (to_addr or "").strip()
    if not to_addr:
        return False, "收件地址为空"
    from_addr = (settings.smtp_from_addr or settings.smtp_user or "").strip()
    if not from_addr:
        return False, "未配置 SMTP_FROM_ADDR 或 SMTP_USER（发件人）"
    subject = "SMTP 连通测试"
    body = f"这是一封来自 Strategy Showcase 管理端的 SMTP 测试邮件。\n发送时间：{now_naive().strftime('%Y-%m-%d %H:%M:%S')} (北京时间)\n"
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = formataddr(("Strategy Showcase SMTP Test", from_addr))
    msg["To"] = to_addr
    try:
        _smtp_deliver(settings, from_addr, [to_addr], msg.as_string())
        return True, "发送成功（请检查收件箱/垃圾箱）"
    except Exception as e:
        logger.exception("smtp_send_test failed to=%s", to_addr)
        return False, str(e) or type(e).__name__

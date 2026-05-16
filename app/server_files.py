"""服务端上传目录：Render 等同区读文件写 Turso；本地未配置时仍走 STRATEGY_ROOT_DIR。"""

from __future__ import annotations

import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import HTTPException, UploadFile

from app.config import settings

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_SAFE_NAME = re.compile(r"[^A-Za-z0-9._\u4e00-\u9fff-]+")
_TABULAR_EXTS = frozenset({".xlsx", ".xls", ".xlsm", ".csv"})
_STRATEGY_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")


def upload_root() -> Path | None:
    raw = (getattr(settings, "server_upload_root", None) or "").strip()
    if not raw:
        return None
    p = Path(raw)
    if not p.is_absolute():
        p = _PROJECT_ROOT / p
    return p.resolve()


def server_upload_enabled() -> bool:
    return upload_root() is not None


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def sanitize_filename(name: str) -> str:
    base = Path(name or "upload").name
    stem = _SAFE_NAME.sub("_", Path(base).stem).strip("._") or "upload"
    ext = Path(base).suffix.lower()
    if ext not in _TABULAR_EXTS:
        ext = ".xlsx"
    return f"{stem[:120]}{ext}"


def file_stat(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    st = path.stat()
    return {
        "path": str(path),
        "size_bytes": st.st_size,
        "size_mb": round(st.st_size / (1024 * 1024), 3),
        "modified_at": datetime.fromtimestamp(st.st_mtime).isoformat(timespec="seconds"),
    }


async def save_upload(upload: UploadFile, dest: Path) -> Path:
    ext = Path(upload.filename or "").suffix.lower()
    if ext and ext not in _TABULAR_EXTS:
        raise HTTPException(
            status_code=400,
            detail=f"unsupported file type {ext}; allowed: {', '.join(sorted(_TABULAR_EXTS))}",
        )
    max_mb = max(1, int(getattr(settings, "server_upload_max_mb", 200)))
    chunk = 1024 * 1024
    read = 0
    limit = max_mb * chunk
    _ensure_dir(dest.parent)
    tmp = dest.with_suffix(dest.suffix + ".part")
    try:
        with open(tmp, "wb") as out:
            while True:
                block = await upload.read(chunk)
                if not block:
                    break
                read += len(block)
                if read > limit:
                    raise HTTPException(
                        status_code=413,
                        detail=f"file too large (max {max_mb} MB)",
                    )
                out.write(block)
        os.replace(tmp, dest)
    finally:
        if tmp.is_file():
            try:
                tmp.unlink()
            except OSError:
                pass
    return dest


def supplement_dir(definition_code: str) -> Path | None:
    root = upload_root()
    if not root:
        return None
    code = (definition_code or "").strip()
    if not code:
        return None
    return _ensure_dir(root / "supplement" / code)


def supplement_canonical_name() -> str:
    return "import.xlsx"


def resolve_supplement_upload_path(definition_code: str) -> Path | None:
    """服务器上该导入类型已上传文件的路径（固定 import.xlsx 或目录内唯一表格）。"""
    d = supplement_dir(definition_code)
    if not d:
        return None
    fixed = d / supplement_canonical_name()
    if fixed.is_file():
        return fixed
    cands = sorted(
        [p for p in d.iterdir() if p.is_file() and p.suffix.lower() in _TABULAR_EXTS],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return cands[0] if cands else None


async def upload_supplement_file(
    definition_code: str,
    upload: UploadFile,
    *,
    use_canonical_name: bool = True,
) -> dict[str, Any]:
    d = supplement_dir(definition_code)
    if not d:
        raise HTTPException(
            status_code=400,
            detail="未配置 SERVER_UPLOAD_ROOT，无法在服务器保存上传文件",
        )
    if use_canonical_name:
        ext = Path(upload.filename or "").suffix.lower()
        if ext not in _TABULAR_EXTS:
            ext = ".xlsx"
        dest = d / f"import{ext}"
    else:
        dest = d / sanitize_filename(upload.filename or "import.xlsx")
    await save_upload(upload, dest)
    st = file_stat(dest)
    return {
        "ok": True,
        "definition_code": definition_code,
        "path": str(dest),
        "overwritten": True,
        "file": st,
    }


def strategy_dir(strategy_id: str) -> Path | None:
    root = upload_root()
    sid = (strategy_id or "").strip()
    if not root or not sid or not _STRATEGY_ID.match(sid):
        return None
    return _ensure_dir(root / "strategies" / sid)


def strategy_relative_dir(strategy_id: str) -> str:
    return f"strategies/{strategy_id.strip()}"


def resolve_strategy_excel_path(file_dir: str | None, file_name: str) -> str:
    """
    解析策略 Excel 绝对路径。
    已配置 SERVER_UPLOAD_ROOT 且 file_dir 为 strategies/... 时从上传根目录读；
    否则从 STRATEGY_ROOT_DIR 读（本地开发）。
    """
    fn = (file_name or "").strip()
    if not fn:
        raise ValueError("file_name empty")
    fd = (file_dir or "").strip().replace("\\", "/").strip("/")
    root = upload_root()
    if root and fd.startswith("strategies/"):
        p = root / fd / fn
        return os.path.normpath(str(p))
    base = Path(str(settings.strategy_root_dir).strip()).expanduser()
    p = (base / fd / fn) if fd else (base / fn)
    return os.path.normpath(str(p))


async def upload_strategy_data_file(strategy_id: str, upload: UploadFile) -> dict[str, Any]:
    sid = (strategy_id or "").strip()
    if not _STRATEGY_ID.match(sid):
        raise HTTPException(status_code=400, detail="invalid strategy_id")
    d = strategy_dir(sid)
    if not d:
        raise HTTPException(
            status_code=400,
            detail="未配置 SERVER_UPLOAD_ROOT，无法在服务器保存上传文件",
        )
    dest = d / sanitize_filename(upload.filename or f"{sid}.xlsx")
    await save_upload(upload, dest)
    return {
        "ok": True,
        "strategy_id": sid,
        "file_dir": strategy_relative_dir(sid),
        "file_name": dest.name,
        "path": str(dest),
        "overwritten": True,
        "file": file_stat(dest),
    }


def resolve_supplement_import_path(
    *,
    definition_code: str,
    explicit_path: str | None,
    default_file_path: str | None,
    fallback_path: str | None,
) -> str:
    """导入前解析可读路径：显式路径 > 服务器已上传 > 库默认 > 本地默认。"""
    path = (explicit_path or "").strip()
    if path and Path(path).is_file():
        return path
    server_p = resolve_supplement_upload_path(definition_code)
    if server_p and server_p.is_file():
        return str(server_p)
    path = (default_file_path or "").strip()
    if path and Path(path).is_file():
        return path
    path = (fallback_path or "").strip()
    if path and Path(path).is_file():
        return path
    if server_p:
        return str(server_p)
    if (explicit_path or "").strip():
        return explicit_path.strip()
    if (default_file_path or "").strip():
        return default_file_path.strip()
    return (fallback_path or "").strip()

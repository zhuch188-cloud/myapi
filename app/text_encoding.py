"""上传文本/CSV 编码探测与规范化（避免 GBK 文件按 UTF-8 读成乱码）。"""

from __future__ import annotations


def decode_text_bytes(raw: bytes) -> str:
    """依次尝试 UTF-8（含 BOM）、GB18030/GBK，适配 Windows Excel 导出的 CSV。"""
    if not raw:
        return ""
    for enc in ("utf-8-sig", "utf-8", "gb18030", "gbk"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def normalize_unicode_text(value: object, *, max_len: int = 0) -> str:
    """写入库前的 Unicode 文本：去首尾空白，可选截断。"""
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    if max_len > 0:
        return s[:max_len]
    return s

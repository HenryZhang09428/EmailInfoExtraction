"""
文件路由模块 (File Router Module)
===============================

根据文件扩展名将输入文件路由为对应的 SourceDoc，用于后续提取流程。
"""

import uuid
from typing import Any, Dict, List, Optional, Set
from pathlib import Path
from core.ir import SourceDoc, SourceType

# 已知文件扩展名白名单，用于正确的类型路由
EXCEL_EXTENSIONS = {'.xlsx', '.xls'}
EMAIL_EXTENSIONS = {'.docx', '.txt', '.eml'}
IMAGE_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.gif', '.bmp', '.webp'}
TEXT_EXTENSIONS = {'.md', '.csv', '.json', '.xml', '.html', '.htm', '.log', '.rtf'}

# Combined allowlist of all known extensions
KNOWN_EXTENSIONS = EXCEL_EXTENSIONS | EMAIL_EXTENSIONS | IMAGE_EXTENSIONS | TEXT_EXTENSIONS

# Supported source types for routing.
_SUPPORTED_SOURCE_TYPES: Set[str] = {"excel", "email", "image", "text", "other"}


def _normalize_extension(ext: Any) -> str:
    if not isinstance(ext, str):
        return ""
    v = ext.strip().lower()
    if not v:
        return ""
    if not v.startswith("."):
        v = "." + v
    if v == ".":
        return ""
    return v


def _normalize_router_options(router_options: Optional[dict]) -> Dict[str, Any]:
    """
    Normalize user/profile-provided router options into a stable internal config.
    All keys are optional; missing keys fall back to module defaults.
    """
    cfg: Dict[str, Any] = {}
    if not isinstance(router_options, dict) or not router_options:
        return cfg

    allowed_source_types = router_options.get("allowed_source_types")
    if isinstance(allowed_source_types, list):
        allowed = []
        for t in allowed_source_types:
            if isinstance(t, str) and t.strip().lower() in _SUPPORTED_SOURCE_TYPES:
                allowed.append(t.strip().lower())
        if allowed:
            cfg["allowed_source_types"] = allowed

    allowed_extensions = router_options.get("allowed_extensions")
    if isinstance(allowed_extensions, list):
        exts = []
        for e in allowed_extensions:
            ne = _normalize_extension(e)
            if ne:
                exts.append(ne)
        if exts:
            cfg["allowed_extensions"] = sorted(set(exts))

    extension_overrides = router_options.get("extension_overrides")
    if isinstance(extension_overrides, dict):
        overrides: Dict[str, str] = {}
        for k, v in extension_overrides.items():
            ek = _normalize_extension(k)
            if not ek:
                continue
            if isinstance(v, str) and v.strip().lower() in _SUPPORTED_SOURCE_TYPES:
                overrides[ek] = v.strip().lower()
        if overrides:
            cfg["extension_overrides"] = overrides

    ignore_unknown_extensions = router_options.get("ignore_unknown_extensions")
    if isinstance(ignore_unknown_extensions, bool):
        cfg["ignore_unknown_extensions"] = ignore_unknown_extensions

    return cfg


def _get_source_type(ext: str, router_cfg: Optional[Dict[str, Any]] = None) -> SourceType:
    """
    根据文件扩展名确定 SourceType。

    未知扩展名返回 'other'，避免将二进制文件误当作文本解析。
    """
    ext = (ext or "").lower()
    if router_cfg and isinstance(router_cfg.get("extension_overrides"), dict):
        override = router_cfg["extension_overrides"].get(ext)
        if override in _SUPPORTED_SOURCE_TYPES:
            return override  # type: ignore[return-value]
    if ext in EXCEL_EXTENSIONS:
        return "excel"
    elif ext in EMAIL_EXTENSIONS:
        return "email"
    elif ext in IMAGE_EXTENSIONS:
        return "image"
    elif ext in TEXT_EXTENSIONS:
        return "text"
    else:
        # Fallback: unknown extension treated as 'other' (binary)
        # This prevents UTF-8 decode errors on binary files
        return "other"


def route_files(file_paths: List[str], router_options: Optional[dict] = None) -> List[SourceDoc]:
    """
    根据文件扩展名将文件列表路由为 SourceDoc 列表。

    为每个文档填充必填的 file_path 字段以唯一标识。
    未知扩展名路由为 'other' 类型，避免二进制误解析。
    """
    router_cfg = _normalize_router_options(router_options)
    allowed_source_types = router_cfg.get("allowed_source_types")
    allowed_extensions = set(router_cfg.get("allowed_extensions") or [])
    ignore_unknown = bool(router_cfg.get("ignore_unknown_extensions"))

    result = []
    
    for file_path in file_paths:
        path = Path(file_path)
        ext = path.suffix.lower()
        if allowed_extensions:
            n_ext = _normalize_extension(ext)
            if not n_ext or n_ext not in allowed_extensions:
                continue
        
        source_type = _get_source_type(ext, router_cfg)
        if ignore_unknown and source_type == "other" and ext not in KNOWN_EXTENSIONS:
            continue
        if isinstance(allowed_source_types, list) and allowed_source_types:
            if source_type not in allowed_source_types:
                continue
        
        source_doc = SourceDoc(
            source_id=str(uuid.uuid4()),
            filename=path.name,
            file_path=str(path.resolve()),  # Mandatory field: absolute path as unique identifier
            source_type=source_type,
            blocks=[],
            extracted=None
        )
        result.append(source_doc)
    
    return result

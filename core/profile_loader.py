"""
配置档案加载模块 (Profile Loader Module)
======================================

从 YAML 档案文件加载作业配置，包括 Excel 工作表选择、模板路径、填充列等。
"""

from pathlib import Path
from typing import Any, Dict, List

import yaml

# 项目根目录
# core/profile_loader.py → parents[1] 才是仓库根目录（EmailsInfoExtraction）
REPO_ROOT = Path(__file__).resolve().parents[1]

# Router supported types (keep in sync with core.router.SourceType values)
_ROUTER_SOURCE_TYPES = {"excel", "email", "image", "text", "other"}


def _ensure_dict(value: Any) -> Dict[str, Any]:
    """
    确保返回字典类型，非字典则返回空字典。
    """
    if isinstance(value, dict):
        return value
    return {}


def _ensure_list(value: Any) -> List[Any]:
    """
    确保返回列表类型，非列表则返回空列表。
    """
    if isinstance(value, list):
        return value
    return []


def _ensure_str_list(value: Any) -> List[str]:
    """确保返回字符串列表，过滤空白项与非字符串项。"""
    if not isinstance(value, list):
        return []
    out: List[str] = []
    for item in value:
        if isinstance(item, str) and item.strip():
            out.append(item.strip())
    return out


def _normalize_extension(ext: Any) -> str:
    """
    规范化扩展名：
    - 统一为小写
    - 确保以 '.' 开头
    - 过滤非法值
    """
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


def load_profile(profile_path: str) -> dict:
    """
    从 YAML 文件加载配置档案。

    参数:
        profile_path: 档案文件路径，支持相对路径（相对于项目根）

    返回:
        包含 profile_id、excel、templates 等配置的字典

    抛出:
        FileNotFoundError: 档案文件不存在时
    """
    if not profile_path:
        return {
            "profile_id": None,
            "excel": {"sheet": "auto"},
            "templates": [],
            "insurance": {},
        }

    path = Path(profile_path).expanduser()
    if not path.is_absolute():
        path = (REPO_ROOT / path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"profile not found: {path}")

    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    data = _ensure_dict(raw)

    excel = _ensure_dict(data.get("excel"))
    sheet = excel.get("sheet") if isinstance(excel.get("sheet"), str) else "auto"

    # extract_mode: "auto" (default) | "single" | "all"
    _VALID_EXTRACT_MODES = {"auto", "single", "all"}
    raw_mode = excel.get("extract_mode")
    extract_mode = (
        raw_mode.strip().lower()
        if isinstance(raw_mode, str) and raw_mode.strip().lower() in _VALID_EXTRACT_MODES
        else "auto"
    )

    templates_raw = _ensure_list(data.get("templates"))
    templates: List[Dict[str, Any]] = []
    for idx, item in enumerate(templates_raw):
        if not isinstance(item, dict):
            continue
        key = item.get("key")
        if not isinstance(key, str) or not key.strip():
            key = f"job_{idx + 1}"
        template_path = item.get("template_path")
        if not isinstance(template_path, str) or not template_path.strip():
            continue
        fill_columns = item.get("fill_columns")
        if not isinstance(fill_columns, list):
            fill_columns = None
        special_field_to_column = item.get("special_field_to_column")
        if not isinstance(special_field_to_column, dict):
            special_field_to_column = None
        output_name = item.get("output_name")
        if not isinstance(output_name, str) or not output_name.strip():
            output_name = None
        max_sources = item.get("max_sources")
        if not isinstance(max_sources, int) or max_sources <= 0:
            max_sources = None
        templates.append(
            {
                "key": key,
                "template_path": template_path,
                "fill_columns": fill_columns,
                "special_field_to_column": special_field_to_column,
                "output_name": output_name,
                "max_sources": max_sources,
            }
        )

    insurance_raw = _ensure_dict(data.get("insurance"))
    add_keywords = _ensure_str_list(insurance_raw.get("add_keywords"))
    remove_keywords = _ensure_str_list(insurance_raw.get("remove_keywords"))
    header_force_map_raw = _ensure_dict(insurance_raw.get("header_force_map"))
    header_force_map: Dict[str, str] = {}
    for key, value in header_force_map_raw.items():
        if isinstance(key, str) and key.strip() and isinstance(value, str) and value.strip():
            header_force_map[key.strip()] = value.strip()
    insurance: Dict[str, Any] = {}
    if add_keywords:
        insurance["add_keywords"] = add_keywords
    if remove_keywords:
        insurance["remove_keywords"] = remove_keywords
    if header_force_map:
        insurance["header_force_map"] = header_force_map

    # Router options (file extension → source_type routing / allowlist)
    router_raw = _ensure_dict(data.get("router"))
    allowed_source_types_raw = _ensure_str_list(router_raw.get("allowed_source_types"))
    allowed_source_types = [
        t.strip().lower()
        for t in allowed_source_types_raw
        if isinstance(t, str) and t.strip().lower() in _ROUTER_SOURCE_TYPES
    ]
    allowed_extensions_raw = _ensure_str_list(router_raw.get("allowed_extensions"))
    allowed_extensions = [
        _normalize_extension(e) for e in allowed_extensions_raw
        if _normalize_extension(e)
    ]
    extension_overrides_raw = _ensure_dict(router_raw.get("extension_overrides"))
    extension_overrides: Dict[str, str] = {}
    for k, v in extension_overrides_raw.items():
        ek = _normalize_extension(k)
        if not ek:
            continue
        if isinstance(v, str) and v.strip().lower() in _ROUTER_SOURCE_TYPES:
            extension_overrides[ek] = v.strip().lower()
    ignore_unknown_extensions = router_raw.get("ignore_unknown_extensions")
    if not isinstance(ignore_unknown_extensions, bool):
        ignore_unknown_extensions = None
    router: Dict[str, Any] = {}
    if allowed_source_types:
        router["allowed_source_types"] = allowed_source_types
    if allowed_extensions:
        router["allowed_extensions"] = allowed_extensions
    if extension_overrides:
        router["extension_overrides"] = extension_overrides
    if ignore_unknown_extensions is not None:
        router["ignore_unknown_extensions"] = ignore_unknown_extensions

    return {
        "profile_id": data.get("profile_id"),
        "excel": {"sheet": sheet, "extract_mode": extract_mode},
        "templates": templates,
        "insurance": insurance,
        "router": router,
    }

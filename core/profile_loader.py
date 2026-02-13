from pathlib import Path
from typing import Any, Dict, List

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]


def _ensure_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _ensure_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    return []


def load_profile(profile_path: str) -> dict:
    if not profile_path:
        return {
            "profile_id": None,
            "excel": {"sheet": "auto"},
            "templates": [],
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
        templates.append(
            {
                "key": key,
                "template_path": template_path,
                "fill_columns": fill_columns,
                "special_field_to_column": special_field_to_column,
            }
        )

    return {
        "profile_id": data.get("profile_id"),
        "excel": {"sheet": sheet},
        "templates": templates,
    }

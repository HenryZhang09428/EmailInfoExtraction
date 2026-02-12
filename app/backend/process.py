import json
import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from core.logger import get_logger
from core.pipeline import fill_template, run_extract
from core.runtime.profile_loader import load_profile

logger = get_logger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]
TEMPLATE_DIR = REPO_ROOT / "app" / "templates"

TEMPLATE_ADD_NAME = "目标1模版：社保增员表.xlsx"
TEMPLATE_REMOVE_NAME = "目标2模版：社保减员表.xlsx"

FALLBACK_ADD_PATH = Path("/Users/zhanghengyu/Desktop/目标1模版：社保增员表.xlsx")
FALLBACK_REMOVE_PATH = Path("/Users/zhanghengyu/Desktop/目标2模版：社保减员表.xlsx")


def resolve_template_paths() -> Dict[str, Path]:
    templates = {
        "add": TEMPLATE_DIR / TEMPLATE_ADD_NAME,
        "remove": TEMPLATE_DIR / TEMPLATE_REMOVE_NAME,
    }
    resolved = {}
    for key, path in templates.items():
        if path.exists():
            resolved[key] = path
            continue
        fallback = FALLBACK_ADD_PATH if key == "add" else FALLBACK_REMOVE_PATH
        if fallback.exists():
            logger.warning("Embedded template missing, using fallback: %s", fallback)
            resolved[key] = fallback
            continue
        raise FileNotFoundError(
            f"Template not found for {key}. Missing both {path} and {fallback}."
        )
    return resolved


def ensure_output_dir(output_dir: str) -> Path:
    output_path = Path(output_dir).resolve()
    output_path.mkdir(parents=True, exist_ok=True)
    return output_path


def _build_source_dict(source: Any) -> Dict[str, Any]:
    return {
        "source_id": getattr(source, "source_id", None),
        "filename": getattr(source, "filename", None),
        "source_type": getattr(source, "source_type", None),
        "parent_source_id": getattr(source, "parent_source_id", None),
        "extracted": getattr(source, "extracted", None),
    }


def _summarize_sources(sources: List[Any]) -> Dict[str, Any]:
    by_type: Dict[str, int] = {}
    errors = []
    for src in sources:
        src_type = getattr(src, "source_type", None) or "unknown"
        by_type[src_type] = by_type.get(src_type, 0) + 1
        extracted = getattr(src, "extracted", None)
        if isinstance(extracted, dict) and extracted.get("error"):
            errors.append(
                {
                    "filename": getattr(src, "filename", None),
                    "error": extracted.get("error"),
                    "source_type": src_type,
                }
            )
    return {
        "total_sources": len(sources),
        "by_type": by_type,
        "errors": errors,
    }


def apply_fill_plan_overrides(
    fill_plan_dict: dict,
    fill_columns: Optional[List[str]] = None,
    special_field_to_column: Optional[Dict[str, str]] = None,
) -> dict:
    if not isinstance(fill_plan_dict, dict):
        return fill_plan_dict
    allowed = None
    if fill_columns:
        allowed = {str(col).upper() for col in fill_columns if isinstance(col, str)}
    special = None
    if special_field_to_column:
        special = {
            str(k): str(v).upper()
            for k, v in special_field_to_column.items()
            if isinstance(k, str) and isinstance(v, str)
        }

    def _apply(mapping: Any) -> Dict[str, str]:
        if not isinstance(mapping, dict):
            mapping = {}
        if special:
            for field, col in special.items():
                mapping[field] = col
        if allowed is not None:
            mapping = {
                k: v
                for k, v in mapping.items()
                if isinstance(v, str) and v.upper() in allowed
            }
        return mapping

    if "column_mapping" in fill_plan_dict or special or allowed is not None:
        fill_plan_dict["column_mapping"] = _apply(fill_plan_dict.get("column_mapping"))

    row_writes = fill_plan_dict.get("row_writes")
    if isinstance(row_writes, list):
        for row_write in row_writes:
            if isinstance(row_write, dict):
                row_write["column_mapping"] = _apply(row_write.get("column_mapping"))

    return fill_plan_dict


def _fill_with_template(
    ir: Any,
    template_path: Path,
    output_dir: Path,
    require_llm: bool = False,
    output_name: Optional[str] = None,
    fill_plan_postprocess: Optional[Any] = None,
) -> Tuple[str, Dict[str, Any]]:
    template_label = template_path.stem
    work_dir = Path(
        tempfile.mkdtemp(prefix=f"work_{template_label}_", dir=str(output_dir))
    )
    try:
        template_copy = work_dir / template_path.name
        shutil.copy2(str(template_path), str(template_copy))
        filled_path, _, fill_plan = fill_template(
            ir,
            str(template_copy),
            require_llm=require_llm,
            fill_plan_postprocess=fill_plan_postprocess,
        )
        final_name = output_name or f"{template_label}_filled.xlsx"
        final_path = output_dir / final_name
        shutil.move(str(filled_path), str(final_path))
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
    return str(final_path), fill_plan or {}


def build_readable_output(
    ir: Any,
    fill_results: Dict[str, Dict[str, Any]],
    input_files: List[str],
    output_dir: str,
) -> Dict[str, Any]:
    sources = getattr(ir, "sources", []) or []
    summary = _summarize_sources(sources)
    return {
        "meta": {
            "processed_at": datetime.now().isoformat(timespec="seconds"),
            "input_files": input_files,
            "output_dir": output_dir,
        },
        "summary": summary,
        "fills": fill_results,
        "sources": [_build_source_dict(s) for s in sources],
    }


def process_files(
    file_paths: List[str],
    output_dir: str,
    require_llm: bool = False,
    profile_path: Optional[str] = None,
) -> Dict[str, Any]:
    if not file_paths:
        raise ValueError("No input files provided.")

    output_path = ensure_output_dir(output_dir)
    profile = None
    templates = None
    excel_sheet = None
    if profile_path:
        profile = load_profile(profile_path)
        excel_sheet = (profile.get("excel") or {}).get("sheet")
        if not isinstance(excel_sheet, str) or excel_sheet.strip() == "" or excel_sheet == "auto":
            excel_sheet = None
        templates = profile.get("templates") or []
    else:
        templates = resolve_template_paths()

    logger.info("Processing %d files", len(file_paths))
    ir = run_extract(file_paths, excel_sheet=excel_sheet)

    fill_results: Dict[str, Dict[str, Any]] = {}
    if profile_path:
        for job in templates:
            if not isinstance(job, dict):
                continue
            key = job.get("key")
            template_path_value = job.get("template_path")
            if not isinstance(key, str) or not isinstance(template_path_value, str):
                continue
            template_path = Path(template_path_value).expanduser()
            if not template_path.is_absolute():
                template_path = (REPO_ROOT / template_path).resolve()
            if not template_path.exists():
                raise FileNotFoundError(f"Template not found: {template_path}")
            fill_columns = job.get("fill_columns")
            special_field_to_column = job.get("special_field_to_column")
            output_name = f"filled_{key}.xlsx"

            def _postprocess(fill_plan_dict: dict) -> dict:
                return apply_fill_plan_overrides(
                    fill_plan_dict,
                    fill_columns=fill_columns if isinstance(fill_columns, list) else None,
                    special_field_to_column=special_field_to_column if isinstance(special_field_to_column, dict) else None,
                )

            filled_path, fill_plan = _fill_with_template(
                ir=ir,
                template_path=template_path,
                output_dir=output_path,
                require_llm=require_llm,
                output_name=output_name,
                fill_plan_postprocess=_postprocess,
            )
            fill_results[key] = {
                "template": str(template_path),
                "output_path": filled_path,
                "llm_used": fill_plan.get("llm_used", False),
                "cells_written": (fill_plan.get("debug") or {}).get("cells_written"),
                "warnings": fill_plan.get("warnings", []),
                "debug": {
                    "filled_columns": fill_columns if isinstance(fill_columns, list) else None,
                    "special_mapping_keys": list(special_field_to_column.keys()) if isinstance(special_field_to_column, dict) else None,
                },
            }
    else:
        for key, template_path in templates.items():
            filled_path, fill_plan = _fill_with_template(
                ir=ir,
                template_path=template_path,
                output_dir=output_path,
                require_llm=require_llm,
            )
            fill_results[key] = {
                "template": str(template_path),
                "output_path": filled_path,
                "llm_used": fill_plan.get("llm_used", False),
                "cells_written": (fill_plan.get("debug") or {}).get("cells_written"),
                "warnings": fill_plan.get("warnings", []),
            }

    return build_readable_output(ir, fill_results, file_paths, str(output_path))


def write_json_output(result: Dict[str, Any], output_dir: str) -> str:
    output_path = Path(output_dir).resolve()
    output_path.mkdir(parents=True, exist_ok=True)
    json_path = output_path / "result.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    return str(json_path)

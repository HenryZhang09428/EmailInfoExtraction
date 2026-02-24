"""
后端处理模块 (Backend Process Module)
====================================

封装文件处理流水线：提取 → 填充模板 → 输出 JSON。
支持 profile 配置和默认模板。
"""

import json
import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from core.logger import get_logger
from core.pipeline import fill_template, run_extract
from core.profile_loader import load_profile

logger = get_logger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]
TEMPLATE_DIR = REPO_ROOT / "app" / "templates"

DEFAULT_TEMPLATE_ADD_NAME = "目标1模版：社保增员表.xlsx"
DEFAULT_TEMPLATE_REMOVE_NAME = "目标2模版：社保减员表.xlsx"


def resolve_template_paths() -> Dict[str, Path]:
    """
    解析增员/减员模板路径。
    优先使用 TEMPLATE_ADD_PATH、TEMPLATE_REMOVE_PATH 环境变量。
    """
    add_name = os.getenv("TEMPLATE_ADD_NAME", DEFAULT_TEMPLATE_ADD_NAME)
    remove_name = os.getenv("TEMPLATE_REMOVE_NAME", DEFAULT_TEMPLATE_REMOVE_NAME)
    templates = {
        "add": _resolve_template_path_from_env_or_default("add", add_name),
        "remove": _resolve_template_path_from_env_or_default("remove", remove_name),
    }
    resolved = {}
    for key, path in templates.items():
        if path.exists():
            resolved[key] = path
            continue
        env_key = f"TEMPLATE_{key.upper()}_PATH"
        raise FileNotFoundError(
            f"Template not found for {key}: {path}. "
            f"Set {env_key} to override template path."
        )
    return resolved


def _resolve_template_path_from_env_or_default(template_key: str, template_name: str) -> Path:
    """从环境变量或默认目录解析模板路径。"""
    env_key = f"TEMPLATE_{template_key.upper()}_PATH"
    env_path = os.getenv(env_key, "").strip()
    if env_path:
        path = Path(env_path).expanduser()
        if not path.is_absolute():
            path = (REPO_ROOT / path).resolve()
        return path
    return TEMPLATE_DIR / template_name


def ensure_output_dir(output_dir: str) -> Path:
    """确保输出目录存在，不存在则创建。"""
    output_path = Path(output_dir).resolve()
    output_path.mkdir(parents=True, exist_ok=True)
    return output_path


def _load_template_registry_overrides(registry_source: str) -> Optional[Dict[str, Any]]:
    path = Path(registry_source).expanduser()
    if not path.is_absolute():
        path = (REPO_ROOT / path).resolve()
    if not path.exists() or not path.is_file():
        return None
    try:
        if path.suffix.lower() == ".json":
            data = json.loads(path.read_text(encoding="utf-8"))
        else:
            import yaml  # Local import keeps YAML optional for runtime.

            data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("Failed loading template registry source %s: %s", path, exc)
        return None
    return data if isinstance(data, dict) else None


def _build_source_dict(source: Any) -> Dict[str, Any]:
    """将 SourceDoc 转为可序列化的字典。"""
    return {
        "source_id": getattr(source, "source_id", None),
        "filename": getattr(source, "filename", None),
        "source_type": getattr(source, "source_type", None),
        "parent_source_id": getattr(source, "parent_source_id", None),
        "extracted": getattr(source, "extracted", None),
    }


def _summarize_sources(sources: List[Any]) -> Dict[str, Any]:
    """汇总源文档：按类型统计、收集错误信息。"""
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
    """
    根据 profile 配置覆盖填充计划：fill_columns 限制列、special_field_to_column 强制映射。
    """
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
    planner_options: Optional[Dict[str, Any]] = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    用 IR 填充单个模板，复制到临时目录执行后移动到 output_dir。
    返回 (输出文件路径, 填充计划字典)。
    """
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
            planner_options=planner_options,
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
    """
    构建可读输出：meta、summary、fills、sources。
    """
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
    """
    处理文件：提取 → 按 profile 或默认模板填充 → 返回可读输出。

    参数:
        file_paths: 输入文件路径列表
        output_dir: 输出目录
        require_llm: 是否强制使用 LLM
        profile_path: 可选 profile 路径，未指定则用默认模板
    """
    if not file_paths:
        raise ValueError("No input files provided.")

    output_path = ensure_output_dir(output_dir)
    profile = None
    templates = None
    insurance_options: Dict[str, Any] = {}
    router_options: Optional[Dict[str, Any]] = None
    excel_preferred_sheet: Optional[str] = None
    if profile_path:
        profile = load_profile(profile_path)
        templates = profile.get("templates") or []
        if isinstance(profile.get("insurance"), dict):
            insurance_options = dict(profile.get("insurance") or {})
        if isinstance(profile.get("router"), dict) and profile.get("router"):
            router_options = dict(profile.get("router") or {})
        excel_cfg = profile.get("excel") if isinstance(profile, dict) else None
        if isinstance(excel_cfg, dict):
            sheet = excel_cfg.get("sheet")
            if isinstance(sheet, str) and sheet.strip() and sheet.strip().lower() != "auto":
                excel_preferred_sheet = sheet.strip()
            extract_mode = excel_cfg.get("extract_mode")
            if isinstance(extract_mode, str) and extract_mode.strip().lower() in {"auto", "single", "all"}:
                excel_extract_mode = extract_mode.strip().lower()
            else:
                excel_extract_mode = None
        else:
            excel_extract_mode = None
    else:
        templates = resolve_template_paths()
        excel_extract_mode = None

    logger.info("Processing %d files", len(file_paths))
    extractor_options: Dict[str, Any] = {}
    if isinstance(excel_preferred_sheet, str) and excel_preferred_sheet.strip():
        extractor_options["excel_preferred_sheet"] = excel_preferred_sheet.strip()
    if isinstance(excel_extract_mode, str) and excel_extract_mode:
        extractor_options["excel_extract_mode"] = excel_extract_mode
    header_force_map = insurance_options.get("header_force_map")
    if isinstance(header_force_map, dict) and header_force_map:
        extractor_options["header_force_map"] = header_force_map
    ir = run_extract(
        file_paths,
        extractor_options=extractor_options or None,
        router_options=router_options,
    )

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
            output_name = (
                job.get("output_name")
                if isinstance(job.get("output_name"), str) and str(job.get("output_name")).strip()
                else f"filled_{key}.xlsx"
            )
            max_sources = job.get("max_sources") if isinstance(job.get("max_sources"), int) else None
            template_key = job.get("template_key") if isinstance(job.get("template_key"), str) else None
            strategy_key = job.get("strategy_key") if isinstance(job.get("strategy_key"), str) else None
            strategy_plugin = job.get("strategy_plugin") if isinstance(job.get("strategy_plugin"), str) else None
            registry_source = job.get("registry_source") if isinstance(job.get("registry_source"), str) else None
            mapping_constraints = (
                job.get("mapping_constraints")
                if isinstance(job.get("mapping_constraints"), dict)
                else None
            )
            planner_options: Dict[str, Any] = {}
            if insurance_options:
                planner_options["insurance"] = dict(insurance_options)
            if max_sources is not None:
                planner_options.setdefault("insurance", {})["max_sources"] = max_sources
            template_opts: Dict[str, Any] = {}
            if template_key and template_key.strip():
                template_opts["template_key"] = template_key.strip()
            if strategy_key and strategy_key.strip():
                template_opts["strategy_key"] = strategy_key.strip()
            if strategy_plugin and strategy_plugin.strip():
                template_opts["strategy_plugin"] = strategy_plugin.strip()
            if registry_source and registry_source.strip():
                template_opts["registry_source"] = registry_source.strip()
            if mapping_constraints:
                template_opts["mapping_constraints"] = dict(mapping_constraints)
            if registry_source and registry_source.strip():
                registry = _load_template_registry_overrides(registry_source.strip())
                if registry:
                    template_opts["registry"] = registry
            if template_opts:
                template_opts["template_filename"] = template_path.name
                planner_options["template"] = template_opts

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
                planner_options=planner_options or None,
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


def _resolve_output_json_name(output_filename: Optional[str] = None) -> str:
    if output_filename and output_filename.strip():
        return output_filename.strip()
    env_output_name = os.getenv("OUTPUT_JSON_NAME", "").strip()
    if env_output_name:
        return env_output_name
    if os.getenv("OUTPUT_JSON_TIMESTAMP", "").strip().lower() in {"1", "true", "yes", "on"}:
        return f"result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    return "result.json"


def write_json_output(
    result: Dict[str, Any],
    output_dir: str,
    output_filename: Optional[str] = None,
) -> str:
    """
    将结果写入 JSON 文件到 output_dir。
    文件名可由 output_filename 或 OUTPUT_JSON_NAME 环境变量指定。
    """
    output_path = Path(output_dir).resolve()
    output_path.mkdir(parents=True, exist_ok=True)
    json_path = output_path / _resolve_output_json_name(output_filename)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    return str(json_path)

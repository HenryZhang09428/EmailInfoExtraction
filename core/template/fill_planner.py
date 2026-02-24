"""
模板填充规划器 — 轻量编排器 (Lightweight Orchestrator / Facade)
================================================================

本文件是对外的唯一入口，不包含任何类定义。

所有实现已拆分到 core.template.logic 包:
  config.py     — InsuranceConfig, CONFIG
  common.py     — 通用核心层 (SourceDataExtractor, HeaderMatcher, SchemaInspector,
                   LLMResponseParser, GenericFillPlanner 及通用工具函数)
  insurance.py  — 保险业务域层 (InsuranceRecord, InsuranceBusinessLogic,
                   InsuranceSourceSelector, InsuranceFillPlanner 及领域管线函数)

本文件职责:
  1. plan_fill()  — 主入口 (路由: 社保 Profile → InsuranceFillPlanner → GenericFillPlanner)
  2. 向后兼容函数 — 保持原有 _function_name 签名, 委托给新模块中的类/方法
  3. 遗留类别名   — SourceDataManager, ColumnMapper 等旧名称 → 新类名

签名契约: 外部调用方 (tests, core/fill.py, social_security.py) 通过本文件导入，
          本文件保证所有曾经暴露的符号仍然可用。
"""
from __future__ import annotations

from dataclasses import replace
import re
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
)

# ---------------------------------------------------------------------------
# 从拆分模块导入全部符号 — 既是内部使用, 也是向后兼容重导出
# ---------------------------------------------------------------------------

# Layer 0: 配置
from core.template.logic.config import InsuranceConfig, CONFIG  # noqa: F401

# Layer 1: 通用核心
from core.template.logic.common import (  # noqa: F401
    SourceDataExtractor,
    HeaderMatcher,
    SchemaInspector,
    LLMResponseParser,
    GenericFillPlanner,
    _normalize_match_text,
    _normalize_header,
    _normalize_text,
    _header_variants,
    _is_non_empty_value,
    _parse_any_date,
    _looks_like_chinese_name,
    _get_record_value,
    _trim_record_fields,
    _dict_to_fill_plan,
    _empty_fill_plan,
    _merge_debug_info,
    _append_warning,
)

# Layer 2: 保险业务域
from core.template.logic.insurance import (  # noqa: F401
    InsuranceRecord,
    InsuranceBusinessLogic,
    InsuranceSourceSelector,
    InsuranceFillPlanner,
    _parse_year_month,
    _format_year_month,
    _detect_year_month_format_from_value,
    _next_month_yyyymm,
    # 领域管线函数 (直接重导出, 外部可 from core.template.fill_planner import ...)
    _apply_derived_fields_to_records,
    _resolve_year_month_format_from_template,
    _apply_record_gating_to_extracted_json,
    _record_filter_conflicts_with_intent,
    _infer_template_intent_from_mapping,
    _apply_record_type_filter,
    _auto_filter_records_by_template_intent,
    _apply_auto_intent_filter,
    _auto_infer_common_fields,
)

# 项目内部依赖 (仅 plan_fill / 部分 wrapper 需要)
from core.ir import FillPlan, FillPlanTarget
from core.llm import LLMClient
from core.logger import get_logger
from core.template.schema import TemplateSchema
from core.template.template_registry import resolve_strategy

logger = get_logger(__name__)


###############################################################################
#  主入口                                                                      #
###############################################################################


def plan_fill(
    template_schema: TemplateSchema,
    extracted: Any,
    llm: LLMClient,
    template_filename: Optional[str] = None,
    require_llm: bool = False,
    planner_options: Optional[Dict[str, Any]] = None,
) -> FillPlan:
    """
    生成模板填充方案（对外主入口）。

    路由逻辑:
      1. 尝试检测社保专用 profile
      2. 检测是否为保险模板 → InsuranceFillPlanner
      3. 否则 → GenericFillPlanner (fallback)

    签名与原版完全一致，确保向后兼容。
    """
    cfg = _build_insurance_config(planner_options)
    extracted_json = extracted if isinstance(extracted, (dict, list)) else {"data": extracted}
    logic = InsuranceBusinessLogic(cfg)

    strategy_resolution = resolve_strategy(
        template_schema=template_schema,
        template_filename=template_filename,
        planner_options=planner_options,
        require_llm=require_llm,
    )
    if strategy_resolution.strategy is not None:
        try:
            planned = strategy_resolution.strategy(
                template_schema,
                extracted_json if isinstance(extracted_json, dict) else {"data": extracted_json},
                llm,
                template_filename,
                strategy_resolution.context,
            )
            if isinstance(planned, FillPlan):
                dbg = dict(planned.debug or {})
                dbg["strategy_key"] = strategy_resolution.context.strategy_key
                dbg["template_key"] = strategy_resolution.context.template_key
                dbg["strategy_routed"] = True
                planned.debug = dbg
                return planned
        except Exception as exc:
            logger.warning("Template strategy failed, fallback to legacy routing: %s", exc)

    # 从文件名推断意图（legacy 路由）
    template_intent = logic.infer_intent_from_filename(template_filename or "")
    if not template_intent:
        return FillPlan(target=FillPlanTarget(), warnings=["insurance_intent_unknown"], llm_used=False)

    # 优先: 社保专用 profile
    try:
        from core.template.profiles.social_security import (
            detect_social_security_template,
            build_social_security_fill_plan,
        )
        profile = detect_social_security_template(template_schema)
        if profile.is_detected:
            logger.info("Social security template detected: %s (intent=%s)", template_filename, template_intent)
            return build_social_security_fill_plan(
                template_schema,
                extracted_json,
                llm,
                template_filename or "",
                profile,
                template_intent,
                planner_options=planner_options,
            )
    except ImportError:
        logger.debug("Social security profile module not available")
    except Exception as e:
        logger.warning("Social security profile detection failed: %s", e)

    # 保险模板规划
    return _plan_insurance_template_with_llm(
        template_schema, extracted_json, llm, template_filename, template_intent, cfg=cfg
    )


###############################################################################
#  向后兼容薄包装层                                                            #
###############################################################################
# 每个函数保持原签名, 委托给拆分模块中的类/方法。
# 外部代码 (tests, core/fill.py, social_security.py) 通过这些函数名导入使用。


def _plan_insurance_template_with_llm(
    template_schema: TemplateSchema, extracted_json: dict, llm: LLMClient,
    template_filename: Optional[str], template_intent: str,
    cfg: InsuranceConfig = CONFIG,
) -> FillPlan:
    return InsuranceFillPlanner(
        template_schema, extracted_json, llm, template_filename, template_intent, cfg
    ).plan()


def _build_insurance_config(planner_options: Optional[Dict[str, Any]]) -> InsuranceConfig:
    """
    从 planner_options 中构建保险配置覆盖。

    目前支持:
      - insurance.add_keywords
      - insurance.remove_keywords
    """
    if not isinstance(planner_options, dict):
        return CONFIG
    insurance_opts = planner_options.get("insurance")
    if not isinstance(insurance_opts, dict):
        return CONFIG

    add_keywords = insurance_opts.get("add_keywords")
    remove_keywords = insurance_opts.get("remove_keywords")
    updates: Dict[str, Any] = {}
    if isinstance(add_keywords, list):
        cleaned = tuple(str(x).strip() for x in add_keywords if isinstance(x, str) and str(x).strip())
        if cleaned:
            updates["add_keywords"] = cleaned
    if isinstance(remove_keywords, list):
        cleaned = tuple(str(x).strip() for x in remove_keywords if isinstance(x, str) and str(x).strip())
        if cleaned:
            updates["remove_keywords"] = cleaned
    if not updates:
        return CONFIG
    return replace(CONFIG, **updates)


def _infer_insurance_template_intent_from_filename(filename: str) -> Optional[str]:
    return InsuranceBusinessLogic(CONFIG).infer_intent_from_filename(filename)


def _is_insurance_add_remove_template(schema: TemplateSchema) -> bool:
    return InsuranceBusinessLogic(CONFIG).is_insurance_template(schema)


def _get_main_table_region(schema: TemplateSchema) -> Optional[Any]:
    return SchemaInspector(schema).get_main_table_region()


def _get_template_headers(schema: TemplateSchema) -> List[str]:
    return SchemaInspector(schema).get_all_header_paths()


def _extract_template_sample_rows(schema: TemplateSchema) -> List[Dict[str, Any]]:
    return SchemaInspector(schema).get_sample_rows()


def _find_header_path_by_suffix(headers: List[Any], suffix: str) -> Optional[str]:
    return SchemaInspector.find_header_path_by_suffix(headers, suffix)


def _collect_header_columns_by_suffix(headers: List[Any], suffix: str) -> List[str]:
    return SchemaInspector.find_columns_by_suffix(headers, suffix)


def _collect_header_columns_with_paths_by_suffix(headers: List[Any], suffix: str) -> List[Tuple[str, str]]:
    return SchemaInspector.find_columns_with_paths_by_suffix(headers, suffix)


def _extract_records(extracted_json: dict) -> List[Dict[str, Any]]:
    return SourceDataExtractor(extracted_json).get_all_records()


def _extract_records_from_container(container: Any) -> List[Dict[str, Any]]:
    return SourceDataExtractor.extract_from_container(container)


def _extract_records_with_provenance(extracted_json: Any) -> Tuple[List[Dict[str, Any]], Dict[str, int], List[str]]:
    return SourceDataExtractor(extracted_json if isinstance(extracted_json, dict) else {}).get_all_records_with_provenance()


def _count_records_in_sources(extracted_json: Any) -> int:
    return SourceDataExtractor(extracted_json if isinstance(extracted_json, dict) else {}).count_records_in_sources()


def _build_insurance_sources_profile(extracted_json: dict) -> List[Dict[str, Any]]:
    return InsuranceBusinessLogic(CONFIG).build_sources_profile(SourceDataExtractor(extracted_json))


def _replace_records_in_extracted(extracted: Any, records: List[Dict[str, Any]]) -> Any:
    ej = extracted if isinstance(extracted, dict) else {}
    if isinstance(extracted, list): return list(records)
    if isinstance(ej, dict):
        updated = dict(ej)
        tk = None
        for k in SourceDataExtractor.CONTAINER_PRIORITY_KEYS:
            v = updated.get(k)
            if isinstance(v, list) and v and isinstance(v[0], dict):
                tk = k; break
        if tk is None:
            for k, v in updated.items():
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    tk = k; break
        updated[tk or "records"] = list(records)
        return updated
    return extracted


def _find_source_by_id(extracted_json: dict, source_id: str) -> Optional[dict]:
    if not isinstance(extracted_json, dict) or not source_id: return None
    for s in extracted_json.get("sources", []):
        if isinstance(s, dict) and s.get("source_id") == source_id: return s
    return None


def _key_present_in_records(records: List[Dict[str, Any]], key: str) -> bool:
    if not key: return False
    return any(_get_record_value(r, key) is not None for r in records if isinstance(r, dict))


def _collect_email_leave_records(extracted_json: dict) -> Tuple[List[Dict[str, Any]], List[str]]:
    return InsuranceBusinessLogic(CONFIG).collect_email_leave_records(SourceDataExtractor(extracted_json))


def _deduplicate_leave_records(records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
    return InsuranceBusinessLogic.deduplicate_leave_records(records)


def _infer_record_type(record: Dict[str, Any]) -> Optional[str]:
    return InsuranceRecord(record, CONFIG).infer_record_type()


def _infer_record_type_from_sheet(record: Dict[str, Any]) -> Optional[str]:
    return InsuranceRecord(record, CONFIG).infer_type_from_sheet()


def _infer_explicit_record_intent(record: Dict[str, Any]) -> Optional[str]:
    return InsuranceRecord(record, CONFIG).infer_explicit_intent()


def _infer_record_intent(record: Dict[str, Any]) -> Optional[str]:
    return InsuranceRecord(record, CONFIG).infer_intent_from_keys_and_values()


def _infer_source_intent(filename: Optional[str]) -> Optional[str]:
    return InsuranceBusinessLogic(CONFIG).infer_source_intent(filename)


def _detect_template_intent(filename: Optional[str], schema: TemplateSchema) -> Optional[str]:
    return InsuranceBusinessLogic(CONFIG).detect_template_intent(filename, schema)


def _parse_column_mapping(resp: Any) -> Optional[Dict[str, str]]:
    return LLMResponseParser.parse_column_mapping(resp)


def _parse_constant_values(resp: Any) -> Dict[str, str]:
    return LLMResponseParser.parse_constant_values(resp)


def _parse_record_filter(resp: Any) -> Optional[Dict[str, Any]]:
    return LLMResponseParser.parse_record_filter(resp)


def _parse_derived_fields(resp: Any) -> Tuple[List[Dict[str, Any]], List[str]]:
    return LLMResponseParser.parse_derived_fields(resp)


def _match_template_headers_to_source_keys(template_headers: List[str], source_keys: List[str]) -> Dict[str, Tuple[str, int]]:
    return HeaderMatcher(CONFIG.fuzzy_match_threshold).match_headers_to_keys(template_headers, source_keys)


def _collect_extracted_keys(records: List[Dict[str, Any]]) -> List[str]:
    return HeaderMatcher.collect_keys(records)


def _resolve_sheet_name(schema: TemplateSchema, sheet_schema: Any = None) -> str:
    return SchemaInspector(schema).resolve_sheet_name(sheet_schema)


def _extract_sheet_title(sheet_schema: Any) -> Optional[str]:
    return SchemaInspector.extract_sheet_title(sheet_schema)


def _build_header_lookup(headers: List[Any]) -> Dict[str, Any]:
    return SchemaInspector.build_header_lookup(headers)


def _lookup_header(lookup: Dict[str, Any], template_header: str) -> Optional[Any]:
    return SchemaInspector.lookup_header(lookup, template_header)


def _dbg_count_records(extracted_json: dict) -> dict:
    recs = _extract_records(extracted_json) if isinstance(extracted_json, dict) else []
    counts = {"total": 0, "add": 0, "remove": 0, "unknown": 0}
    for r in recs:
        if not isinstance(r, dict): continue
        counts["total"] += 1
        rt = _infer_record_type(r) or _infer_record_type_from_sheet(r)
        counts[rt if rt in ("add", "remove") else "unknown"] += 1
    return counts


def _enrich_mapping_with_fuzzy_match(
    schema: TemplateSchema, extracted_json: dict, header_to_key: Dict[str, str],
) -> Tuple[Dict[str, str], List[str]]:
    if not schema.sheet_schemas: return header_to_key, []
    sheet = schema.sheet_schemas[0]
    if not sheet.regions or not sheet.regions[0].table: return header_to_key, []
    headers = sheet.regions[0].table.header or []
    if not headers: return header_to_key, []
    recs = SourceDataExtractor(extracted_json).get_all_records()
    keys = HeaderMatcher.collect_keys(recs)
    if not keys: return header_to_key, []
    return HeaderMatcher(CONFIG.fuzzy_match_threshold).enrich_mapping(headers, keys, header_to_key)


def _build_fill_plan_from_mapping(
    schema: TemplateSchema, extracted_json: dict, header_to_key: Dict[str, str],
    mapping_warnings: Optional[List[str]] = None, constant_values: Optional[Dict[str, str]] = None,
    template_intent: Optional[str] = None,
) -> Optional[dict]:
    records, _, _ = _extract_records_with_provenance(extracted_json)
    if not records: return None
    if template_intent in ("add", "remove"):
        filtered = [r for r in records if (_infer_record_type(r) or _infer_record_type_from_sheet(r)) == template_intent]
        if filtered:
            records = filtered
        else:
            if mapping_warnings is not None: mapping_warnings.append("record_type_filtered_empty")
            records = []
    if not records: return None
    return GenericFillPlanner(schema).build_fill_plan_from_mapping(
        records, header_to_key, constant_values, mapping_warnings
    )


def build_fallback_fill_plan(schema: TemplateSchema, extracted_json: dict) -> Optional[dict]:
    logic = InsuranceBusinessLogic(CONFIG)
    return GenericFillPlanner(schema).build_fallback_plan(extracted_json, logic.synonym_resolver)


# ============================================================================
# 通用管线辅助函数
# ============================================================================


def _attach_template_headers_to_derived_fields(
    derived_fields: List[Dict[str, Any]], column_mapping: Dict[str, str],
) -> List[Dict[str, Any]]:
    if not derived_fields or not column_mapping: return derived_fields
    hbnk: Dict[str, List[str]] = {}
    for h, k in column_mapping.items():
        if isinstance(k, str): hbnk.setdefault(k, []).append(h)
    return [{**item, "_template_headers": hbnk.get(item.get("new_key"), [])} if isinstance(item, dict) else item for item in derived_fields]


def _promote_derived_constants(
    derived_fields: List[Dict[str, Any]], records: List[Dict[str, Any]],
    column_mapping: Dict[str, str], constant_values: Dict[str, str], warnings: List[str],
) -> Tuple[Dict[str, str], Dict[str, str]]:
    if not derived_fields or not records: return column_mapping, constant_values
    um, uc = dict(column_mapping), dict(constant_values)
    for item in derived_fields:
        if not isinstance(item, dict): continue
        nk = item.get("new_key")
        if not isinstance(nk, str) or not nk: continue
        vals = [str(r.get(nk)).strip() if _is_non_empty_value(r.get(nk)) else "" for r in records if isinstance(r, dict)]
        ne = [v for v in vals if v]
        if ne and all(v == ne[0] for v in ne) and len(ne) == len(vals):
            for h in [h for h, k in um.items() if k == nk]:
                if h in uc:
                    if uc[h] != ne[0]: warnings.append(f"derived constant for '{h}' differs; keeping existing")
                else:
                    uc[h] = ne[0]; um.pop(h, None)
    return um, uc


def _count_records_in_extracted(extracted: Any) -> int:
    if extracted is None: return 0
    if isinstance(extracted, list):
        return len(extracted) if extracted and isinstance(extracted[0], dict) else 0
    if isinstance(extracted, dict):
        for k in ("data", "records", "rows", "items"):
            if k in extracted and isinstance(extracted[k], list): return len(extracted[k])
        for v in extracted.values():
            if isinstance(v, list) and v and isinstance(v[0], dict): return len(v)
    return 0


def _apply_record_filter(extracted_json: dict, record_filter: Optional[Dict[str, Any]]) -> dict:
    if record_filter is None: return extracted_json
    fld, vals, excl = record_filter.get("field"), record_filter.get("values", []), record_filter.get("exclude", False)
    if not fld or not vals: return extracted_json
    nv = [str(v).lower().strip() for v in vals]
    def ok(r: dict) -> bool:
        rv = r.get(fld)
        if rv is None: return excl
        m = str(rv).lower().strip() in nv
        return not m if excl else m
    def fl(recs: list) -> list: return [r for r in recs if isinstance(r, dict) and ok(r)]
    def fe(ex: Any) -> Any:
        if ex is None: return None
        if isinstance(ex, list): return fl(ex) if ex and isinstance(ex[0], dict) else ex
        if isinstance(ex, dict):
            return {k: (fl(v) if isinstance(v, list) and v and isinstance(v[0], dict) else v) for k, v in ex.items()}
        return ex
    filt: Dict[str, Any] = {}
    if "sources" in extracted_json:
        filt["sources"] = []
        for s in extracted_json["sources"]:
            if not isinstance(s, dict): filt["sources"].append(s); continue
            fs = dict(s); ex = s.get("extracted")
            if ex is not None: fs["extracted"] = fe(ex)
            filt["sources"].append(fs)
    if "merged" in extracted_json and isinstance(extracted_json["merged"], dict):
        filt["merged"] = {k: (fl(v) if isinstance(v, list) and v and isinstance(v[0], dict) else v) for k, v in extracted_json["merged"].items()}
    for k in extracted_json:
        if k not in filt: filt[k] = extracted_json[k]
    return filt


def _infer_fee_month(
    schema: TemplateSchema, records: List[Dict[str, Any]], intent: Optional[str],
    column_mapping: Dict[str, str], constant_values: Dict[str, str], warnings: List[str],
) -> Tuple[Dict[str, str], Dict[str, str]]:
    return InsuranceBusinessLogic(CONFIG).infer_fee_month_for_records(schema, records, intent, column_mapping, constant_values, warnings)


def _find_fee_month_headers(schema: TemplateSchema) -> List[str]:
    return InsuranceBusinessLogic(CONFIG).find_fee_month_headers(schema)


def _extract_fee_month_from_record(record: Dict[str, Any], intent: Optional[str]) -> Optional[Tuple[int, int]]:
    return InsuranceBusinessLogic(CONFIG).extract_fee_month(record, intent)


def _gate_records_by_filled_ratio(
    schema: TemplateSchema, records: List[Dict[str, Any]], warnings: List[str],
    min_ratio: float = 0.4, fallback_top_n: int = 5,
) -> List[Dict[str, Any]]:
    if not records: return records
    matcher = HeaderMatcher(CONFIG.fuzzy_match_threshold)
    t_headers = SchemaInspector(schema).get_all_header_paths()
    sk: Set[str] = set()
    for r in records:
        if isinstance(r, dict): sk.update(k for k in r if isinstance(k, str))
    matched = matcher.match_headers_to_keys(t_headers, sorted(sk))
    mk = [k for k, _ in matched.values()]
    if not mk: warnings.append("record_gating: no matched keys; skipping"); return records
    scored = [(sum(1 for k in mk if _is_non_empty_value(r.get(k))) / len(mk) if isinstance(r, dict) else 0.0, r) for r in records]
    kept = [r for ratio, r in scored if ratio >= min_ratio]
    if not kept:
        scored.sort(key=lambda x: x[0], reverse=True)
        kept = [r for _, r in scored[:fallback_top_n]]
        warnings.append("record_gating: all below threshold; keeping top")
    return kept


# ============================================================================
# 遗留类别名 — 外部可能以旧名 import
# ============================================================================
SourceDataManager = SourceDataExtractor
ColumnMapper = HeaderMatcher
TemplateAnalyzer = SchemaInspector
RecordProcessor = InsuranceBusinessLogic
LLMPlanner = LLMResponseParser
HeuristicPlanner = InsuranceSourceSelector

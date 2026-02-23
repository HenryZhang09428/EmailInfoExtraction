#!/usr/bin/env python3
"""
One-shot generator: reads fill_planner.py and extracts the insurance layer
into core/template/logic/insurance.py. Delete this script after running.
"""
import pathlib

SRC = pathlib.Path(__file__).resolve().parent.parent / "fill_planner.py"
DST = pathlib.Path(__file__).resolve().parent / "insurance.py"

lines = SRC.read_text(encoding="utf-8").splitlines(keepends=True)

# ---- Header / imports ----
header = '''\
"""
保险业务域层 (Insurance Domain — Layer 2)
==========================================

本模块封装所有社保增减员的业务规则、中文关键词和特定策略。

包含:
  - 领域工具函数 (_parse_year_month, _format_year_month 等)
  - InsuranceRecord           — 单条记录的领域封装 (意图推断)
  - InsuranceBusinessLogic    — 意图检测/记录过滤/去重/费用年月计算
  - InsuranceSourceSelector   — LLM + 启发式数据源选择
  - InsuranceFillPlanner      — 编排保险业务流程 → FillPlan
  - 领域管线函数 (_apply_record_type_filter, _auto_infer_common_fields 等)

依赖: config.py (InsuranceConfig, CONFIG) + common.py (通用工具 + 类)
      不依赖 fill_planner.py (单向依赖, 避免循环引用)
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
)

from openpyxl.utils import get_column_letter, range_boundaries

from core.ir import FillPlan, FillPlanTarget
from core.llm import LLMClient
from core.logger import get_logger
from core.template.prompts import build_insurance_param_prompt
from core.template.schema import TemplateSchema

from core.template.logic.config import InsuranceConfig, CONFIG
from core.template.logic.common import (
    SourceDataExtractor,
    HeaderMatcher,
    SchemaInspector,
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
    _merge_debug_info,
    _dict_to_fill_plan,
)

logger = get_logger(__name__)

'''

# ---- Extract code blocks from fill_planner.py (1-indexed lines) ----

def extract(start_marker, end_marker):
    """Extract lines between two markers (inclusive of start, exclusive of end)."""
    collecting = False
    result = []
    for line in lines:
        stripped = line.strip()
        if not collecting and start_marker in stripped:
            collecting = True
        if collecting:
            if end_marker and end_marker in stripped and result:
                break
            result.append(line)
    return "".join(result)

def extract_range(start_line, end_line):
    """Extract lines by 1-indexed line numbers (inclusive)."""
    return "".join(lines[start_line - 1 : end_line])

# 1. Domain utility functions: _parse_year_month .. _next_month_yyyymm (lines 179-246)
domain_utils = extract_range(179, 247)

# 2. InsuranceRecord class (lines 1101-1196)
insurance_record = extract_range(1096, 1196)

# 3. InsuranceBusinessLogic class (lines 1198-1545)
insurance_logic = extract_range(1198, 1545)

# 4. InsuranceSourceSelector class (lines 1547-1655)
insurance_selector = extract_range(1547, 1656)

# 5. InsuranceFillPlanner class (lines 1658-1833)
insurance_planner = extract_range(1658, 1833)

# 6. Pipeline functions from backward compat layer
pipeline_separator = '''

# ============================================================================
# 领域管线函数 — 向后兼容 (从 fill_planner.py 迁移)
# ============================================================================
# 这些函数以前定义在 fill_planner.py 的兼容层中，因为它们包含大量领域逻辑，
# 按"瘦编排器"原则迁移到本模块。fill_planner.py 通过 import 重新导出它们。

'''

# _apply_derived_fields_to_records (lines 2131-2157)
apply_derived = extract_range(2131, 2158)

# _resolve_year_month_format_from_template (lines 2160-2167)
resolve_ym = extract_range(2160, 2168)

# _apply_record_gating_to_extracted_json (lines 2235-2271)
apply_gating = extract_range(2235, 2272)

# _record_filter_conflicts_with_intent (lines 2274-2285)
record_filter_conflict = extract_range(2274, 2286)

# _infer_template_intent_from_mapping (lines 2288-2305)
infer_intent_mapping = extract_range(2288, 2306)

# _apply_record_type_filter (lines 2308-2328)
apply_record_type = extract_range(2308, 2329)

# _auto_filter_records_by_template_intent + _apply_auto_intent_filter (lines 2331-2384)
auto_filter = extract_range(2331, 2385)

# _auto_infer_common_fields (lines 2424-2496)
auto_infer = extract_range(2424, 2497)

# ---- Assemble ----
content = (
    header
    + domain_utils + "\n\n"
    + insurance_record + "\n"
    + insurance_logic + "\n"
    + insurance_selector + "\n"
    + insurance_planner + "\n"
    + pipeline_separator
    + apply_derived + "\n\n"
    + resolve_ym + "\n\n"
    + apply_gating + "\n\n"
    + record_filter_conflict + "\n\n"
    + infer_intent_mapping + "\n\n"
    + apply_record_type + "\n\n"
    + auto_filter + "\n\n"
    + auto_infer
)

DST.write_text(content, encoding="utf-8")
print(f"Written {len(content)} chars ({content.count(chr(10))} lines) to {DST}")

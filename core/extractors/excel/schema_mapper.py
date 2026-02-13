"""
SchemaMapper: semantic key inference and conflict resolution.

Translates raw header text into semantic keys (``name``, ``employee_id``,
``start_date``, …) using a combination of:
1. LLM-inferred mappings (primary)
2. Deterministic keyword rules (override / fallback)
3. Sample-value heuristics (last resort)
4. Conflict resolution for duplicate keys
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from core.extractors.excel.config import (
    LONG_DIGITS_RE,
    PURE_NUMBER_RE,
    DATE_LIKE_RE,
    ALPHA_LIKE_RE,
    REMOVE_INTENT_KEYWORDS,
    TERMINATION_KEYS,
    ExtractorConfig,
    DEFAULT_CONFIG,
)
from core.extractors.excel.data_cleaner import DataCleaner


class SchemaMapper:
    """
    Map header paths to semantic keys and resolve conflicts.

    Typical call sequence inside the extraction pipeline::

        mapper = SchemaMapper()
        mapping = mapper.sanitize(llm_output, header_paths)
        mapping, n = mapper.apply_deterministic_overrides(mapping, header_paths)
        coverage = mapper.compute_coverage(mapping, header_paths)
        if coverage < threshold:
            mapping = mapper.fallback_infer(header_paths, summaries)
        mapping = mapper.resolve_conflicts(mapping, header_paths, warnings)
        header_paths, mapping = mapper.apply_forced_mappings(header_paths, mapping)
        mapping = mapper.ensure_termination_reason(df, header_paths, mapping, ...)
        keys = mapper.build_semantic_keys(header_paths, mapping, warnings)
    """

    def __init__(self, cfg: ExtractorConfig = DEFAULT_CONFIG):
        self._cfg = cfg

    # ------------------------------------------------------------------
    # Sanitize / validate raw LLM output
    # ------------------------------------------------------------------

    @staticmethod
    def sanitize(
        raw: Dict[str, Any],
        header_paths: List[str],
    ) -> Dict[str, str]:
        """Keep only valid string values for known header paths."""
        cleaned: Dict[str, str] = {}
        for hp in header_paths:
            val = raw.get(hp)
            cleaned[hp] = val.strip() if isinstance(val, str) else ""
        return cleaned

    # ------------------------------------------------------------------
    # Deterministic keyword map (highest priority)
    # ------------------------------------------------------------------

    @staticmethod
    def deterministic_header_map(header_text: str) -> Optional[str]:
        """
        Return a semantic key if the header text matches a hard-coded rule,
        or ``None`` if no deterministic match is found.
        """
        header = str(header_text or "").strip()
        if not header:
            return None
        tc = DataCleaner.normalize_header_compact(header)

        # Order matters: more-specific rules first.
        if "供应商发薪编码名称" in tc:
            return "vendor_pay_code_name"
        if "供应商发薪编码" in tc:
            return "vendor_pay_code"
        if "个人电脑编号" in tc:
            return "employee_id"
        if "员工工号" in tc or "人员编号" in tc or "工号" in tc:
            return "employee_id"
        if "员工姓名" in tc or "姓名" in tc:
            return "name"
        if "性别" in tc:
            return "gender"
        if "身份证号码" in tc or "证件号码" in tc:
            return "id_number"
        if "民族" in tc:
            return "ethnicity"
        if "学历" in tc:
            return "education_level"
        if "职工身份" in tc:
            return "employee_status"
        if "入职日期" in tc or "到岗日期" in tc:
            return "start_date"
        if "参加保险年月" in tc:
            return "start_date"
        if "月缴费工资" in tc:
            return "monthly_contribution"
        if "户口情况" in tc:
            return "household_registration"
        if "参加保险情况" in tc:
            return "pension_insurance_status"
        if "减员年月" in tc or "离职年月" in tc:
            return "termination_date"
        if "备注" in tc:
            return "remark"
        if "甲方公司" in tc and ("劳动合同" in tc or "合同" in tc):
            return "contract_company"
        if "合同起始日期" in tc:
            return "contract_start_date"
        if "合同终止日期" in tc:
            return "contract_end_date"
        if "签订日期" in tc:
            return "sign_date"
        if "高峰期预计到期日期" in tc:
            return "peak_end_date"
        if "培训服务协议开始日期" in tc:
            return "training_start_date"
        if "培训协议结束日期" in tc:
            return "training_end_date"
        if "竞业禁止协议签订日期" in tc:
            return "noncompete_sign_date"
        if "竞业禁止解除日期" in tc:
            return "noncompete_end_date"
        if "所属组织单位" in tc or "组织单位" in tc:
            return "org_unit"
        if "公司名称" in tc:
            return "company"
        if "工作地点" in tc or "工作地" in tc:
            return "work_location"
        if "所属地区" in tc:
            return "region"
        if "职位名称" in tc or "岗位名称" in tc:
            return "position"
        if "岗位属性" in tc:
            return "position_attribute"
        if "雇佣状态" in tc or "在职状态" in tc:
            return "employment_status"
        if "平台供应商" in tc or "供应商" in tc:
            return "vendor"
        return None

    @staticmethod
    def apply_deterministic_overrides(
        mapping: Dict[str, str],
        header_paths: List[str],
    ) -> Tuple[Dict[str, str], int]:
        """Apply deterministic rules on top of *mapping*, counting overrides."""
        updated = dict(mapping or {})
        overrides = 0
        for hp in header_paths:
            det = SchemaMapper.deterministic_header_map(hp)
            if det and (updated.get(hp) or "").strip() != det:
                updated[hp] = det
                overrides += 1
        return updated, overrides

    # ------------------------------------------------------------------
    # Header-based inference (secondary)
    # ------------------------------------------------------------------

    @staticmethod
    def infer_key_from_header(header: str) -> str:
        """Infer a semantic key purely from header text keywords."""
        text = DataCleaner.normalize_header_compact(header)
        if not text:
            return ""
        # Specific disambiguation first
        if any(t in text for t in ["岗位属性", "岗位类别", "岗位序列", "一线", "二线", "三线", "职级", "层级"]) or (
            "属性" in text and any(t in text for t in ["岗位", "职位", "职务", "position", "title"])
        ):
            return "position_attribute"
        if any(t in text for t in ["职位名称", "岗位名称"]):
            return "position"
        if any(t in text for t in ["甲方公司", "甲方", "劳动合同", "合同"]) and any(t in text for t in ["公司", "company"]):
            return "contract_company"
        if any(t in text for t in ["所属组织", "组织单位", "组织", "归属单位"]):
            return "company"
        if any(t in text for t in ["姓名", "name"]):
            return "name"
        if "个人电脑编号" in text:
            return "employee_id"
        if any(t in text for t in ["工号", "员工编号", "employee", "emp no", "employee id", "staff id"]):
            return "employee_id"
        if any(t in text for t in ["证件", "身份证", "id", "证号", "document"]):
            return "id_number"
        if "民族" in text:
            return "ethnicity"
        if "学历" in text:
            return "education_level"
        if "职工身份" in text:
            return "employee_status"
        if any(t in text for t in ["手机号", "电话", "联系方式", "mobile", "phone", "tel"]):
            return "phone"
        if any(t in text for t in ["入职", "onboard", "start date", "参加保险年月"]):
            return "start_date"
        if any(t in text for t in ["离职", "退场", "offboard", "end date", "离岗"]):
            return "end_date"
        if "合同起始日期" in text:
            return "contract_start_date"
        if "合同终止日期" in text:
            return "contract_end_date"
        if "签订日期" in text:
            return "sign_date"
        if "高峰期预计到期日期" in text:
            return "peak_end_date"
        if "培训服务协议开始日期" in text:
            return "training_start_date"
        if "培训协议结束日期" in text:
            return "training_end_date"
        if "竞业禁止协议签订日期" in text:
            return "noncompete_sign_date"
        if "竞业禁止解除日期" in text:
            return "noncompete_end_date"
        if any(t in text for t in ["出生", "birth"]):
            return "birth_date"
        if any(t in text for t in ["邮箱", "email"]):
            return "email"
        if any(t in text for t in ["部门", "department"]):
            return "department"
        if any(t in text for t in ["岗位", "职位", "职务", "position", "title"]):
            return "position"
        if "月缴费工资" in text:
            return "monthly_contribution"
        if "户口情况" in text:
            return "household_registration"
        if "参加保险情况" in text:
            return "pension_insurance_status"
        if "备注" in text:
            return "remark"
        if "公司名称" in text:
            return "company"
        if any(t in text for t in ["公司", "单位", "company"]):
            return "company"
        if any(t in text for t in ["工作地", "所属地区", "地区"]):
            return "address"
        if any(t in text for t in ["地址", "address"]):
            return "address"
        if any(t in text for t in ["金额", "费用", "薪资", "amount", "price", "fee"]):
            return "amount"
        if any(t in text for t in ["备注", "说明", "comment", "remark"]):
            return "remark"
        return ""

    # ------------------------------------------------------------------
    # Sample-value-based inference (last resort)
    # ------------------------------------------------------------------

    @staticmethod
    def infer_key_from_values(samples: List[str]) -> str:
        """Regex heuristics over sample values (phone, date, ID, amount)."""
        if not samples:
            return ""
        joined = " ".join(samples)
        if re.search(r"\b1\d{10}\b", joined):
            return "phone"
        if re.search(r"\b\d{4}[-/]\d{1,2}[-/]\d{1,2}\b", joined) or re.search(r"\d{4}年\d{1,2}月\d{1,2}日", joined):
            return "date"
        if re.search(r"\b\d{15}(\d{2}[0-9Xx])?\b", joined):
            return "id_number"
        if re.search(r"[¥￥$]|金额|费用|amount|price", joined, re.IGNORECASE):
            return "amount"
        return ""

    @staticmethod
    def infer_key_from_samples(samples: List[str]) -> str:
        """Statistical heuristics over sample values (gender, name, date, …)."""
        if not samples:
            return ""
        total = 0
        long_digit = name_like = gender_like = date_like = 0
        company_like = position_attr_like = employment_status_like = 0
        for raw in samples:
            if raw is None:
                continue
            v = str(raw).strip()
            if not v:
                continue
            total += 1
            if LONG_DIGITS_RE.match(v):
                long_digit += 1
            if DATE_LIKE_RE.match(v):
                date_like += 1
            if v in {"男", "女"}:
                gender_like += 1
            if re.fullmatch(r"[\u4e00-\u9fff]{2,4}", v):
                name_like += 1
            if "有限公司" in v:
                company_like += 1
            if v in {"一线", "二线", "三线"} or "线" in v:
                position_attr_like += 1
            if v in {"在职", "离职", "执行中", "已签署"}:
                employment_status_like += 1
        if total == 0:
            return ""
        if gender_like / total >= 0.6:
            return "gender"
        if name_like / total >= 0.6:
            return "name"
        if long_digit / total >= 0.6:
            return "employee_id"
        if date_like / total >= 0.5:
            return "date"
        if position_attr_like / total >= 0.5:
            return "position_attribute"
        if employment_status_like / total >= 0.5:
            return "employment_status"
        if company_like / total >= 0.5:
            return "company"
        return SchemaMapper.infer_key_from_values([str(s) for s in samples if s is not None])

    # ------------------------------------------------------------------
    # Fallback: build mapping entirely from header + sample heuristics
    # ------------------------------------------------------------------

    @staticmethod
    def fallback_infer(
        header_paths: List[str],
        column_summaries: List[Dict[str, Any]],
    ) -> Dict[str, str]:
        summary_by_header = {c.get("header_path"): c for c in column_summaries if isinstance(c, dict)}
        mapping: Dict[str, str] = {}
        for hp in header_paths:
            inferred = SchemaMapper.infer_key_from_header(hp)
            samples = summary_by_header.get(hp, {}).get("samples") or []
            is_generic = not hp or re.match(r"^col_\d+$", str(hp).strip())
            if not inferred or is_generic:
                inferred = SchemaMapper.infer_key_from_samples([str(s) for s in samples if s is not None])
            mapping[hp] = inferred
        return mapping

    # ------------------------------------------------------------------
    # Conflict resolution
    # ------------------------------------------------------------------

    @staticmethod
    def resolve_conflicts(
        mapping: Dict[str, str],
        header_paths: List[str],
        warnings: List[str],
    ) -> Dict[str, str]:
        """
        Disambiguate when multiple headers map to the same semantic key.

        Uses core-key priority rules, then header-keyword re-mapping.
        """
        if not isinstance(mapping, dict) or not header_paths:
            return mapping
        updated, core_changed = SchemaMapper._resolve_core_collisions(mapping, header_paths)
        counts: Dict[str, int] = {}
        for hp in header_paths:
            k = (updated.get(hp) or "").strip()
            if k:
                counts[k] = counts.get(k, 0) + 1
        conflicts = {k for k, v in counts.items() if v > 1}
        if not conflicts:
            return updated
        changed = core_changed
        for hp in header_paths:
            cur = (updated.get(hp) or "").strip()
            if not cur or cur not in conflicts:
                continue
            proposed = SchemaMapper._propose_disambiguation(hp)
            if proposed and proposed != cur:
                updated[hp] = proposed
                changed = True
        if changed:
            warnings.append("semantic_key_collision_resolved")
            warnings.append("semantic_core_key_collision_resolved")
        return updated

    # ------------------------------------------------------------------
    # Forced header normalisation + hard-coded social-security mappings
    # ------------------------------------------------------------------

    @staticmethod
    def apply_forced_mappings(
        header_paths: List[str],
        mapping: Dict[str, str],
    ) -> Tuple[List[str], Dict[str, str]]:
        """
        Normalise header paths and apply social-security forced mappings.

        Returns ``(normalised_headers, updated_mapping)``.
        """
        force_map = {
            "姓名": "name", "性别": "gender",
            "个人电脑编号": "employee_id",
            "身份证号码": "id_number", "证件号码": "id_number",
            "民族": "ethnicity", "学历": "education_level",
            "职工身份": "employee_status",
            "参加保险年月": "start_date", "参保年月": "start_date", "缴费年月": "start_date",
            "月缴费工资": "monthly_contribution",
            "户口情况": "household_registration",
            "参加保险情况": "pension_insurance_status",
            "减员年月": "termination_date", "离职年月": "termination_date",
            "备注": "remark",
        }
        normed_headers: List[str] = []
        updated: Dict[str, str] = {}
        for hp in header_paths:
            n = DataCleaner.normalize_header_for_semantic_key(hp)
            normed_headers.append(n)
            existing = (mapping.get(hp) or "").strip()
            forced = force_map.get(n)
            if n in updated:
                if not updated[n] and (forced or existing):
                    updated[n] = forced or existing
            else:
                updated[n] = forced or existing
        return normed_headers, updated

    # ------------------------------------------------------------------
    # Termination-reason enrichment (remove-intent sheets)
    # ------------------------------------------------------------------

    @staticmethod
    def ensure_termination_reason(
        df: pd.DataFrame,
        header_paths: List[str],
        mapping: Dict[str, str],
        data_start_idx: int,
        sheet_name: Any,
        max_rows: int = 1000,
    ) -> Dict[str, str]:
        """Add ``termination_reason`` key for reason-like columns on remove sheets."""
        if not header_paths:
            return mapping
        if not SchemaMapper._is_remove_intent(sheet_name, header_paths, mapping):
            return mapping
        updated = dict(mapping or {})
        end = min(len(df), data_start_idx + max_rows)
        for ci, hp in enumerate(header_paths):
            ht = DataCleaner.normalize_header_compact(hp)
            if "原因" not in hp and "原因" not in ht and "reason" not in ht:
                continue
            if "reason" in ht or any(k in ht for k in REMOVE_INTENT_KEYWORDS) or "原因" in hp:
                col_vals = [DataCleaner.cell_to_str(df.iat[r, ci]) for r in range(data_start_idx, end)]
                ne, text_r, long_r, num_r = SchemaMapper._column_text_profile(col_vals)
                if ne == 0 or text_r < 0.3 or long_r > 0.3 or num_r > 0.6:
                    continue
                updated[hp] = "termination_reason"
        return updated

    # ------------------------------------------------------------------
    # Coverage calculation
    # ------------------------------------------------------------------

    @staticmethod
    def compute_coverage(
        mapping: Dict[str, str],
        header_paths: List[str],
    ) -> float:
        if not header_paths:
            return 0.0
        relevant = [h for h in header_paths if h and not re.match(r"^col_\d+$", str(h).strip())]
        if not relevant:
            relevant = header_paths
        mapped = sum(1 for h in relevant if mapping.get(h, "").strip())
        return mapped / max(1, len(relevant))

    # ------------------------------------------------------------------
    # Build final de-duplicated key list
    # ------------------------------------------------------------------

    @staticmethod
    def build_semantic_keys(
        header_paths: List[str],
        mapping: Dict[str, str],
        warnings: List[str],
    ) -> List[str]:
        """De-duplicate keys, appending ``__<col_idx>`` suffixes on collision."""
        keys: List[str] = []
        used: Dict[str, int] = {}
        for idx, hp in enumerate(header_paths):
            raw = mapping.get(hp, "").strip()
            if not raw:
                keys.append("")
                continue
            if raw in used:
                used[raw] += 1
                keys.append(f"{raw}__{idx + 1}")
            else:
                used[raw] = 1
                keys.append(raw)
        if any(c > 1 for c in used.values()):
            warnings.append("列语义存在重复键，已自动去重")
        return keys

    # ------------------------------------------------------------------
    # Header-path normalisation (display-level)
    # ------------------------------------------------------------------

    @staticmethod
    def normalize_header_paths_and_summaries(
        header_paths: List[str],
        summaries: List[Dict[str, Any]],
    ) -> Tuple[List[str], List[Dict[str, Any]]]:
        normed: List[str] = []
        for i, hp in enumerate(header_paths):
            n = DataCleaner.normalize_header_for_semantic_key(hp) or hp
            normed.append(n)
            if i < len(summaries):
                summaries[i]["header_path"] = n
        return normed, summaries

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _is_remove_intent(
        sheet_name: Any,
        header_paths: List[str],
        mapping: Dict[str, str],
    ) -> bool:
        name_text = DataCleaner.normalize_header_compact(sheet_name)
        header_text = DataCleaner.normalize_header_compact(" ".join(header_paths or []))
        if any(k in name_text for k in REMOVE_INTENT_KEYWORDS):
            return True
        if any(k in header_text for k in REMOVE_INTENT_KEYWORDS):
            return True
        for v in (mapping or {}).values():
            if (v or "").strip() in TERMINATION_KEYS:
                return True
        return False

    @staticmethod
    def _column_text_profile(values: List[str]) -> Tuple[int, float, float, float]:
        non_empty = [v for v in values if v]
        if not non_empty:
            return 0, 0.0, 0.0, 0.0
        text = long = num = 0
        for v in non_empty:
            if LONG_DIGITS_RE.match(v):
                long += 1
            if PURE_NUMBER_RE.match(v):
                num += 1
            if ALPHA_LIKE_RE.search(v) and not PURE_NUMBER_RE.match(v) and not LONG_DIGITS_RE.match(v):
                text += 1
        d = max(1, len(non_empty))
        return len(non_empty), text / d, long / d, num / d

    @staticmethod
    def _resolve_core_collisions(
        mapping: Dict[str, str],
        header_paths: List[str],
    ) -> Tuple[Dict[str, str], bool]:
        core_rules: Dict[str, List[str]] = {
            "employee_id": ["员工工号", "工号", "人员编号"],
            "name": ["姓名", "员工姓名"],
            "company": ["公司名称"],
            "position": ["职位名称", "岗位名称"],
            "start_date": ["入职日期", "到岗日期"],
            "date": ["日期"],
        }
        updated = dict(mapping)
        changed = False
        for core_key, kws in core_rules.items():
            cands = [h for h in header_paths if (updated.get(h) or "").strip() == core_key]
            if len(cands) <= 1:
                continue
            ranked = []
            for hp in cands:
                r = SchemaMapper._header_keyword_rank(hp, kws)
                if r is not None:
                    ranked.append((r, hp))
            ranked.sort(key=lambda x: (x[0], cands.index(x[1])))
            keep = ranked[0][1] if ranked else None
            for hp in cands:
                if keep and hp == keep:
                    continue
                remap = SchemaMapper._remap_non_core(hp)
                updated[hp] = remap if remap and remap != core_key else ""
                changed = True
        return updated, changed

    @staticmethod
    def _header_keyword_rank(header_path: str, keywords: List[str]) -> Optional[int]:
        h = str(header_path or "").strip()
        if not h:
            return None
        tc = re.sub(r"[\s\W_]+", "", h.lower())
        for idx, kw in enumerate(keywords):
            if kw in tc:
                return idx
        return None

    @staticmethod
    def _remap_non_core(header_path: str) -> str:
        h = str(header_path or "").strip()
        if not h:
            return ""
        tc = re.sub(r"[\s\W_]+", "", h.lower())
        if "雇佣状态" in tc or "在职状态" in tc:
            return "employment_status"
        if "员工子组" in tc:
            return "subgroup"
        if "岗" in tc and "岗位" not in tc and "职位" not in tc:
            return "job_family"
        if "合同期限单位" in tc:
            return "duration_unit"
        if "试用期" in tc and "单位" in tc:
            return "probation_unit"
        if "岗位职能类" in tc or "岗位职能" in tc:
            return "job_function"
        if "供应商" in tc:
            return "vendor"
        if "所属组织单位" in tc or "组织单位" in tc:
            return "org_unit"
        return ""

    @staticmethod
    def _propose_disambiguation(header_path: str) -> str:
        h = str(header_path or "").strip()
        if not h:
            return ""
        if re.search(r"(岗位属性|一线|二线|三线|职级|层级|序列)", h):
            return "position_attribute"
        if re.search(r"(职位名称|岗位名称)", h):
            return "position"
        if re.search(r"(甲方公司|劳动合同)", h):
            return "contract_company"
        if re.search(r"(公司名称|所属组织单位|组织单位|组织)", h):
            return "company"
        return ""

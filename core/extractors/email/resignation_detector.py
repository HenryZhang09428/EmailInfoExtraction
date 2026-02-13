"""
Resignation / leave-line detection for email bodies.

This is **critical business logic**: it identifies employee departure
records from free-text email bodies using a two-stage approach:

1. Lightweight regex heuristic to find candidate lines.
2. Optional LLM refinement + validation.
3. Merge of regex and LLM results with conflict resolution.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from core.extractors.email.config import (
    CHINESE_NAME_RE,
    DIGIT_ID_RE,
    ISO_DATE_STRICT_RE,
    LEAVE_DATE_PATTERNS,
    LEAVE_EXTRACT_DATE_PATTERNS,
    LEAVE_KEYWORD_RE,
    NAME_ID_CAPTURE_RE,
    NAME_ID_RE,
)
from core.extractors.email.content_cleaner import ContentCleaner
from core.extractors.email.date_parser import DateParser
from core.logger import get_logger

logger = get_logger(__name__)


class ResignationDetector:
    """Detect and extract employee leave/resignation records from email body text."""

    def __init__(self, llm: Any = None, prompts: Optional[dict] = None) -> None:
        self._llm = llm
        self._prompts = prompts or {}

    # ------------------------------------------------------------------
    # Stage 1: heuristic line detection
    # ------------------------------------------------------------------

    @staticmethod
    def detect_leave_lines(body_text: str) -> Tuple[List[str], dict]:
        """Find lines that likely describe employee departures.

        A line matches when it contains:
        - A Chinese name followed by an employee ID in parentheses.
        - One of the leave keywords.
        - A date-like fragment.

        Returns ``(matched_lines, detection_meta)``.
        """
        if not body_text or not body_text.strip():
            return [], {"triggered": False, "reason": "empty_body"}

        lines = body_text.splitlines()
        matched_lines: List[str] = []
        high_confidence_count = 0

        for line in lines:
            line = line.strip()
            if not line:
                continue

            has_name_id = bool(NAME_ID_RE.search(line))
            has_leave_keyword = bool(LEAVE_KEYWORD_RE.search(line))
            has_date = any(dp.search(line) for dp in LEAVE_DATE_PATTERNS)

            if has_name_id and has_leave_keyword and has_date:
                matched_lines.append(line)
                high_confidence_count += 1
            elif has_name_id and (has_leave_keyword or has_date):
                matched_lines.append(line)
            elif has_leave_keyword and has_date and len(line) < 100:
                matched_lines.append(line)

        triggered = len(matched_lines) >= 2 or high_confidence_count >= 1

        meta = {
            "triggered": triggered,
            "total_lines": len(lines),
            "matched_count": len(matched_lines),
            "high_confidence_count": high_confidence_count,
        }
        return matched_lines, meta

    # ------------------------------------------------------------------
    # Stage 2: deterministic regex extraction
    # ------------------------------------------------------------------

    @staticmethod
    def regex_extract_leave_records(
        lines: List[str],
        email_dt: Optional[datetime],
    ) -> List[Dict[str, Any]]:
        """Deterministic regex extraction from candidate lines.

        High-recall extractor for patterns like:
        ``邝莲爱（42648073）申请离职，预计11月1日离职生效``
        """
        records: List[Dict[str, Any]] = []
        seen_ids: set = set()

        def _extract_date_from_line(line: str) -> Tuple[str, str]:
            for pattern, fmt in LEAVE_EXTRACT_DATE_PATTERNS:
                match = pattern.search(line)
                if match:
                    groups = match.groups()
                    if fmt == "full_cn":
                        year, month, day = int(groups[0]), int(groups[1]), int(groups[2])
                        date_text = f"{year}年{month}月{day}日"
                    elif fmt == "iso":
                        year, month, day = int(groups[0]), int(groups[1]), int(groups[2])
                        date_text = match.group(0)
                    else:  # partial_cn
                        month, day = int(groups[0]), int(groups[1])
                        year = DateParser.infer_year_extended(email_dt, month)
                        date_text = f"{month}月{day}日"

                    try:
                        datetime(year, month, day)
                        iso_date = f"{year:04d}-{month:02d}-{day:02d}"
                        return date_text, iso_date
                    except ValueError:
                        pass
            return "", ""

        for line in lines:
            matches = NAME_ID_CAPTURE_RE.findall(line)
            if not matches:
                continue

            date_text, iso_date = _extract_date_from_line(line)

            for name, emp_id in matches:
                if emp_id in seen_ids:
                    continue
                seen_ids.add(emp_id)

                records.append({
                    "name": name,
                    "employee_id": emp_id,
                    "leave_date_text": date_text,
                    "leave_date": iso_date,
                    "intent": "remove",
                    "note": "",
                    "_source": "regex_extraction",
                })
        return records

    # ------------------------------------------------------------------
    # Stage 3: LLM-based extraction + validation
    # ------------------------------------------------------------------

    def llm_extract(
        self,
        matched_lines: List[str],
        filename: str,
    ) -> Tuple[List[Dict], List[str]]:
        """Call LLM to extract leave records.

        Returns ``(llm_records, warnings)``.
        """
        llm_records: List[Dict] = []
        warnings: List[str] = []
        prompt_tpl = self._prompts.get("EMAIL_LEAVE_LINES_TO_JSON_PROMPT", "")
        if not prompt_tpl.strip():
            return llm_records, warnings

        snippet = "\n".join(matched_lines[:50])
        prompt = prompt_tpl + "\n\nLEAVE_LINES:\n" + snippet
        try:
            logger.info("Leave lines extraction: LLM start (%s)", filename)
            if hasattr(self._llm, "chat_json_once"):
                raw = self._llm.chat_json_once(
                    prompt, system=None, temperature=0, timeout=30.0,
                    step="email_leave_lines_to_json",
                )
            else:
                raw = self._llm.chat_json(
                    prompt, system=None, temperature=0,
                    step="email_leave_lines_to_json",
                )
            logger.info("Leave lines extraction: LLM done (%s)", filename)

            if isinstance(raw, dict):
                llm_data = raw.get("data", [])
                if isinstance(llm_data, list):
                    llm_records = [r for r in llm_data if isinstance(r, dict)]
        except Exception as e:
            warnings.append(
                f"leave_lines_extracted: llm_exception: {type(e).__name__}: {str(e)[:200]}"
            )
        return llm_records, warnings

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    @staticmethod
    def validate_llm_records(
        llm_records: List[Dict],
        regex_records: List[Dict],
    ) -> List[Dict[str, Any]]:
        """Keep only LLM records that pass basic sanity checks.

        Falls back to *regex_records* if nothing from LLM validates.
        """
        def _is_valid(rec: Dict) -> bool:
            name = str(rec.get("name", "")).strip()
            emp_id = str(rec.get("employee_id", "")).strip()
            if not CHINESE_NAME_RE.match(name):
                return False
            if emp_id and not DIGIT_ID_RE.match(emp_id):
                return False
            leave_date = str(rec.get("leave_date", "")).strip()
            if leave_date:
                if not ISO_DATE_STRICT_RE.match(leave_date):
                    return False
                try:
                    parts = leave_date.split("-")
                    datetime(int(parts[0]), int(parts[1]), int(parts[2]))
                except (ValueError, IndexError):
                    return False
            return True

        valid = [r for r in llm_records if _is_valid(r)]
        if not valid and regex_records:
            return regex_records
        return valid

    # ------------------------------------------------------------------
    # Merge regex + LLM
    # ------------------------------------------------------------------

    @staticmethod
    def merge_records(
        regex_records: List[Dict],
        llm_records: List[Dict],
    ) -> List[Dict[str, Any]]:
        """Merge regex and LLM records (LLM preferred, dedup by employee_id)."""
        merged: List[Dict[str, Any]] = []
        seen_ids: set = set()

        for rec in llm_records:
            emp_id = str(rec.get("employee_id", "")).strip()
            if emp_id:
                seen_ids.add(emp_id)
            merged.append(rec)

        for rec in regex_records:
            emp_id = str(rec.get("employee_id", "")).strip()
            if emp_id and emp_id not in seen_ids:
                merged.append(rec)
                seen_ids.add(emp_id)

        return merged

    # ------------------------------------------------------------------
    # Date normalisation for final records
    # ------------------------------------------------------------------

    @staticmethod
    def normalize_leave_dates(
        extracted: dict,
        email_dt: Optional[datetime],
        warnings: List[str],
    ) -> None:
        """Normalise *leave_date_text* → *leave_date* (YYYY-MM-DD) in-place."""
        records = extracted.get("data", [])
        for rec in records:
            if not isinstance(rec, dict):
                continue

            leave_date_text = rec.get("leave_date_text", "").strip()
            if not leave_date_text:
                rec["leave_date"] = ""
                continue

            full_dt = DateParser.parse_any_date(leave_date_text)
            if full_dt:
                rec["leave_date"] = DateParser.to_iso_date(full_dt.year, full_dt.month, full_dt.day)
                continue

            partial = DateParser.parse_partial_cn_date(leave_date_text)
            if partial:
                month, day = partial
                if email_dt:
                    year = DateParser.infer_year_from_email_date(email_dt, month)
                else:
                    year = datetime.now().year
                    warnings.append(
                        f"leave_date: no email date for year inference, using current year {year} "
                        f"for '{leave_date_text}'"
                    )
                try:
                    datetime(year, month, day)
                    rec["leave_date"] = DateParser.to_iso_date(year, month, day)
                except ValueError as e:
                    rec["leave_date"] = ""
                    warnings.append(
                        f"leave_date: invalid date {year}-{month}-{day} from '{leave_date_text}': {e}"
                    )
                continue

            rec["leave_date"] = ""
            warnings.append(f"leave_date: failed to parse '{leave_date_text}'")

    # ------------------------------------------------------------------
    # Shape coercion for leave-line result
    # ------------------------------------------------------------------

    @staticmethod
    def coerce_leave_lines_shape(
        obj: Any,
        meta: dict,
        warnings: List[str],
    ) -> dict:
        """Ensure ``{"data": [...], "metadata": {...}, "warnings": [...]}``.

        Forces every record's *intent* to ``"remove"`` and filters fields.
        """
        base = ContentCleaner.coerce_body_extracted_shape(obj, meta, warnings)
        if "source" not in base.get("metadata", {}):
            base["metadata"]["source"] = "email_body_leave_lines"

        allowed_fields = {"name", "employee_id", "leave_date_text", "leave_date", "intent", "note"}
        normalized: List[dict] = []
        for item in base.get("data", []):
            if isinstance(item, dict):
                rec = {}
                for k in allowed_fields:
                    val = item.get(k, "")
                    rec[k] = str(val) if val is not None else ""
                rec["intent"] = "remove"
                normalized.append(rec)
        base["data"] = normalized
        return base

    # ------------------------------------------------------------------
    # Top-level convenience: run full detection pipeline
    # ------------------------------------------------------------------

    def run(
        self,
        body_text: str,
        email_dt: Optional[datetime],
        filename: str,
    ) -> Optional[dict]:
        """Execute the full detection pipeline.

        Returns the structured ``leave_lines_extracted`` dict, or *None*
        if the heuristic did not trigger.
        """
        matched_lines, detect_meta = self.detect_leave_lines(body_text)

        if not detect_meta.get("triggered"):
            logger.debug(
                "Leave lines detector not triggered: %s (matched=%d)",
                filename,
                detect_meta.get("matched_count", 0),
            )
            return None

        logger.info(
            "Leave lines detector triggered: %s (matched=%d, high_conf=%d)",
            filename,
            detect_meta.get("matched_count", 0),
            detect_meta.get("high_confidence_count", 0),
        )

        # Regex stage
        regex_records = self.regex_extract_leave_records(matched_lines, email_dt)
        logger.info("Regex extraction: %s (records=%d)", filename, len(regex_records))

        # LLM stage
        llm_records, leave_warnings = self.llm_extract(matched_lines, filename)

        # Validate + merge
        validated_llm = self.validate_llm_records(llm_records, regex_records)
        final_records = self.merge_records(regex_records, validated_llm)

        # Determine source
        extraction_source = "regex_only"
        if llm_records and validated_llm:
            extraction_source = "llm_validated"
        elif llm_records and not validated_llm:
            extraction_source = "regex_fallback_llm_invalid"
            leave_warnings.append("leave_lines: LLM records invalid, using regex fallback")
        elif not llm_records and regex_records:
            extraction_source = "regex_fallback_llm_empty"

        leave_meta = {
            **detect_meta,
            "matched_lines_count": len(matched_lines),
            "regex_records_count": len(regex_records),
            "llm_records_count": len(llm_records),
            "final_records_count": len(final_records),
            "extraction_source": extraction_source,
        }

        for rec in final_records:
            rec["intent"] = "remove"
            rec.pop("_source", None)

        result = {
            "data": final_records,
            "metadata": {**leave_meta, "source": "email_body_leave_lines"},
            "warnings": leave_warnings,
        }

        # Normalise dates
        date_warnings: List[str] = []
        self.normalize_leave_dates(result, email_dt, date_warnings)
        if date_warnings:
            existing = result.get("warnings", [])
            if not isinstance(existing, list):
                existing = []
            existing.extend(date_warnings)
            result["warnings"] = existing

        logger.info(
            "Leave lines extraction done: %s (records=%d, source=%s)",
            filename,
            len(result.get("data", [])),
            extraction_source,
        )
        return result

import re
from pathlib import Path

import pytest

from core.extractors.excel_extractor import ExcelExtractor
from core.llm import LLMClient


class MockLLMClient(LLMClient):
    def __init__(self):
        pass

    def chat_json_once(self, prompt, system=None, temperature=0, step=None, timeout=None):
        return {}

    def chat_json(self, prompt, system=None, temperature=0, step=None, filename=None, source_id=None, mode=None):
        return {}


def _normalize(text: str) -> str:
    raw = str(text).strip().lower()
    raw = raw.replace("\u3000", "")
    raw = re.sub(r"[\s\W_]+", "", raw)
    return raw


def _is_remove_sheet(sheet_name: str, semantic_key_by_header: dict) -> bool:
    if any(k in sheet_name for k in ("减员", "离职", "退工", "退保")):
        return True
    for value in (semantic_key_by_header or {}).values():
        if (value or "") in ("termination_date", "terminationdate", "leave_date", "leavedate", "end_date", "enddate"):
            return True
    return False


def test_termination_reason_mapping_for_remove_sheets():
    file_path = Path("/mnt/data/天草2025年1月参保情况1.13.xls")
    if not file_path.exists():
        pytest.skip("missing /mnt/data/天草2025年1月参保情况1.13.xls")
    llm = MockLLMClient()
    prompts = {"EXCEL_SCHEMA_INFER_PROMPT": "mock"}
    extractor = ExcelExtractor(llm, prompts)
    source_doc = extractor.safe_extract(str(file_path), extract_all_sheets=True)
    extracted = source_doc.extracted or {}
    records = extracted.get("data") or []
    per_sheet = (extracted.get("metadata") or {}).get("per_sheet") or {}

    sheet_meta = per_sheet.get("福临门减员")
    assert isinstance(sheet_meta, dict)
    mapping = sheet_meta.get("semantic_key_by_header") or {}
    assert any(
        ("原因" in k or "reason" in _normalize(k)) and v == "termination_reason"
        for k, v in mapping.items()
    )
    sheet_records = [r for r in records if isinstance(r, dict) and r.get("__sheet_name__") == "福临门减员"]
    assert any((r.get("termination_reason") or "").strip() for r in sheet_records)

    for sheet_name, meta in per_sheet.items():
        if not isinstance(meta, dict):
            continue
        mapping = meta.get("semantic_key_by_header") or {}
        if not _is_remove_sheet(sheet_name, mapping):
            continue
        has_reason_header = any(
            ("原因" in k or "reason" in _normalize(k)) and v == "termination_reason"
            for k, v in mapping.items()
        )
        if not has_reason_header:
            continue
        sheet_records = [r for r in records if isinstance(r, dict) and r.get("__sheet_name__") == sheet_name]
        if not sheet_records:
            continue
        assert any((r.get("termination_reason") or "").strip() for r in sheet_records)

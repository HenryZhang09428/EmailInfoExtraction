from pathlib import Path
from typing import Any, Dict, List

from core.extractors.excel_extractor import ExcelExtractor
from core.llm import LLMClient


class MockLLMClient(LLMClient):
    def __init__(self) -> None:
        pass

    def chat_json_once(self, prompt, system=None, temperature=0, step=None, timeout=None):
        return {}

    def chat_json(self, prompt, system=None, temperature=0, step=None, filename=None, source_id=None, mode=None):
        return {}


def _find_workbook() -> Path:
    root = Path(__file__).resolve().parents[1]
    cache_dir = root / ".cache" / "eml_parts"
    if not cache_dir.exists():
        raise FileNotFoundError(str(cache_dir))
    matches = list(cache_dir.rglob("天草2025年1月参保情况1.13.xls"))
    if not matches:
        raise FileNotFoundError("天草2025年1月参保情况1.13.xls")
    return matches[0]


def _extract_records(extracted: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = extracted.get("data")
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    return []


def main() -> None:
    workbook_path = _find_workbook()
    llm = MockLLMClient()
    prompts = {"EXCEL_SCHEMA_INFER_PROMPT": "mock"}
    extractor = ExcelExtractor(llm, prompts)
    result = extractor.safe_extract(str(workbook_path), extract_all_sheets=True)
    extracted = result.extracted or {}
    records = _extract_records(extracted)
    metadata = extracted.get("metadata") if isinstance(extracted, dict) else {}
    records_count_by_sheet = {}
    if isinstance(metadata, dict):
        records_count_by_sheet = metadata.get("records_count_by_sheet") or {}

    assert len(records) > 50
    assert records_count_by_sheet.get("宁军增员", 0) > 0
    assert all("__sheet_name__" in record for record in records)
    for record in records:
        for value in record.values():
            if "datetime.datetime(" in str(value):
                raise AssertionError("datetime.datetime repr detected")


if __name__ == "__main__":
    main()

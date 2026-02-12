import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List

sys.path.append(str(Path(__file__).resolve().parents[1]))

from core.extractors.excel_extractor import ExcelExtractor
from core.llm import LLMClient


class MockLLMClient(LLMClient):
    def __init__(self) -> None:
        pass

    def chat_json_once(self, prompt, system=None, temperature=0, step=None, timeout=None):
        return {}

    def chat_json(self, prompt, system=None, temperature=0, step=None, filename=None, source_id=None, mode=None):
        return {}


def _extract_records(extracted: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = extracted.get("data")
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    return []


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("workbook_path")
    args = parser.parse_args()
    workbook_path = Path(args.workbook_path).expanduser()
    if not workbook_path.exists():
        raise FileNotFoundError(str(workbook_path))

    llm = MockLLMClient()
    prompts = {"EXCEL_SCHEMA_INFER_PROMPT": "mock"}
    extractor = ExcelExtractor(llm, prompts)
    result = extractor.extract(str(workbook_path), extract_all_sheets=True)
    extracted = result.extracted or {}
    records = _extract_records(extracted)
    metadata = extracted.get("metadata") if isinstance(extracted, dict) else {}
    records_count_by_sheet = {}
    if isinstance(metadata, dict):
        records_count_by_sheet = metadata.get("records_count_by_sheet") or {}

    add_records = [
        r for r in records
        if "增员" in str(r.get("__sheet_name__", "") or "")
    ]
    remove_records = [
        r for r in records
        if any(k in str(r.get("__sheet_name__", "") or "") for k in ("减员", "离职"))
    ]

    start_date_samples = []
    for record in add_records:
        if "start_date" in record:
            start_date_samples.append(record["start_date"])
        if len(start_date_samples) >= 5:
            break

    remove_filtered = [
        r for r in records
        if "离职" in str(r.get("__sheet_name__", "") or "")
    ]

    print(f"Total records: {len(records)}")
    print(f"ADD records (sheet contains 增员): {len(add_records)}")
    print("start_date samples with types:")
    for sample in start_date_samples:
        print(f"  {sample!r} ({type(sample).__name__})")
    print(f"REMOVE candidates from 离职 sheets: {len(remove_filtered)}")
    print(f"REMOVE candidates from 减员/离职 sheets: {len(remove_records)}")
    print("records_count_by_sheet:")
    for sheet_name, count in records_count_by_sheet.items():
        print(f"  {sheet_name}: {count}")


if __name__ == "__main__":
    main()

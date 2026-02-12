from pathlib import Path

from core.extractors.excel_extractor import ExcelExtractor


class DummyLLM:
    def chat_json(self, prompt: str, system=None, temperature=None, step=None, **kwargs):
        return {}


def test_excel_multisheet_tiancao_regression():
    fixture_path = Path(__file__).resolve().parents[1] / "eval" / "fixtures" / "天草2025年1月参保情况1.13.xls"
    assert fixture_path.exists()

    extractor = ExcelExtractor(DummyLLM(), prompts={})
    doc = extractor.safe_extract(str(fixture_path), extract_all_sheets=True)
    extracted_json = doc.extracted

    assert isinstance(extracted_json, dict)
    metadata = extracted_json.get("metadata") or {}
    records = extracted_json.get("data") or []

    expected_counts = {
        "福临门增员": 5,
        "福临门减员": 6,
        "药房增员": 3,
        "药房减员": 7,
        "大兴安岭增员": 1,
        "大兴安岭减员": 0,
        "百益康增员": 24,
        "百益康减员": 4,
        "宁军增员": 1,
    }
    assert metadata.get("records_count_by_sheet") == expected_counts
    assert metadata.get("workbook_records_seen") == 51
    assert metadata.get("truncated_workbook_level") is False

    assert isinstance(records, list)
    target_record = None
    for record in records:
        if not isinstance(record, dict):
            continue
        if record.get("__sheet_name__") == "宁军增员" and record.get("id_number") == "445381198501184037":
            target_record = record
            break
    assert target_record is not None
    for key in [
        "name",
        "gender",
        "id_number",
        "start_date",
        "monthly_contribution",
        "household_registration",
        "pension_insurance_status",
    ]:
        assert target_record.get(key)

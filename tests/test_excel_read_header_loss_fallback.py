from pathlib import Path

import pandas as pd

from core.extractors.excel_extractor import ExcelExtractor


class DummyLLM:
    def chat_json(self, prompt: str, system=None, temperature=None, step=None, **kwargs):
        return {}


def test_excel_read_header_loss_fallback(monkeypatch):
    fixture_path = Path(__file__).resolve().parents[1] / "eval" / "fixtures" / "sample.xlsx"
    assert fixture_path.exists()

    def _fake_read_excel(*args, **kwargs):
        return pd.DataFrame(
            [
                ["", "", "", ""],
                ["员工工号", "姓名", "公司名称", "入职日期"],
                ["42700001", "张三", "某某公司", "2025-11-19"],
            ]
        )

    monkeypatch.setattr(pd, "read_excel", _fake_read_excel)

    extractor = ExcelExtractor(DummyLLM(), prompts={})
    doc = extractor.safe_extract(str(fixture_path))
    extracted_json = doc.extracted

    assert isinstance(extracted_json, dict)
    metadata = extracted_json.get("metadata") or {}
    records = extracted_json.get("data") or []

    assert metadata.get("header_row_idx") == 0
    assert isinstance(records, list) and len(records) > 0

    semantic_key_by_header = metadata.get("semantic_key_by_header") or {}
    hits = sum(
        1
        for value in semantic_key_by_header.values()
        if value in {"employee_id", "name", "company", "start_date"}
    )
    assert hits >= 2
    assert metadata.get("read_backend") == "openpyxl_values_full"

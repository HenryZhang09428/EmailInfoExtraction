import zipfile

from openpyxl import Workbook

from core.extractors.excel_extractor import ExcelExtractor


class DummyLLM:
    def chat_json(self, prompt: str, system=None, temperature=None, step=None, **kwargs):
        # Force fallback path for deterministic regression test.
        return {}


def _write_normal_xlsx(path: str) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "Sheet1"
    ws.append(["员工工号", "姓名", "入职日期", "公司名称", "岗位属性"])
    ws.append(["42720001", "张三", "2025-11-19", "某某有限公司", "二线"])
    ws.append(["42720002", "李四", "2025-11-20", "某某有限公司", "一线"])
    wb.save(path)


def _make_row_index_zero_xlsx(src_path: str, dst_path: str) -> None:
    with zipfile.ZipFile(src_path, "r") as zin, zipfile.ZipFile(dst_path, "w") as zout:
        for item in zin.infolist():
            data = zin.read(item.filename)
            if item.filename == "xl/worksheets/sheet1.xml":
                data = data.replace(b'<row r="1"', b'<row r="0"', 1)
            zout.writestr(item, data)


def test_excel_read_row_index_zero_fallback(tmp_path):
    normal_path = tmp_path / "normal.xlsx"
    broken_path = tmp_path / "row_index_zero.xlsx"

    _write_normal_xlsx(str(normal_path))
    _make_row_index_zero_xlsx(str(normal_path), str(broken_path))

    extractor = ExcelExtractor(DummyLLM(), prompts={})
    doc = extractor.safe_extract(str(broken_path))
    extracted_json = doc.extracted

    assert isinstance(extracted_json, dict)
    assert "metadata" in extracted_json
    assert "data" in extracted_json

    metadata = extracted_json["metadata"]
    records = extracted_json["data"]

    assert isinstance(records, list)
    assert len(records) >= 2

    assert metadata.get("read_backend") == "pandas_openpyxl_full"
    warnings = metadata.get("warnings") or extracted_json.get("warnings") or []
    assert (
        "xlsx_row_index_zero_detected" in warnings
        or "excel_read_fallback_full_openpyxl" in warnings
    )

    debug = metadata.get("header_selection_debug") or {}
    scanned = debug.get("scanned_rows") or []
    assert scanned and scanned[0].get("non_empty_ratio", 0.0) > 0

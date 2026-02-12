import re

from openpyxl import Workbook

from core.extractors.excel_extractor import ExcelExtractor


class DummyLLM:
    def chat_json(self, prompt: str, system=None, temperature=None, step=None, **kwargs):
        # Force fallback path for deterministic regression test.
        return {}


def _is_data_like_text(s: str) -> bool:
    """
    Detect whether a string looks like a data sample (id/date/money), not a header label.
    Used to guard against header_path accidentally including sample values.
    """
    if not isinstance(s, str):
        return False
    text = s.strip()
    if not text:
        return False

    # long digits (>=6)
    if re.search(r"\d{6,}", text):
        return True
    # date-like patterns (YYYY-MM-DD, YYYY/MM/DD, optional time)
    if re.search(r"\b\d{4}[-/]\d{1,2}[-/]\d{1,2}(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?\b", text):
        return True
    # money/amount-like patterns (e.g. 1108.00)
    if re.search(r"\b\d{1,6}\.\d{2}\b", text):
        return True

    return False


def _write_wide_sparse_header_xlsx(path: str) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "Sheet1"

    # 75 columns total; real header is sparse (only first 15 columns filled)
    headers_15 = [
        "员工工号",
        "姓名",
        "性别",
        "人事范围名称",
        "所属地区",
        "员工子组",
        "公司名称",
        "入职日期",
        "是否高峰期",
        "高峰期预计到期日期",
        "雇佣状态",
        "工作地",
        "所属组织单位",
        "职位名称",
        "岗位属性",
    ]
    header_row = headers_15 + [""] * (75 - len(headers_15))
    ws.append(header_row)

    # Row 2: dense data (intentionally very continuous non-empty, to reproduce mis-selection)
    dense_row = [""] * 75
    dense_row[0] = "42721486"          # long-ish numeric employee id
    dense_row[1] = "苏月群"            # name
    dense_row[2] = "女"
    dense_row[3] = "华东人事范围"
    dense_row[4] = "华东"
    dense_row[5] = "A1"
    dense_row[6] = "某某科技有限公司"
    dense_row[7] = "2025-11-19"        # start date
    dense_row[8] = "是"
    dense_row[9] = "2026-02-01"
    dense_row[10] = "在职"
    dense_row[11] = "上海"
    dense_row[12] = "华东区交付部"      # org unit -> company
    dense_row[13] = "项目经理"          # position
    dense_row[14] = "二线"              # position_attribute

    # Fill remaining columns with concrete text/amounts to maximize contiguous non-empty run.
    for i in range(15, 75):
        if i == 20:
            dense_row[i] = "1108.00"   # amount-like sample
        else:
            dense_row[i] = f"文本{i}"
    ws.append(dense_row)

    # Row 3: another data row
    row3 = [""] * 75
    row3[0] = "42721487"
    row3[1] = "张三"
    row3[2] = "男"
    row3[7] = "2025-11-20"
    row3[12] = "华南区交付部"
    row3[13] = "实施顾问"
    row3[14] = "一线"
    for i in range(15, 75):
        row3[i] = f"其他{i}"
    ws.append(row3)

    wb.save(path)


def test_excel_wide_sparse_header_should_not_pick_data_row_as_header(tmp_path):
    """
    Regression: wide tables with sparse/empty header cells can cause header_row_idx
    to be incorrectly selected as a dense data row.
    """
    xlsx_path = tmp_path / "wide_sparse_header.xlsx"
    _write_wide_sparse_header_xlsx(str(xlsx_path))

    extractor = ExcelExtractor(DummyLLM(), prompts={})
    doc = extractor.safe_extract(str(xlsx_path))
    extracted_json = doc.extracted

    assert isinstance(extracted_json, dict)
    assert "metadata" in extracted_json
    assert "data" in extracted_json

    metadata = extracted_json["metadata"]
    records = extracted_json["data"]

    # Expected correct behavior (currently buggy): header is row 0, data starts at row 1.
    assert metadata["header_row_idx"] == 0
    assert metadata["data_start_idx"] == 1

    semantic_key_by_header = metadata.get("semantic_key_by_header") or {}
    assert isinstance(semantic_key_by_header, dict)
    for header_path in semantic_key_by_header.keys():
        # header_path must not contain sample-like values such as:
        # 42721486 / 苏月群 / 1108.00 / 2025-11-19
        assert not _is_data_like_text(header_path)

    assert isinstance(records, list)
    assert len(records) > 0

    # Must contain at least some expected semantic keys (depending on LLM/fallback mapping).
    all_keys = set()
    for r in records:
        assert isinstance(r, dict)
        all_keys.update(r.keys())

    expected_some = {
        "employee_id",
        "name",
        "start_date",
        "date",
        "company",
        "contract_company",
        "position",
        "position_attribute",
    }
    assert all_keys.intersection(expected_some), f"missing expected keys, got: {sorted(all_keys)}"


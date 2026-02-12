import re

from openpyxl import Workbook

from core.extractors.excel_extractor import ExcelExtractor


class DummyLLM:
    def chat_json(self, prompt: str, system=None, temperature=None, step=None, **kwargs):
        # Force fallback path for deterministic regression test.
        return {}


def _write_minimal_employee_xlsx(path: str) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "Sheet1"

    headers = [
        "员工工号",
        "姓名",
        "性别",
        "入职日期",
        "所属组织单位",
        "职位名称",
        "岗位属性",
        "甲方公司(劳动合同)",
        "签订日期",
    ]
    ws.append(headers)

    # Row 2: typical data (should be the first data row)
    ws.append(
        [
            "427012345678901234",
            "张三",
            "男",
            "2025-09-18",
            "华东区交付部",
            "项目经理",
            "二线",
            "某某劳务公司",
            "2025-09-18",
        ]
    )

    # Row 3: another data row (keeps total_rows>=3 to reproduce the bug)
    ws.append(
        [
            "427012345678901235",
            "李四",
            "女",
            "2025-09-18",
            "华南区交付部",
            "实施顾问",
            "二线",
            "某某劳务公司",
            "2025-09-18",
        ]
    )

    wb.save(path)


def test_excel_header_row_and_semantic_keys_regression(tmp_path):
    """
    Regression: for Chinese header-only first row, the extractor must NOT treat the
    first data row as a second header row (data_start_idx must be 1, not 2).

    Also ensure header_path doesn't concatenate sample values (e.g. '岗位属性 / 二线'),
    and semantic keys do not get auto-deduped into position__* / company__*.
    """
    xlsx_path = tmp_path / "employees_min.xlsx"
    _write_minimal_employee_xlsx(str(xlsx_path))

    extractor = ExcelExtractor(DummyLLM(), prompts={})
    doc = extractor.safe_extract(str(xlsx_path))
    extracted_json = doc.extracted

    assert isinstance(extracted_json, dict)
    assert "metadata" in extracted_json
    assert "data" in extracted_json

    metadata = extracted_json["metadata"]
    records = extracted_json["data"]

    assert metadata["header_row_idx"] == 0
    assert metadata["data_start_idx"] == 1

    # Must not concatenate sample values into header_path keys.
    semantic_key_by_header = metadata.get("semantic_key_by_header") or {}
    assert isinstance(semantic_key_by_header, dict)

    # Specifically guard against the known bad pattern: '表头 / 样本值' where 样本值来自数据行。
    bad_sample_values = {"427012345678901234", "张三", "二线", "某某劳务公司"}
    for header_path in semantic_key_by_header.keys():
        assert isinstance(header_path, str)
        assert " / " not in header_path
        for sample in bad_sample_values:
            assert sample not in header_path

    # Records must include a semantic mapping for position and position_attribute (or synonym),
    # and for company and contract_company (or synonym). No position__* / company__* keys allowed.
    assert isinstance(records, list)
    assert len(records) == 2

    rec0 = records[0]
    assert isinstance(rec0, dict)

    assert "position" in rec0
    assert rec0["position"] == "项目经理"

    position_attribute_key = semantic_key_by_header.get("岗位属性", "")
    assert isinstance(position_attribute_key, str)
    assert position_attribute_key
    assert position_attribute_key != "position"
    assert not position_attribute_key.startswith("position__")
    assert position_attribute_key in rec0
    assert rec0[position_attribute_key] == "二线"

    assert semantic_key_by_header.get("所属组织单位") == "company"
    assert "company" in rec0
    assert rec0["company"] == "华东区交付部"

    contract_company_key = semantic_key_by_header.get("甲方公司(劳动合同)", "")
    assert isinstance(contract_company_key, str)
    assert contract_company_key
    assert contract_company_key != "company"
    assert not contract_company_key.startswith("company__")
    assert contract_company_key in rec0
    assert rec0[contract_company_key] == "某某劳务公司"

    for k in rec0.keys():
        assert not k.startswith("position__")
        assert not k.startswith("company__")
        assert not re.match(r"^position__\\d+$", k)
        assert not re.match(r"^company__\\d+$", k)


import re

from openpyxl import Workbook

from core.extractors.excel_extractor import ExcelExtractor


class DummyLLM:
    def chat_json(self, prompt: str, system=None, temperature=None, step=None, **kwargs):
        return {}


def _write_hr_contract_fixture(path: str) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "Sheet1"

    headers = [
        "员工工号",
        "工号",
        "姓名",
        "员工姓名",
        "入职日期",
        "公司名称",
        "所属组织单位",
        "职位名称",
        "岗位名称",
        "供应商",
        "雇佣状态",
        "员工子组",
        "岗位职能",
        "合同起始日期",
        "合同终止日期",
        "签订日期",
        "高峰期预计到期日期",
        "培训服务协议开始日期",
        "培训协议结束日期",
        "竞业禁止协议签订日期",
        "竞业禁止解除日期",
        "其他日期",
    ]
    ws.append(headers)

    ws.append([
        "10001",
        "10001-A",
        "张三",
        "张三(别名)",
        "2025-01-15",
        "某某科技有限公司",
        "华东区交付部",
        "项目经理",
        "项目经理",
        "平台供应商A",
        "在职",
        "A1",
        "交付",
        "2025-01-15",
        "2026-01-14",
        "2025-01-10",
        "2026-02-01",
        "2025-02-01",
        "2025-12-31",
        "2025-01-12",
        "2025-12-12",
        "2025-03-01",
    ])
    ws.append([
        "10002",
        "10002-B",
        "李四",
        "李四(别名)",
        "2025-02-20",
        "某某科技有限公司",
        "华南区交付部",
        "实施顾问",
        "实施顾问",
        "平台供应商B",
        "在职",
        "B2",
        "实施",
        "2025-02-20",
        "2026-02-19",
        "2025-02-10",
        "2026-03-01",
        "2025-03-01",
        "2026-01-01",
        "2025-02-12",
        "2026-02-12",
        "2025-04-01",
    ])

    wb.save(path)


def _count_non_empty_fields(record, required_keys):
    return sum(1 for key in required_keys if record.get(key))


def test_excel_semantic_key_sanity_hr_contract(tmp_path):
    xlsx_path = tmp_path / "hr_contract_fixture.xlsx"
    _write_hr_contract_fixture(str(xlsx_path))

    extractor = ExcelExtractor(DummyLLM(), prompts={})
    doc = extractor.safe_extract(str(xlsx_path))
    extracted_json = doc.extracted

    assert isinstance(extracted_json, dict)
    assert "data" in extracted_json
    records = extracted_json["data"]
    assert isinstance(records, list)
    assert records

    required = ["employee_id", "name", "start_date", "company", "position"]
    for record in records:
        assert _count_non_empty_fields(record, required) >= 4

    all_keys = set()
    for record in records:
        all_keys.update(record.keys())

    name_aliases = [k for k in all_keys if k.startswith("name__")]
    assert len(name_aliases) <= 1

    company_aliases = [k for k in all_keys if k.startswith("company__")]
    assert len(company_aliases) <= 1

    date_aliases = [k for k in all_keys if k.startswith("date__")]
    assert len(date_aliases) <= 3

    assert "vendor" in all_keys
    for record in records:
        assert record.get("vendor")
        employee_id = record.get("employee_id", "")
        vendor = record.get("vendor", "")
        assert employee_id != vendor
        assert re.fullmatch(r"\d+", str(employee_id))

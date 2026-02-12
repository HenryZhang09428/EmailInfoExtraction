from core.template.fill_planner import plan_fill
from core.template.schema import (
    TemplateSchema,
    SheetSchema,
    RegionSchema,
    TableInfo,
    TableHeader,
    Constraints,
)
from core.llm import LLMClient


class MockLLMClient(LLMClient):
    def __init__(self, mapping_response):
        self.mapping_response = mapping_response

    def chat_json(self, prompt, system=None, temperature=0, step=None, filename=None, source_id=None, mode=None):
        return self.mapping_response

    def chat_json_once(self, prompt, system=None, temperature=0, step=None, timeout=None):
        return self.mapping_response


def _build_template_schema(headers):
    table_headers = []
    for idx, h in enumerate(headers, start=1):
        table_headers.append(TableHeader(col_letter=chr(64 + idx), col_index=idx, header_path=h, sample_values=[]))
    table = TableInfo(range=f"A1:{chr(64 + len(headers))}10", header=table_headers, sample_rows=[])
    constraints = Constraints(has_formulas=False, formula_cells=[], validations=[], number_formats={})
    region = RegionSchema(region_id="region_1", layout_type="table", header_rows=[1], table=table, constraints=constraints)
    sheet = SheetSchema(sheet="Sheet1", regions=[region])
    return TemplateSchema(sheet_schemas=[sheet])


def _build_extracted(filename):
    records = [
        {"name": "A", "start_date": "2025-01-01", "__sheet_name__": "XX增员"},
        {"name": "B", "termination_date": "2025-02-01", "__sheet_name__": "XX减员"},
    ]
    return {
        "sources": [
            {
                "filename": filename,
                "source_type": "excel",
                "extracted": {"data": records},
            }
        ],
        "merged": {},
    }


def _extract_names(fill_plan):
    rows = []
    for rw in fill_plan.row_writes:
        rows.extend(rw.rows or [])
    return [row.get("name") for row in rows]


def test_remove_template_filters_add_records(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test")
    template_schema = _build_template_schema(["姓名", "离职日期"])
    mapping_response = {
        "column_mapping": {"姓名": "name", "离职日期": "termination_date"},
        "record_filter": {"field": "name", "values": ["A", "B"], "exclude": False},
    }
    llm = MockLLMClient(mapping_response)
    extracted = _build_extracted("减员来源.xlsx")
    fill_plan = plan_fill(template_schema, extracted, llm, "减员模板.xlsx", require_llm=False)
    names = _extract_names(fill_plan)
    assert names == ["B"]


def test_add_template_filters_remove_records(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test")
    template_schema = _build_template_schema(["姓名", "入职日期"])
    mapping_response = {
        "column_mapping": {"姓名": "name", "入职日期": "start_date"},
        "record_filter": {"field": "name", "values": ["A", "B"], "exclude": False},
    }
    llm = MockLLMClient(mapping_response)
    extracted = _build_extracted("增员来源.xlsx")
    fill_plan = plan_fill(template_schema, extracted, llm, "增员模板.xlsx", require_llm=False)
    names = _extract_names(fill_plan)
    assert names == ["A"]

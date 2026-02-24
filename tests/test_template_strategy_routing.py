from core.template.fill_planner import plan_fill
from core.template.schema import (
    Constraints,
    RegionSchema,
    SheetSchema,
    TableHeader,
    TableInfo,
    TemplateSchema,
)


class MockLLM:
    def __init__(self, mapping_response):
        self.mapping_response = mapping_response

    def chat_json_once(self, prompt, system=None, temperature=0, step=None, timeout=None):
        return self.mapping_response

    def chat_json(self, prompt, system=None, temperature=0, step=None):
        return self.mapping_response


def _build_social_security_template_schema() -> TemplateSchema:
    headers = [
        TableHeader(col_letter="A", col_index=1, header_path="参保人/姓名", sample_values=[]),
        TableHeader(col_letter="B", col_index=2, header_path="证件号码", sample_values=[]),
        TableHeader(col_letter="C", col_index=3, header_path="社保/申报类型", sample_values=[]),
        TableHeader(col_letter="D", col_index=4, header_path="社保/费用年月", sample_values=[]),
        TableHeader(col_letter="E", col_index=5, header_path="公积金/申报类型", sample_values=[]),
        TableHeader(col_letter="F", col_index=6, header_path="公积金/费用年月", sample_values=[]),
    ]
    table = TableInfo(range="A2:F20", header=headers, sample_rows=[])
    region = RegionSchema(
        region_id="region_1",
        layout_type="table",
        header_rows=[2],
        table=table,
        constraints=Constraints(has_formulas=False, formula_cells=[], validations=[], number_formats={}),
    )
    return TemplateSchema(sheet_schemas=[SheetSchema(sheet="Sheet1", regions=[region])])


def _extract_rows(fill_plan):
    rows = []
    for rw in fill_plan.row_writes:
        rows.extend(rw.rows or [])
    return rows


def test_strategy_key_routes_to_social_security_llm_mapping():
    schema = _build_social_security_template_schema()
    llm = MockLLM(
        {
            "target_field_to_source_key": {
                "name": "员工姓名",
                "id_number": "证件号码",
                "event_date": "离职日期",
            },
            "confidence": 0.92,
            "warnings": [],
        }
    )
    extracted = {
        "sources": [
            {
                "filename": "离职通知.xlsx",
                "source_type": "excel",
                "extracted": {
                    "data": [
                        {
                            "员工姓名": "张三",
                            "证件号码": "110101199001011234",
                            "离职日期": "2025-11-01",
                            "intent": "remove",
                            "__sheet_name__": "十一月减员",
                        }
                    ]
                },
            }
        ],
        "merged": {},
    }
    fill_plan = plan_fill(
        schema,
        extracted,
        llm,
        template_filename="减员模板.xlsx",
        planner_options={"template": {"strategy_key": "social_security_llm_mapping"}},
    )
    rows = _extract_rows(fill_plan)
    assert len(rows) == 1
    assert (fill_plan.debug or {}).get("strategy_key") == "social_security_llm_mapping"
    assert (fill_plan.debug or {}).get("strategy_routed") is True


def test_llm_invalid_mapping_falls_back_to_heuristics():
    schema = _build_social_security_template_schema()
    llm = MockLLM(
        {
            "target_field_to_source_key": {
                "name": "不存在字段",
                "id_number": "bad_key",
                "event_date": "离职日期",
            },
            "confidence": 0.2,
            "warnings": ["uncertain mapping"],
        }
    )
    extracted = {
        "sources": [
            {
                "filename": "离职通知.xlsx",
                "source_type": "excel",
                "extracted": {
                    "data": [
                        {
                            "name": "李四",
                            "employee_id": "42648073",
                            "离职日期": "2025-11-01",
                            "intent": "remove",
                            "__sheet_name__": "十一月减员",
                        }
                    ]
                },
            }
        ],
        "merged": {},
    }
    fill_plan = plan_fill(
        schema,
        extracted,
        llm,
        template_filename="减员模板.xlsx",
        planner_options={"template": {"strategy_key": "social_security_llm_mapping"}},
    )
    rows = _extract_rows(fill_plan)
    assert len(rows) == 1
    assert any("llm_mapping" in w for w in fill_plan.warnings)

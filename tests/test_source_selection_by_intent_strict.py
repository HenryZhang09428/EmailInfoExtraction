from core.template import fill_planner as fp
from core.template.schema import (
    TemplateSchema,
    SheetSchema,
    RegionSchema,
    TableInfo,
    TableHeader,
    Constraints,
)


class DummyLLM:
    def chat_json(self, prompt: str, system=None, temperature=None, step=None, **kwargs):
        return {}


def _build_template_schema_add():
    headers = ["姓名", "入职日期", "增员原因", "参保月份"]
    table_headers = []
    for idx, h in enumerate(headers, start=1):
        table_headers.append(TableHeader(col_letter=chr(64 + idx), col_index=idx, header_path=h, sample_values=[]))
    table = TableInfo(range="A1:D10", header=table_headers, sample_rows=[])
    constraints = Constraints(has_formulas=False, formula_cells=[], validations=[], number_formats={})
    region = RegionSchema(region_id="region_1", layout_type="table", header_rows=[1], table=table, constraints=constraints)
    sheet = SheetSchema(sheet="Sheet1", regions=[region])
    return TemplateSchema(sheet_schemas=[sheet])


def _build_extracted_json(include_add_source: bool = True):
    source_a_records = [
        {
            "employee_id": "10001",
            "name": "张三",
            "gender": "男",
            "company": "某某科技有限公司",
            "position": "工程师",
            "work_location": "上海",
        }
    ]
    source_a = {
        "filename": "社保在职人员花名册.xlsx",
        "source_type": "excel",
        "extracted": {
            "metadata": {
                "semantic_key_by_header": {
                    "姓名": "name",
                    "工号": "employee_id",
                    "公司名称": "company",
                    "工作地点": "work_location",
                }
            },
            "data": source_a_records,
        },
    }

    sources = [source_a]

    if include_add_source:
        source_b_records = [
            {
                "employee_id": "20001",
                "name": "李四",
                "start_date": "2025-09-18",
                "add_reason": "新增",
                "fee_month": "202509",
            }
        ]
        source_b = {
            "filename": "社保增员_入职.xlsx",
            "source_type": "excel",
            "extracted": {
                "metadata": {
                    "semantic_key_by_header": {
                        "姓名": "name",
                        "入职日期": "start_date",
                        "增员原因": "add_reason",
                        "参保月份": "fee_month",
                    }
                },
                "data": source_b_records,
            },
        }
        sources.append(source_b)

    return {"sources": sources, "merged": {}}


def test_source_selection_strict_intent_picks_add_source():
    template_schema = _build_template_schema_add()
    template_headers = fp._get_template_headers(template_schema)
    template_intent = fp._infer_template_intent_simple("模板.xlsx", template_headers)
    assert template_intent == "add"

    extracted_json = _build_extracted_json(include_add_source=True)
    warnings = []
    selected_sources, _ = fp._select_sources_for_template(
        extracted_json,
        template_schema,
        warnings,
        template_intent=template_intent,
        strict_intent=True,
        excel_only=True,
        max_sources=1,
    )
    selected_filenames = [s.get("filename") for s in selected_sources]
    assert selected_filenames == ["社保增员_入职.xlsx"]


def test_source_selection_strict_intent_no_match_returns_empty_plan():
    template_schema = _build_template_schema_add()
    extracted_json = _build_extracted_json(include_add_source=False)

    plan = fp.plan_fill(
        template_schema,
        extracted_json,
        DummyLLM(),
        template_filename="模板.xlsx",
        require_llm=False,
    )
    assert "no_source_matches_template_intent" in (plan.warnings or [])
    assert plan.llm_used is False
    assert isinstance(plan.debug, dict)
    assert plan.debug.get("template_intent") == "add"

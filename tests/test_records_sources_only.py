from core.template.fill_planner import plan_fill
from core.template.schema import TemplateSchema, SheetSchema, RegionSchema, TableInfo, TableHeader, Constraints


class DummyLLM:
    def chat_json(self, prompt: str, system=None, temperature=None, step=None):
        # Should not be called in this test (no API key -> fallback path).
        return {}


def _minimal_template_schema() -> TemplateSchema:
    constraints = Constraints(
        has_formulas=False,
        formula_cells=[],
        validations=[],
        number_formats={},
    )
    table = TableInfo(
        range="A1:A10",
        header=[
            TableHeader(col_letter="A", col_index=1, header_path="姓名", sample_values=["张三"]),
        ],
        sample_rows=[{"姓名": "张三"}],
    )
    region = RegionSchema(
        region_id="r1",
        layout_type="table",
        header_rows=[1],
        key_value_pairs=None,
        table=table,
        constraints=constraints,
    )
    sheet = SheetSchema(sheet="Sheet1", regions=[region])
    return TemplateSchema(sheet_schemas=[sheet])


def test_records_come_from_sources_not_merged():
    template_schema = _minimal_template_schema()

    extracted = {
        "sources": [
            {"filename": "source_A.xlsx", "source_type": "excel", "parent_source_id": None, "extracted": {"data": [{"姓名": "A"}]}},
            {"filename": "source_B.xlsx", "source_type": "excel", "parent_source_id": None, "extracted": {"data": [{"姓名": "B"}]}},
        ],
        # merged must not be used for records
        "merged": {"some_scalar": "x"},
    }

    fill_plan = plan_fill(template_schema, extracted, DummyLLM(), template_filename="template.xlsx")
    plan_dict = fill_plan.to_dict()
    debug = plan_dict.get("debug", {}) or {}

    assert debug.get("records_source_files") == ["source_A.xlsx", "source_B.xlsx"]
    assert debug.get("records_count_per_source") == {"source_A.xlsx": 1, "source_B.xlsx": 1}


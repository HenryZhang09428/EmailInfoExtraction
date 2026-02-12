import pytest

from core.template import fill_planner as fp
from core.template.schema import (
    TemplateSchema,
    SheetSchema,
    RegionSchema,
    TableInfo,
    TableHeader,
    Constraints,
)


def _build_template_schema(sample_fee: str = "202509") -> TemplateSchema:
    headers = [
        TableHeader(col_letter="A", col_index=1, header_path="参保人/姓名", sample_values=[]),
        TableHeader(col_letter="B", col_index=2, header_path="证件号码", sample_values=[]),
        TableHeader(col_letter="C", col_index=3, header_path="社保/申报类型", sample_values=[]),
        TableHeader(col_letter="D", col_index=4, header_path="费用年月", sample_values=[sample_fee]),
        TableHeader(col_letter="E", col_index=5, header_path="方案名称", sample_values=[]),
    ]

    table = TableInfo(
        range="A1:E10",
        header=headers,
        sample_rows=[
            {
                "参保人/姓名": "张三",
                "证件号码": "110101199001011234",
                "社保/申报类型": "增",
                "费用年月": sample_fee,
                "方案名称": "广州仕邦",
            }
        ],
    )

    constraints = Constraints(
        has_formulas=False,
        formula_cells=[],
        validations=[],
        number_formats={},
    )

    region = RegionSchema(
        region_id="region_1",
        layout_type="table",
        header_rows=[1],
        table=table,
        constraints=constraints,
    )

    sheet = SheetSchema(sheet="Sheet1", regions=[region])
    return TemplateSchema(sheet_schemas=[sheet])


def _build_extracted_json(records):
    return {
        "sources": [
            {
                "filename": "source.xlsx",
                "source_type": "excel",
                "extracted": {"data": records},
            }
        ],
        "merged": {"data": records},
    }


def test_derived_fields_month_from_date_and_fill_plan_mapping():
    template_schema = _build_template_schema(sample_fee="202509")
    records = [
        {"姓名": "梁蕴铧", "员工工号": "42700995", "入职日期": "2025-09-18 00:00:00"},
        {"姓名": "李四", "员工工号": "42700996", "入职日期": "2025-09-18"},
        {"姓名": "王五", "员工工号": "42700997", "入职日期": "2025/09/18"},
    ]

    derived_fields = [
        {
            "new_key": "__fee_month__",
            "op": "MONTH_FROM_DATE",
            "args": {
                "source_keys": ["入职日期", "签订日期", "生效日期", "变动日期"],
                "strategy": "first_non_empty",
                "output_format": "from_template_sample",
            },
        }
    ]

    column_mapping = {
        "参保人/姓名": "姓名",
        "证件号码": "员工工号",
        "费用年月": "__fee_month__",
    }

    derived_fields = fp._attach_template_headers_to_derived_fields(derived_fields, column_mapping)
    warnings = []
    fp._apply_derived_fields_to_records(
        template_schema, records, derived_fields, "add", warnings
    )

    assert all(rec.get("__fee_month__") == "202509" for rec in records)

    extracted_json = _build_extracted_json(records)
    fill_plan = fp._build_fill_plan_from_mapping(
        template_schema,
        extracted_json,
        column_mapping,
        [],
        {"社保/申报类型": "增", "方案名称": "广州仕邦"},
    )

    assert fill_plan is not None
    row_write = fill_plan["row_writes"][0]
    assert row_write["column_mapping"].get("__fee_month__") == "D"


def test_constant_values_force_fee_month_write_even_without_mapping():
    template_schema = _build_template_schema(sample_fee="2025-09")
    records = [
        {"姓名": "梁蕴铧", "员工工号": "42700995", "入职日期": "2025-09-18 00:00:00"}
    ]

    column_mapping = {
        "参保人/姓名": "姓名",
        "证件号码": "员工工号",
    }

    extracted_json = _build_extracted_json(records)
    fill_plan = fp._build_fill_plan_from_mapping(
        template_schema,
        extracted_json,
        column_mapping,
        [],
        {"社保/申报类型": "增", "方案名称": "广州仕邦", "费用年月": "2025-09"},
    )

    assert fill_plan is not None
    row_write = fill_plan["row_writes"][0]
    assert row_write["column_mapping"].get("__const__费用年月") == "D"
    assert row_write["rows"][0].get("__const__费用年月") == "2025-09"

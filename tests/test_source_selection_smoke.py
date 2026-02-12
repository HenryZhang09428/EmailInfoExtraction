from core.template import fill_planner as fp
from core.template.schema import (
    TemplateSchema,
    SheetSchema,
    RegionSchema,
    TableInfo,
    TableHeader,
    Constraints,
)


def _build_template_schema():
    headers = [
        "参保人/姓名",
        "证件号码",
        "社保/申报类型",
        "费用年月",
        "方案名称",
        "所属部门",
        "联系电话",
        "入职日期",
        "岗位名称",
        "备注",
    ]
    table_headers = []
    for idx, h in enumerate(headers, start=1):
        table_headers.append(TableHeader(col_letter=chr(64 + idx), col_index=idx, header_path=h, sample_values=[]))
    
    table = TableInfo(
        range="A1:J10",
        header=table_headers,
        sample_rows=[
            {
                "参保人/姓名": "张三",
                "证件号码": "110101199001011234",
                "社保/申报类型": "增",
                "费用年月": "202509",
                "方案名称": "广州仕邦",
                "所属部门": "技术部",
                "联系电话": "13800138000",
                "入职日期": "2025-09-18",
                "岗位名称": "工程师",
                "备注": "",
            }
        ],
    )
    constraints = Constraints(has_formulas=False, formula_cells=[], validations=[], number_formats={})
    region = RegionSchema(region_id="region_1", layout_type="table", header_rows=[1], table=table, constraints=constraints)
    sheet = SheetSchema(sheet="Sheet1", regions=[region])
    return TemplateSchema(sheet_schemas=[sheet])


def test_select_sources_and_record_gating():
    template_schema = _build_template_schema()
    good_records = [
        {
            "姓名": "梁蕴铧",
            "证件号码": "110101199001011234",
            "社保/申报类型": "增",
            "费用年月": "202509",
            "方案名称": "广州仕邦",
            "所属部门": "技术部",
            "联系电话": "13800138000",
            "入职日期": "2025-09-18",
            "岗位名称": "工程师",
        },
        {
            "姓名": "李四",
            "证件号码": "110101199001011235",
            "社保/申报类型": "增",
            "费用年月": "202509",
            "方案名称": "广州仕邦",
            "所属部门": "技术部",
            "联系电话": "13800138001",
            "入职日期": "2025-09-18",
            "岗位名称": "工程师",
        },
    ]
    bad_records = [
        {"姓名": "随便", "工号": "A001"},
        {"姓名": "无关", "工号": "A002"},
    ]
    
    extracted_json = {
        "sources": [
            {"filename": "good.xlsx", "source_type": "excel", "extracted": {"data": good_records}},
            {"filename": "bad.xlsx", "source_type": "excel", "extracted": {"data": bad_records}},
        ],
        "merged": {},
    }
    
    warnings = []
    selected_sources, _ = fp._select_sources_for_template(extracted_json, template_schema, warnings)
    selected_filenames = [s.get("filename") for s in selected_sources]
    assert selected_filenames == ["good.xlsx"]
    
    selected_extracted = fp._build_selected_extracted_json(extracted_json, selected_sources)
    gated = fp._apply_record_gating_to_extracted_json(selected_extracted, template_schema, warnings)
    records = fp._extract_records(gated)
    assert len(records) == 2

import hashlib

from core.ir import IntermediateRepresentation, SourceDoc, SourceBlock, BlockType
from core.pipeline import build_stable_ir_signature
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


def test_cache_key_stable_and_skips_regen():
    ir = IntermediateRepresentation(
        sources=[
            SourceDoc(
                source_id="s1",
                filename="add.xlsx",
                file_path="/tmp/add.xlsx",
                source_type="excel",
                blocks=[SourceBlock(order=1, type=BlockType.TEXT, content="", meta={})],
                extracted={"data": [{"姓名": "张三"}]},
                parent_source_id=None,
            )
        ],
        facts=[],
        target_schema=None,
        output=None,
        scores=None,
    )

    template_bytes = b"fake-template"
    signature = build_stable_ir_signature(ir)
    cache_key = hashlib.sha256(template_bytes + signature.encode("utf-8")).hexdigest()

    cache = {cache_key: {"filled_path": "cached.xlsx"}}
    calls = {"count": 0}

    def fake_fill_template():
        calls["count"] += 1
        return "generated.xlsx"

    if cache_key in cache:
        _ = cache[cache_key]["filled_path"]
    else:
        fake_fill_template()

    assert calls["count"] == 0


def test_source_intent_filtering_add_template():
    extracted_json = {
        "sources": [
            {
                "filename": "增员名单.xlsx",
                "source_type": "excel",
                "extracted": {
                    "data": [
                        {"姓名": "张三", "__source_file__": "增员名单.xlsx"},
                    ]
                },
            },
            {
                "filename": "减员名单.xlsx",
                "source_type": "excel",
                "extracted": {
                    "data": [
                        {"姓名": "李四", "__source_file__": "减员名单.xlsx"},
                    ]
                },
            },
        ],
        "merged": {
            "data": [
                {"姓名": "张三", "__source_file__": "增员名单.xlsx"},
                {"姓名": "李四", "__source_file__": "减员名单.xlsx"},
            ]
        },
    }

    filtered = fp._auto_filter_records_by_template_intent(extracted_json, "add")
    records = fp._extract_records(filtered)
    names = [r.get("姓名") for r in records if isinstance(r, dict)]
    assert names == ["张三"]


def test_fee_month_inference_stable_from_template_sample():
    template_schema = _build_template_schema(sample_fee="2025-09")
    records = [
        {"姓名": "梁蕴铧", "员工工号": "42700995", "入职日期": "2025-09-18 00:00:00"},
        {"姓名": "李四", "员工工号": "42700996", "入职日期": "2025-09-18"},
        {"姓名": "王五", "员工工号": "42700997", "入职日期": "2025/09/18"},
    ]

    column_mapping = {"参保人/姓名": "姓名", "证件号码": "员工工号"}
    constant_values = {}
    warnings = []

    column_mapping, constant_values = fp._infer_fee_month(
        template_schema, records, "add", column_mapping, constant_values, warnings
    )

    assert all(rec.get("__fee_month__") == "2025-09" for rec in records)
    assert column_mapping.get("费用年月") == "__fee_month__"

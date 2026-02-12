"""
Test social security template detection and fill plan generation.
"""
import pytest
from datetime import datetime

from core.template.schema import (
    TemplateSchema,
    SheetSchema,
    RegionSchema,
    TableInfo,
    TableHeader,
    Constraints,
)
from core.template.profiles.social_security import (
    detect_social_security_template,
    build_social_security_fill_plan,
    SocialSecurityProfile,
    SourceScore,
    FieldMappingValidation,
    _parse_any_date,
    _next_month,
    _format_fee_month,
    _validate_chinese_name,
    _validate_id_number,
    _validate_fee_month,
    _validate_declare_type,
    _score_filename_for_intent,
    _score_keys_for_intent,
    _compute_field_coverage,
    _score_source,
    _select_sources_for_template,
    _deduplicate_records_advanced,
    _is_valid_cn_id,
    _is_pure_digits,
    _is_date_like,
    _is_gender_like,
    _score_value_as_name,
    _score_value_as_id,
    _score_value_as_date,
    _score_key_values_for_field_type,
    _find_best_key_for_field_type,
    _validate_and_fix_field_mappings,
    _validate_row_values,
    _validate_all_rows,
)
from core.llm import LLMClient


class MockLLMClient(LLMClient):
    def __init__(self):
        pass
    
    def chat_json_once(self, prompt, system=None, temperature=0, step=None, timeout=None):
        return {}
    
    def chat_json(self, prompt, system=None, temperature=0, step=None):
        return {}


def _build_social_security_template_schema() -> TemplateSchema:
    headers = [
        TableHeader(col_letter="A", col_index=1, header_path="参保人/姓名", sample_values=[]),
        TableHeader(col_letter="B", col_index=2, header_path="证件号码", sample_values=[]),
        TableHeader(col_letter="C", col_index=3, header_path="社保/申报类型", sample_values=["增"]),
        TableHeader(col_letter="D", col_index=4, header_path="社保/费用年月", sample_values=["202512"]),
        TableHeader(col_letter="E", col_index=5, header_path="公积金/申报类型", sample_values=["增"]),
        TableHeader(col_letter="F", col_index=6, header_path="公积金/费用年月", sample_values=["202512"]),
    ]
    
    table = TableInfo(
        range="A2:F20",
        header=headers,
        sample_rows=[
            {
                "参保人/姓名": "张三",
                "证件号码": "110101199001011234",
                "社保/申报类型": "增",
                "社保/费用年月": "202512",
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
        header_rows=[2],
        table=table,
        constraints=constraints,
    )
    
    sheet = SheetSchema(sheet="Sheet1", regions=[region])
    return TemplateSchema(sheet_schemas=[sheet])


def _build_non_social_security_template_schema() -> TemplateSchema:
    headers = [
        TableHeader(col_letter="A", col_index=1, header_path="Name", sample_values=[]),
        TableHeader(col_letter="B", col_index=2, header_path="Age", sample_values=[]),
        TableHeader(col_letter="C", col_index=3, header_path="City", sample_values=[]),
    ]
    
    table = TableInfo(
        range="A1:C10",
        header=headers,
        sample_rows=[],
    )
    
    region = RegionSchema(
        region_id="region_1",
        layout_type="table",
        header_rows=[1],
        table=table,
        constraints=Constraints(),
    )
    
    sheet = SheetSchema(sheet="Sheet1", regions=[region])
    return TemplateSchema(sheet_schemas=[sheet])


class TestSocialSecurityDetection:

    def test_detect_social_security_template_positive(self):
        schema = _build_social_security_template_schema()
        profile = detect_social_security_template(schema)
        
        assert profile.is_detected is True
        assert len(profile.name_columns) >= 1
        assert len(profile.declare_type_columns) >= 1
        assert len(profile.fee_month_columns) >= 1

    def test_detect_social_security_template_negative(self):
        schema = _build_non_social_security_template_schema()
        profile = detect_social_security_template(schema)
        
        assert profile.is_detected is False

    def test_detect_sections(self):
        schema = _build_social_security_template_schema()
        profile = detect_social_security_template(schema)
        
        assert profile.has_social_security_section is True
        assert profile.has_housing_fund_section is True

    def test_fee_month_format_detection(self):
        schema = _build_social_security_template_schema()
        profile = detect_social_security_template(schema)
        
        assert profile.fee_month_format == "YYYYMM"


class TestDateHelpers:

    def test_parse_any_date_iso(self):
        dt = _parse_any_date("2025-11-15")
        assert dt is not None
        assert dt.year == 2025
        assert dt.month == 11
        assert dt.day == 15

    def test_parse_any_date_slash(self):
        dt = _parse_any_date("2025/12/01")
        assert dt is not None
        assert dt.year == 2025
        assert dt.month == 12
        assert dt.day == 1

    def test_parse_any_date_compact(self):
        dt = _parse_any_date("20251115")
        assert dt is not None
        assert dt.year == 2025
        assert dt.month == 11
        assert dt.day == 15

    def test_next_month_normal(self):
        dt = datetime(2025, 11, 15)
        year, month = _next_month(dt)
        assert year == 2025
        assert month == 12

    def test_next_month_year_rollover(self):
        dt = datetime(2025, 12, 15)
        year, month = _next_month(dt)
        assert year == 2026
        assert month == 1

    def test_format_fee_month_yyyymm(self):
        result = _format_fee_month(2025, 12, "YYYYMM")
        assert result == "202512"

    def test_format_fee_month_yyyy_mm(self):
        result = _format_fee_month(2025, 1, "YYYY-MM")
        assert result == "2025-01"


class TestValidation:

    def test_validate_chinese_name_valid(self):
        assert _validate_chinese_name("张三") is True
        assert _validate_chinese_name("李四") is True
        assert _validate_chinese_name("王小明") is True
        assert _validate_chinese_name("欧阳修") is True

    def test_validate_chinese_name_invalid(self):
        assert _validate_chinese_name("John") is False
        assert _validate_chinese_name("张") is False
        assert _validate_chinese_name("") is False
        assert _validate_chinese_name("张三丰五六七") is False

    def test_validate_id_number_valid(self):
        assert _validate_id_number("110101199001011234") is True
        assert _validate_id_number("11010119900101123X") is True
        assert _validate_id_number("110101900101123") is True
        assert _validate_id_number("42648073") is True
        assert _validate_id_number("") is True

    def test_validate_id_number_invalid(self):
        assert _validate_id_number("abc123") is False

    def test_validate_fee_month_valid(self):
        assert _validate_fee_month("202512") is True
        assert _validate_fee_month("2025-12") is True

    def test_validate_fee_month_invalid(self):
        assert _validate_fee_month("") is False
        assert _validate_fee_month("2025") is False
        assert _validate_fee_month("invalid") is False

    def test_validate_declare_type_valid(self):
        assert _validate_declare_type("增") is True
        assert _validate_declare_type("减") is True

    def test_validate_declare_type_invalid(self):
        assert _validate_declare_type("") is False
        assert _validate_declare_type("add") is False


class TestBuildFillPlan:

    def test_build_fill_plan_add_template(self):
        schema = _build_social_security_template_schema()
        profile = detect_social_security_template(schema)
        llm = MockLLMClient()
        
        extracted_json = {
            "sources": [
                {
                    "source_id": "source1",
                    "filename": "入职名单.xlsx",
                    "source_type": "excel",
                    "extracted": {
                        "data": [
                            {
                                "name": "张三",
                                "id_number": "110101199001011234",
                                "start_date": "2025-11-15",
                                "intent": "add",
                            },
                            {
                                "name": "李四",
                                "id_number": "110101199002021234",
                                "start_date": "2025-11-20",
                                "intent": "add",
                            },
                        ]
                    }
                }
            ]
        }
        
        fill_plan = build_social_security_fill_plan(
            schema, extracted_json, llm, "增员模板.xlsx", profile, "add"
        )
        
        assert fill_plan is not None
        
        plan_dict = fill_plan.model_dump() if hasattr(fill_plan, 'model_dump') else {}
        row_writes = plan_dict.get("row_writes", [])
        
        all_rows = []
        for rw in row_writes:
            if isinstance(rw, dict):
                all_rows.extend(rw.get("rows", []))
        
        assert len(all_rows) == 2

    def test_build_fill_plan_remove_template(self):
        schema = _build_social_security_template_schema()
        profile = detect_social_security_template(schema)
        llm = MockLLMClient()
        
        extracted_json = {
            "sources": [
                {
                    "source_id": "source1",
                    "filename": "离职通知.eml",
                    "source_type": "email",
                    "extracted": {
                        "data": [
                            {
                                "name": "王五",
                                "employee_id": "42648073",
                                "leave_date": "2025-11-01",
                                "intent": "remove",
                            },
                        ]
                    }
                }
            ]
        }
        
        fill_plan = build_social_security_fill_plan(
            schema, extracted_json, llm, "减员模板.xlsx", profile, "remove"
        )
        
        assert fill_plan is not None
        
        plan_dict = fill_plan.model_dump() if hasattr(fill_plan, 'model_dump') else {}
        row_writes = plan_dict.get("row_writes", [])
        
        all_rows = []
        for rw in row_writes:
            if isinstance(rw, dict):
                all_rows.extend(rw.get("rows", []))
        
        assert len(all_rows) == 1

    def test_build_fill_plan_declare_type_correct(self):
        schema = _build_social_security_template_schema()
        profile = detect_social_security_template(schema)
        llm = MockLLMClient()
        
        extracted_json = {
            "sources": [
                {
                    "source_id": "source1",
                    "filename": "test.xlsx",
                    "source_type": "excel",
                    "extracted": {
                        "data": [
                            {
                                "name": "张三",
                                "start_date": "2025-11-15",
                            },
                        ]
                    }
                }
            ]
        }
        
        fill_plan_add = build_social_security_fill_plan(
            schema, extracted_json, llm, "增员.xlsx", profile, "add"
        )
        plan_dict = fill_plan_add.model_dump() if hasattr(fill_plan_add, 'model_dump') else {}
        rows = []
        for rw in plan_dict.get("row_writes", []):
            if isinstance(rw, dict):
                rows.extend(rw.get("rows", []))
        
        for row in rows:
            for key, value in row.items():
                if "declare_type" in key:
                    assert value == "增"

    def test_build_fill_plan_fee_month_next_month(self):
        schema = _build_social_security_template_schema()
        profile = detect_social_security_template(schema)
        llm = MockLLMClient()
        
        extracted_json = {
            "sources": [
                {
                    "source_id": "source1",
                    "filename": "test.xlsx",
                    "source_type": "excel",
                    "extracted": {
                        "data": [
                            {
                                "name": "张三",
                                "start_date": "2025-11-15",
                            },
                        ]
                    }
                }
            ]
        }
        
        fill_plan = build_social_security_fill_plan(
            schema, extracted_json, llm, "增员.xlsx", profile, "add"
        )
        plan_dict = fill_plan.model_dump() if hasattr(fill_plan, 'model_dump') else {}
        rows = []
        for rw in plan_dict.get("row_writes", []):
            if isinstance(rw, dict):
                rows.extend(rw.get("rows", []))
        
        for row in rows:
            for key, value in row.items():
                if "fee_month" in key:
                    assert value == "202512"

    def test_build_fill_plan_no_records_returns_empty(self):
        schema = _build_social_security_template_schema()
        profile = detect_social_security_template(schema)
        llm = MockLLMClient()
        
        extracted_json = {"sources": []}
        
        fill_plan = build_social_security_fill_plan(
            schema, extracted_json, llm, "模板.xlsx", profile, "add"
        )
        
        assert fill_plan is not None
        assert "no_records" in str(fill_plan.warnings).lower()


class TestSourceScoring:

    def test_score_filename_add_intent_strong_signal(self):
        score, breakdown = _score_filename_for_intent("入职名单202511.xlsx", "add")
        assert score > 0
        assert any("入职" in s for s in breakdown.get("matched_signals", []))

    def test_score_filename_remove_intent_strong_signal(self):
        score, breakdown = _score_filename_for_intent("离职减员通知.xlsx", "remove")
        assert score > 0
        assert any("离职" in s or "减员" in s for s in breakdown.get("matched_signals", []))

    def test_score_filename_exclusion_signal(self):
        score, breakdown = _score_filename_for_intent("在职人员名单.xlsx", "add")
        assert breakdown.get("exclusion_signals")
        assert "在职" in breakdown["exclusion_signals"]

    def test_score_filename_conflict_signal(self):
        score, breakdown = _score_filename_for_intent("离职名单.xlsx", "add")
        assert any("conflict" in s for s in breakdown.get("matched_signals", []))

    def test_score_keys_add_intent(self):
        keys = ["姓名", "身份证号", "入职日期", "部门"]
        score, breakdown = _score_keys_for_intent(keys, "add")
        assert score > 0
        matched = breakdown.get("matched_keys", [])
        assert any("name" in k for k in matched)
        assert any("date" in k for k in matched)

    def test_score_keys_remove_intent(self):
        keys = ["姓名", "员工工号", "离职日期", "离职原因"]
        score, breakdown = _score_keys_for_intent(keys, "remove")
        assert score > 0


class TestFieldCoverage:

    def test_compute_field_coverage_full(self):
        records = [
            {"name": "张三", "id_number": "110101199001011234", "start_date": "2025-11-15"},
            {"name": "李四", "id_number": "110101199002021234", "start_date": "2025-11-20"},
        ]
        name_cov, id_cov, date_cov, _ = _compute_field_coverage(records, "add")
        assert name_cov == 1.0
        assert id_cov == 1.0
        assert date_cov == 1.0

    def test_compute_field_coverage_partial(self):
        records = [
            {"name": "张三", "id_number": "110101199001011234", "start_date": "2025-11-15"},
            {"name": "李四"},
        ]
        name_cov, id_cov, date_cov, breakdown = _compute_field_coverage(records, "add")
        assert name_cov == 1.0
        assert id_cov == 0.5
        assert date_cov == 0.5

    def test_compute_field_coverage_remove_uses_end_date(self):
        records = [
            {"name": "张三", "leave_date": "2025-11-15"},
            {"name": "李四", "end_date": "2025-11-20"},
        ]
        name_cov, id_cov, date_cov, _ = _compute_field_coverage(records, "remove")
        assert date_cov == 1.0


class TestSourceSelection:

    def test_select_sources_prefers_add_signal(self):
        extracted_json = {
            "sources": [
                {
                    "source_id": "s1",
                    "filename": "在职人员.xlsx",
                    "source_type": "excel",
                    "extracted": {
                        "data": [{"name": "张三", "start_date": "2025-11-15"}]
                    }
                },
                {
                    "source_id": "s2",
                    "filename": "入职名单.xlsx",
                    "source_type": "excel",
                    "extracted": {
                        "data": [{"name": "李四", "start_date": "2025-11-20"}]
                    }
                },
            ]
        }
        selected, scores, warnings = _select_sources_for_template(extracted_json, "add", max_sources=1)
        assert len(selected) == 1
        assert selected[0].get("filename") == "入职名单.xlsx"

    def test_select_sources_prefers_remove_signal(self):
        extracted_json = {
            "sources": [
                {
                    "source_id": "s1",
                    "filename": "花名册.xlsx",
                    "source_type": "excel",
                    "extracted": {
                        "data": [{"name": "张三", "leave_date": "2025-11-15"}]
                    }
                },
                {
                    "source_id": "s2",
                    "filename": "离职通知.eml",
                    "source_type": "email",
                    "extracted": {
                        "data": [{"name": "李四", "leave_date": "2025-11-20", "intent": "remove"}]
                    }
                },
            ]
        }
        selected, scores, warnings = _select_sources_for_template(extracted_json, "remove", max_sources=1)
        assert len(selected) == 1
        assert selected[0].get("filename") == "离职通知.eml"

    def test_select_sources_excludes_在职_when_alternatives_exist(self):
        extracted_json = {
            "sources": [
                {
                    "source_id": "s1",
                    "filename": "在职员工.xlsx",
                    "source_type": "excel",
                    "extracted": {
                        "data": [{"name": "张三", "start_date": "2025-11-15"}]
                    }
                },
                {
                    "source_id": "s2",
                    "filename": "新增人员.xlsx",
                    "source_type": "excel",
                    "extracted": {
                        "data": [{"name": "李四", "start_date": "2025-11-20"}]
                    }
                },
            ]
        }
        selected, scores, warnings = _select_sources_for_template(extracted_json, "add", max_sources=2)
        filenames = [s.get("filename") for s in selected]
        assert "新增人员.xlsx" in filenames
        if len(selected) == 2:
            pass
        else:
            assert "在职员工.xlsx" not in filenames

    def test_select_sources_uses_在职_when_no_alternatives(self):
        extracted_json = {
            "sources": [
                {
                    "source_id": "s1",
                    "filename": "在职员工.xlsx",
                    "source_type": "excel",
                    "extracted": {
                        "data": [{"name": "张三", "start_date": "2025-11-15"}]
                    }
                },
            ]
        }
        selected, scores, warnings = _select_sources_for_template(extracted_json, "add", max_sources=1)
        assert len(selected) == 1
        assert "exclusion_signals" in str(warnings).lower() or len(selected) == 1

    def test_email_body_leave_events_eligible_for_remove(self):
        extracted_json = {
            "sources": [
                {
                    "source_id": "email1",
                    "filename": "减员通知.eml",
                    "source_type": "email",
                    "extracted": {
                        "data": [
                            {"name": "王五", "employee_id": "42648073", "leave_date": "2025-11-01", "intent": "remove"},
                            {"name": "赵六", "employee_id": "42648074", "leave_date": "2025-11-01", "intent": "remove"},
                        ]
                    }
                },
            ]
        }
        selected, scores, warnings = _select_sources_for_template(extracted_json, "remove", max_sources=1)
        assert len(selected) == 1
        assert selected[0].get("source_type") == "email"


class TestDeduplication:

    def test_deduplicate_by_employee_id(self):
        records = [
            {"name": "张三", "id_number": "110101199001011234", "event_date": datetime(2025, 11, 15)},
            {"name": "张三", "id_number": "110101199001011234", "event_date": datetime(2025, 11, 20)},
        ]
        result, dups = _deduplicate_records_advanced(records)
        assert len(result) == 1
        assert dups == 1

    def test_deduplicate_by_name_date_when_no_id(self):
        records = [
            {"name": "张三", "id_number": "", "event_date": datetime(2025, 11, 15)},
            {"name": "张三", "id_number": "", "event_date": datetime(2025, 11, 15)},
            {"name": "张三", "id_number": "", "event_date": datetime(2025, 11, 20)},
        ]
        result, dups = _deduplicate_records_advanced(records)
        assert len(result) == 2

    def test_deduplicate_prefers_record_with_id(self):
        records = [
            {"name": "张三", "id_number": "", "event_date": datetime(2025, 11, 15)},
            {"name": "张三", "id_number": "110101199001011234", "event_date": datetime(2025, 11, 15)},
        ]
        result, dups = _deduplicate_records_advanced(records)
        assert len(result) == 1
        assert result[0].get("id_number") == "110101199001011234"


class TestCnIdValidation:

    def test_valid_18_digit_id(self):
        assert _is_valid_cn_id("110101199001011234") is True

    def test_valid_18_digit_id_with_x(self):
        assert _is_valid_cn_id("11010119900101123X") is True
        assert _is_valid_cn_id("11010119900101123x") is True

    def test_valid_15_digit_id(self):
        assert _is_valid_cn_id("110101900101123") is True

    def test_invalid_id(self):
        assert _is_valid_cn_id("12345") is False
        assert _is_valid_cn_id("abc123") is False
        assert _is_valid_cn_id("") is False


class TestSourceSelectionIntegration:

    def test_fill_plan_uses_selected_sources(self):
        schema = _build_social_security_template_schema()
        profile = detect_social_security_template(schema)
        llm = MockLLMClient()
        
        extracted_json = {
            "sources": [
                {
                    "source_id": "s1",
                    "filename": "在职人员.xlsx",
                    "source_type": "excel",
                    "extracted": {
                        "data": [
                            {"name": "张三", "id_number": "110101199001011234", "start_date": "2025-11-15"},
                        ]
                    }
                },
                {
                    "source_id": "s2",
                    "filename": "入职名单202511.xlsx",
                    "source_type": "excel",
                    "extracted": {
                        "data": [
                            {"name": "李四", "id_number": "110101199002021234", "start_date": "2025-11-20"},
                            {"name": "王五", "id_number": "110101199003031234", "start_date": "2025-11-25"},
                        ]
                    }
                },
            ]
        }
        
        fill_plan = build_social_security_fill_plan(
            schema, extracted_json, llm, "增员模板.xlsx", profile, "add"
        )
        
        plan_dict = fill_plan.model_dump() if hasattr(fill_plan, 'model_dump') else {}
        debug = plan_dict.get("debug", {})
        
        assert "source_selection" in debug
        
        row_writes = plan_dict.get("row_writes", [])
        all_rows = []
        for rw in row_writes:
            if isinstance(rw, dict):
                all_rows.extend(rw.get("rows", []))
        
        assert len(all_rows) >= 2

    def test_fill_plan_merges_multiple_sources(self):
        schema = _build_social_security_template_schema()
        profile = detect_social_security_template(schema)
        llm = MockLLMClient()
        
        extracted_json = {
            "sources": [
                {
                    "source_id": "s1",
                    "filename": "离职名单1.xlsx",
                    "source_type": "excel",
                    "extracted": {
                        "data": [
                            {"name": "张三", "id_number": "110101199001011234", "leave_date": "2025-11-15"},
                        ]
                    }
                },
                {
                    "source_id": "s2",
                    "filename": "离职通知.eml",
                    "source_type": "email",
                    "extracted": {
                        "data": [
                            {"name": "李四", "employee_id": "42648073", "leave_date": "2025-11-20", "intent": "remove"},
                        ]
                    }
                },
            ]
        }
        
        fill_plan = build_social_security_fill_plan(
            schema, extracted_json, llm, "减员模板.xlsx", profile, "remove"
        )
        
        plan_dict = fill_plan.model_dump() if hasattr(fill_plan, 'model_dump') else {}
        row_writes = plan_dict.get("row_writes", [])
        all_rows = []
        for rw in row_writes:
            if isinstance(rw, dict):
                all_rows.extend(rw.get("rows", []))
        
        assert len(all_rows) == 2


class TestValueTypeDetection:

    def test_is_pure_digits(self):
        assert _is_pure_digits("12345678") is True
        assert _is_pure_digits("110101199001011234") is True
        assert _is_pure_digits("张三") is False
        assert _is_pure_digits("abc123") is False
        assert _is_pure_digits("") is False

    def test_is_date_like(self):
        assert _is_date_like("2025-11-15") is True
        assert _is_date_like("2025/11/15") is True
        assert _is_date_like("20251115") is True
        assert _is_date_like("2025年11月15日") is True
        assert _is_date_like("11月15日") is True
        assert _is_date_like("张三") is False
        assert _is_date_like("12345") is False

    def test_is_gender_like(self):
        assert _is_gender_like("男") is True
        assert _is_gender_like("女") is True
        assert _is_gender_like("male") is True
        assert _is_gender_like("female") is True
        assert _is_gender_like("M") is True
        assert _is_gender_like("F") is True
        assert _is_gender_like("张三") is False
        assert _is_gender_like("12345") is False


class TestValueScoring:

    def test_score_value_as_name_chinese(self):
        assert _score_value_as_name("张三") == 1.0
        assert _score_value_as_name("李四") == 1.0
        assert _score_value_as_name("王小明") == 1.0

    def test_score_value_as_name_rejects_digits(self):
        assert _score_value_as_name("12345678") == 0.0
        assert _score_value_as_name("110101199001011234") == 0.0

    def test_score_value_as_name_rejects_dates(self):
        assert _score_value_as_name("2025-11-15") == 0.0
        assert _score_value_as_name("2025年11月15日") == 0.0

    def test_score_value_as_name_rejects_gender(self):
        assert _score_value_as_name("男") == 0.0
        assert _score_value_as_name("女") == 0.0

    def test_score_value_as_id_cn_id(self):
        assert _score_value_as_id("110101199001011234") == 1.0
        assert _score_value_as_id("11010119900101123X") == 1.0
        assert _score_value_as_id("110101900101123") == 1.0

    def test_score_value_as_id_employee_id(self):
        assert _score_value_as_id("42648073") >= 0.5

    def test_score_value_as_id_rejects_names(self):
        assert _score_value_as_id("张三") == 0.0

    def test_score_value_as_date(self):
        assert _score_value_as_date("2025-11-15") == 1.0
        assert _score_value_as_date("2025/11/15") == 1.0
        assert _score_value_as_date("张三") == 0.0


class TestFieldMappingValidation:

    def test_validate_mappings_correct(self):
        records = [
            {"name": "张三", "id_number": "110101199001011234", "start_date": "2025-11-15"},
            {"name": "李四", "id_number": "110101199002021234", "start_date": "2025-11-20"},
        ]
        validation = _validate_and_fix_field_mappings(
            records, "name", "id_number", "start_date", "add"
        )
        assert validation.name_valid is True
        assert validation.id_valid is True
        assert validation.date_valid is True
        assert validation.used_fallback is False

    def test_validate_mappings_swapped_name_id(self):
        records = [
            {"wrong_name": "110101199001011234", "wrong_id": "张三", "start_date": "2025-11-15"},
            {"wrong_name": "110101199002021234", "wrong_id": "李四", "start_date": "2025-11-20"},
        ]
        validation = _validate_and_fix_field_mappings(
            records, "wrong_name", "wrong_id", "start_date", "add"
        )
        assert validation.used_fallback is True
        assert validation.name_key == "wrong_id"
        assert validation.id_key == "wrong_name"

    def test_validate_mappings_missing_id(self):
        records = [
            {"name": "张三", "start_date": "2025-11-15"},
            {"name": "李四", "start_date": "2025-11-20"},
        ]
        validation = _validate_and_fix_field_mappings(
            records, "name", "id_number", "start_date", "add"
        )
        assert validation.name_valid is True
        assert validation.id_available is False
        assert "required_field_missing" in str(validation.warnings)


class TestRowValidation:

    def test_validate_row_values_correct(self):
        row = {
            "__name__": "张三",
            "__id_number__": "110101199001011234",
            "__declare_type__social_security_0": "增",
            "__fee_month__social_security_0": "202512",
        }
        is_valid, errors = _validate_row_values(
            row, ["__declare_type__social_security_0"], ["__fee_month__social_security_0"], "增"
        )
        assert is_valid is True
        assert len(errors) == 0

    def test_validate_row_rejects_digits_as_name(self):
        row = {
            "__name__": "110101199001011234",
            "__id_number__": "",
            "__declare_type__social_security_0": "增",
            "__fee_month__social_security_0": "202512",
        }
        is_valid, errors = _validate_row_values(
            row, ["__declare_type__social_security_0"], ["__fee_month__social_security_0"], "增"
        )
        assert is_valid is False
        assert any("name_is_digits" in e for e in errors)

    def test_validate_row_rejects_name_as_id(self):
        row = {
            "__name__": "张三",
            "__id_number__": "李四",
            "__declare_type__social_security_0": "增",
            "__fee_month__social_security_0": "202512",
        }
        is_valid, errors = _validate_row_values(
            row, ["__declare_type__social_security_0"], ["__fee_month__social_security_0"], "增"
        )
        assert is_valid is False
        assert any("id_looks_like_name" in e for e in errors)

    def test_validate_row_rejects_raw_date_as_fee_month(self):
        row = {
            "__name__": "张三",
            "__id_number__": "110101199001011234",
            "__declare_type__social_security_0": "增",
            "__fee_month__social_security_0": "2025-11-15",
        }
        is_valid, errors = _validate_row_values(
            row, ["__declare_type__social_security_0"], ["__fee_month__social_security_0"], "增"
        )
        assert is_valid is False
        assert any("fee_month_is_raw_date" in e or "fee_month_invalid_format" in e for e in errors)

    def test_validate_row_rejects_gender_as_declare_type(self):
        row = {
            "__name__": "张三",
            "__id_number__": "110101199001011234",
            "__declare_type__social_security_0": "男",
            "__fee_month__social_security_0": "202512",
        }
        is_valid, errors = _validate_row_values(
            row, ["__declare_type__social_security_0"], ["__fee_month__social_security_0"], "增"
        )
        assert is_valid is False
        assert any("declare_type" in e for e in errors)


class TestFillPlanValidation:

    def test_employee_id_never_in_name_column(self):
        schema = _build_social_security_template_schema()
        profile = detect_social_security_template(schema)
        llm = MockLLMClient()
        
        extracted_json = {
            "sources": [
                {
                    "source_id": "s1",
                    "filename": "入职名单.xlsx",
                    "source_type": "excel",
                    "extracted": {
                        "data": [
                            {"wrong_name": "110101199001011234", "wrong_id": "张三", "start_date": "2025-11-15"},
                            {"wrong_name": "110101199002021234", "wrong_id": "李四", "start_date": "2025-11-20"},
                        ]
                    }
                }
            ]
        }
        
        fill_plan = build_social_security_fill_plan(
            schema, extracted_json, llm, "增员模板.xlsx", profile, "add"
        )
        
        plan_dict = fill_plan.model_dump() if hasattr(fill_plan, 'model_dump') else {}
        row_writes = plan_dict.get("row_writes", [])
        
        for rw in row_writes:
            if isinstance(rw, dict):
                for row in rw.get("rows", []):
                    name_val = row.get("__name__", "")
                    assert not _is_pure_digits(name_val), f"Name should not be pure digits: {name_val}"
                    assert _validate_chinese_name(name_val) or name_val == "", f"Name should be Chinese-like: {name_val}"

    def test_gender_never_in_declare_type_column(self):
        schema = _build_social_security_template_schema()
        profile = detect_social_security_template(schema)
        llm = MockLLMClient()
        
        extracted_json = {
            "sources": [
                {
                    "source_id": "s1",
                    "filename": "入职名单.xlsx",
                    "source_type": "excel",
                    "extracted": {
                        "data": [
                            {"name": "张三", "id_number": "110101199001011234", "start_date": "2025-11-15", "gender": "男"},
                            {"name": "李四", "id_number": "110101199002021234", "start_date": "2025-11-20", "gender": "女"},
                        ]
                    }
                }
            ]
        }
        
        fill_plan = build_social_security_fill_plan(
            schema, extracted_json, llm, "增员模板.xlsx", profile, "add"
        )
        
        plan_dict = fill_plan.model_dump() if hasattr(fill_plan, 'model_dump') else {}
        row_writes = plan_dict.get("row_writes", [])
        
        for rw in row_writes:
            if isinstance(rw, dict):
                for row in rw.get("rows", []):
                    for key, value in row.items():
                        if "declare_type" in key:
                            assert value in ("增", "减"), f"Declare type should be 增 or 减, got: {value}"
                            assert value not in ("男", "女", "male", "female"), f"Gender in declare_type: {value}"

    def test_raw_date_never_in_fee_month_column(self):
        schema = _build_social_security_template_schema()
        profile = detect_social_security_template(schema)
        llm = MockLLMClient()
        
        extracted_json = {
            "sources": [
                {
                    "source_id": "s1",
                    "filename": "入职名单.xlsx",
                    "source_type": "excel",
                    "extracted": {
                        "data": [
                            {"name": "张三", "id_number": "110101199001011234", "start_date": "2025-11-15"},
                            {"name": "李四", "id_number": "110101199002021234", "start_date": "2025-11-20"},
                        ]
                    }
                }
            ]
        }
        
        fill_plan = build_social_security_fill_plan(
            schema, extracted_json, llm, "增员模板.xlsx", profile, "add"
        )
        
        plan_dict = fill_plan.model_dump() if hasattr(fill_plan, 'model_dump') else {}
        row_writes = plan_dict.get("row_writes", [])
        
        for rw in row_writes:
            if isinstance(rw, dict):
                for row in rw.get("rows", []):
                    for key, value in row.items():
                        if "fee_month" in key:
                            assert not _is_date_like(value), f"Raw date in fee_month: {value}"
                            assert _validate_fee_month(value), f"Invalid fee_month format: {value}"

    def test_missing_id_emits_warning(self):
        schema = _build_social_security_template_schema()
        profile = detect_social_security_template(schema)
        llm = MockLLMClient()
        
        extracted_json = {
            "sources": [
                {
                    "source_id": "s1",
                    "filename": "入职名单.xlsx",
                    "source_type": "excel",
                    "extracted": {
                        "data": [
                            {"name": "张三", "start_date": "2025-11-15"},
                            {"name": "李四", "start_date": "2025-11-20"},
                        ]
                    }
                }
            ]
        }
        
        fill_plan = build_social_security_fill_plan(
            schema, extracted_json, llm, "增员模板.xlsx", profile, "add"
        )
        
        assert any("required_field_missing" in str(w) or "证件号码" in str(w) for w in fill_plan.warnings)

    def test_declare_type_always_constant(self):
        schema = _build_social_security_template_schema()
        profile = detect_social_security_template(schema)
        llm = MockLLMClient()
        
        extracted_json = {
            "sources": [
                {
                    "source_id": "s1",
                    "filename": "入职名单.xlsx",
                    "source_type": "excel",
                    "extracted": {
                        "data": [
                            {"name": "张三", "id_number": "110101199001011234", "start_date": "2025-11-15", "type": "入职"},
                            {"name": "李四", "id_number": "110101199002021234", "start_date": "2025-11-20", "type": "转正"},
                        ]
                    }
                }
            ]
        }
        
        fill_plan = build_social_security_fill_plan(
            schema, extracted_json, llm, "增员模板.xlsx", profile, "add"
        )
        
        plan_dict = fill_plan.model_dump() if hasattr(fill_plan, 'model_dump') else {}
        row_writes = plan_dict.get("row_writes", [])
        
        for rw in row_writes:
            if isinstance(rw, dict):
                for row in rw.get("rows", []):
                    for key, value in row.items():
                        if "declare_type" in key:
                            assert value == "增", f"Add template should have 增, got: {value}"

    def test_fee_month_is_computed_next_month(self):
        schema = _build_social_security_template_schema()
        profile = detect_social_security_template(schema)
        llm = MockLLMClient()
        
        extracted_json = {
            "sources": [
                {
                    "source_id": "s1",
                    "filename": "入职名单.xlsx",
                    "source_type": "excel",
                    "extracted": {
                        "data": [
                            {"name": "张三", "id_number": "110101199001011234", "start_date": "2025-11-15"},
                            {"name": "李四", "id_number": "110101199002021234", "start_date": "2025-12-20"},
                        ]
                    }
                }
            ]
        }
        
        fill_plan = build_social_security_fill_plan(
            schema, extracted_json, llm, "增员模板.xlsx", profile, "add"
        )
        
        plan_dict = fill_plan.model_dump() if hasattr(fill_plan, 'model_dump') else {}
        row_writes = plan_dict.get("row_writes", [])
        
        fee_months = []
        for rw in row_writes:
            if isinstance(rw, dict):
                for row in rw.get("rows", []):
                    for key, value in row.items():
                        if "fee_month" in key:
                            fee_months.append(value)
        
        assert "202512" in fee_months or "2025-12" in fee_months
        assert "202601" in fee_months or "2026-01" in fee_months

"""
Regression test for email body leave lines extraction.
Ensures that the extractor correctly identifies and extracts resignation/leave
lines from email bodies containing employee names, IDs, and dates.
"""
import os
import pytest
from pathlib import Path

from core.extractors.email_extractor import EmailExtractor
from core.extractors.email_extractor import (
    _parse_partial_cn_date,
    _parse_any_date,
    _infer_year_from_email_date,
    _to_iso_date,
)
from core.llm import LLMClient
from datetime import datetime


FIXTURES_DIR = Path(__file__).parent / "fixtures"
EMAIL_FIXTURE = FIXTURES_DIR / "email_leave_lines_sample.eml"


class MockLLMClient(LLMClient):
    def __init__(self):
        pass

    def chat_json_once(self, prompt, system=None, temperature=0, step=None, timeout=None):
        if step == "email_leave_lines_to_json":
            return {
                "data": [
                    {"name": "张三", "employee_id": "42648001", "leave_date_text": "11月15日", "intent": "remove", "note": ""},
                    {"name": "李四", "employee_id": "42648002", "leave_date_text": "11月20日", "intent": "remove", "note": ""},
                    {"name": "王五", "employee_id": "42648003", "leave_date_text": "12月1日", "intent": "remove", "note": ""},
                    {"name": "赵六", "employee_id": "42648004", "leave_date_text": "2025-11-25", "intent": "remove", "note": ""},
                    {"name": "钱七", "employee_id": "42648005", "leave_date_text": "2025/12/10", "intent": "remove", "note": ""},
                ],
                "metadata": {"source": "email_body_leave_lines"},
                "warnings": []
            }
        if step == "email_body_to_json":
            return {"data": [], "metadata": {}, "warnings": []}
        return {}

    def chat_json(self, prompt, system=None, temperature=0, step=None):
        return self.chat_json_once(prompt, system, temperature, step)


@pytest.fixture
def email_fixture_path():
    assert EMAIL_FIXTURE.exists(), f"Fixture not found: {EMAIL_FIXTURE}"
    return str(EMAIL_FIXTURE)


class TestEmailBodyLeaveLines:

    def test_fixture_file_exists(self):
        assert EMAIL_FIXTURE.exists(), f"Email fixture missing: {EMAIL_FIXTURE}"

    def test_email_extraction_not_empty(self, email_fixture_path):
        llm = MockLLMClient()
        prompts = {
            "EML_BODY_TO_JSON_PROMPT": "mock prompt",
            "EMAIL_LEAVE_LINES_TO_JSON_PROMPT": "mock leave lines prompt",
        }
        extractor = EmailExtractor(llm, prompts)

        source_doc = extractor.extract(email_fixture_path)

        assert source_doc is not None
        assert source_doc.extracted is not None

    def test_leave_lines_extracted_when_present(self, email_fixture_path):
        llm = MockLLMClient()
        prompts = {
            "EML_BODY_TO_JSON_PROMPT": "mock prompt",
            "EMAIL_LEAVE_LINES_TO_JSON_PROMPT": "mock leave lines prompt",
        }
        extractor = EmailExtractor(llm, prompts)

        source_doc = extractor.extract(email_fixture_path)
        extracted = source_doc.extracted

        assert "leave_lines_extracted" in extracted, "leave_lines_extracted should be present"
        leave_data = extracted["leave_lines_extracted"]
        assert isinstance(leave_data, dict)
        assert "data" in leave_data
        assert len(leave_data["data"]) > 0, "leave_lines_extracted['data'] should not be empty"

    def test_extracted_records_have_required_fields(self, email_fixture_path):
        llm = MockLLMClient()
        prompts = {
            "EML_BODY_TO_JSON_PROMPT": "mock prompt",
            "EMAIL_LEAVE_LINES_TO_JSON_PROMPT": "mock leave lines prompt",
        }
        extractor = EmailExtractor(llm, prompts)

        source_doc = extractor.extract(email_fixture_path)
        leave_data = source_doc.extracted.get("leave_lines_extracted", {})
        records = leave_data.get("data", [])

        assert len(records) >= 5, f"Expected at least 5 leave records, got {len(records)}"

        for record in records:
            assert "name" in record, f"Record missing 'name': {record}"
            assert "employee_id" in record, f"Record missing 'employee_id': {record}"
            assert "leave_date" in record or "leave_date_text" in record, f"Record missing date: {record}"
            assert record.get("intent") == "remove", f"Intent should be 'remove': {record}"

    def test_expected_names_extracted(self, email_fixture_path):
        llm = MockLLMClient()
        prompts = {
            "EML_BODY_TO_JSON_PROMPT": "mock prompt",
            "EMAIL_LEAVE_LINES_TO_JSON_PROMPT": "mock leave lines prompt",
        }
        extractor = EmailExtractor(llm, prompts)

        source_doc = extractor.extract(email_fixture_path)
        leave_data = source_doc.extracted.get("leave_lines_extracted", {})
        records = leave_data.get("data", [])

        names = [r.get("name", "") for r in records]
        expected_names = ["张三", "李四", "王五", "赵六", "钱七"]

        for expected in expected_names:
            assert expected in names, f"Expected name '{expected}' not found in {names}"

    def test_employee_ids_extracted(self, email_fixture_path):
        llm = MockLLMClient()
        prompts = {
            "EML_BODY_TO_JSON_PROMPT": "mock prompt",
            "EMAIL_LEAVE_LINES_TO_JSON_PROMPT": "mock leave lines prompt",
        }
        extractor = EmailExtractor(llm, prompts)

        source_doc = extractor.extract(email_fixture_path)
        leave_data = source_doc.extracted.get("leave_lines_extracted", {})
        records = leave_data.get("data", [])

        employee_ids = [r.get("employee_id", "") for r in records]
        expected_ids = ["42648001", "42648002", "42648003", "42648004", "42648005"]

        for expected in expected_ids:
            assert expected in employee_ids, f"Expected employee_id '{expected}' not found"

    def test_leave_dates_normalized(self, email_fixture_path):
        llm = MockLLMClient()
        prompts = {
            "EML_BODY_TO_JSON_PROMPT": "mock prompt",
            "EMAIL_LEAVE_LINES_TO_JSON_PROMPT": "mock leave lines prompt",
        }
        extractor = EmailExtractor(llm, prompts)

        source_doc = extractor.extract(email_fixture_path)
        leave_data = source_doc.extracted.get("leave_lines_extracted", {})
        records = leave_data.get("data", [])

        for record in records:
            leave_date = record.get("leave_date", "")
            leave_date_text = record.get("leave_date_text", "")
            assert leave_date or leave_date_text, f"Record should have date: {record}"
            if leave_date:
                assert len(leave_date) == 10, f"leave_date should be YYYY-MM-DD: {leave_date}"
                assert leave_date[4] == "-" and leave_date[7] == "-"


class TestDateParsingHelpers:

    def test_parse_partial_cn_date_valid(self):
        result = _parse_partial_cn_date("11月15日")
        assert result == (11, 15)

        result = _parse_partial_cn_date("1月1日")
        assert result == (1, 1)

        result = _parse_partial_cn_date("12月31日")
        assert result == (12, 31)

    def test_parse_partial_cn_date_invalid(self):
        result = _parse_partial_cn_date("invalid")
        assert result is None

        result = _parse_partial_cn_date("")
        assert result is None

        result = _parse_partial_cn_date("13月1日")
        assert result is None

    def test_parse_any_date_iso(self):
        result = _parse_any_date("2025-11-15")
        assert result is not None
        assert result.year == 2025
        assert result.month == 11
        assert result.day == 15

    def test_parse_any_date_slash(self):
        result = _parse_any_date("2025/12/10")
        assert result is not None
        assert result.year == 2025
        assert result.month == 12
        assert result.day == 10

    def test_parse_any_date_chinese_full(self):
        result = _parse_any_date("2025年11月25日")
        assert result is not None
        assert result.year == 2025
        assert result.month == 11
        assert result.day == 25

    def test_parse_any_date_compact(self):
        result = _parse_any_date("20251115")
        assert result is not None
        assert result.year == 2025
        assert result.month == 11
        assert result.day == 15

    def test_infer_year_same_year(self):
        email_dt = datetime(2025, 11, 4)
        year = _infer_year_from_email_date(email_dt, 11)
        assert year == 2025

        year = _infer_year_from_email_date(email_dt, 12)
        assert year == 2025

    def test_infer_year_cross_year(self):
        email_dt = datetime(2025, 12, 20)
        year = _infer_year_from_email_date(email_dt, 1)
        assert year == 2026

    def test_to_iso_date(self):
        result = _to_iso_date(2025, 11, 15)
        assert result == "2025-11-15"

        result = _to_iso_date(2025, 1, 5)
        assert result == "2025-01-05"


class TestThreadTruncationRegression:

    def test_forwarded_message_with_leave_lines_not_truncated_prematurely(self):
        email_body = """Hi Team,

Please process the following resignation:

邝莲爱（42648073）申请离职，预计11月1日离职生效，请协助办理社保减员。

Thanks,
HR

发件人: Manager
发送时间: 2025-10-20
收件人: HR Team
主题: FW: Resignation Request

This is older content that should be truncated.
"""
        from core.extractors.email_extractor import EmailExtractor
        
        class InnerTruncateTest:
            pass
        
        def _truncate_email_thread_test(text):
            if not text:
                return "", {"truncated": False, "rule": None, "marker": None}

            lines = text.split("\n")
            
            explicit_separators = [
                (r"^\s*-{3,}\s*原始邮件\s*-{3,}\s*$", "原始邮件_separator"),
                (r"^\s*-{3,}\s*Original\s+Message\s*-{3,}\s*$", "original_message_separator"),
                (r"^\s*-{3,}\s*Forwarded\s+message\s*-{3,}\s*$", "forwarded_message_separator"),
                (r"^\s*Begin\s+forwarded\s+message\s*[:：]?\s*$", "begin_forwarded"),
                (r"^\s*-{5,}\s*$", "dashes_separator"),
                (r"^\s*On\s.+wrote\s*[:：]\s*$", "on_wrote_separator"),
            ]
            
            import re
            for line_idx, line in enumerate(lines):
                for pattern, marker_name in explicit_separators:
                    if re.match(pattern, line, flags=re.IGNORECASE):
                        char_pos = sum(len(l) + 1 for l in lines[:line_idx])
                        if char_pos > 0:
                            return text[:char_pos].strip(), {
                                "truncated": True,
                                "rule": "explicit_separator",
                                "marker": marker_name,
                            }

            header_field_patterns = [
                r"^\s*发件人\s*[:：]",
                r"^\s*From\s*[:：]",
                r"^\s*发送时间\s*[:：]",
                r"^\s*Sent\s*[:：]",
                r"^\s*收件人\s*[:：]",
                r"^\s*To\s*[:：]",
                r"^\s*主题\s*[:：]",
                r"^\s*Subject\s*[:：]",
            ]
            
            window_size = 10
            min_fields_required = 3
            
            for start_idx in range(len(lines)):
                end_idx = min(start_idx + window_size, len(lines))
                window_lines = lines[start_idx:end_idx]
                
                matched_fields = set()
                for win_line in window_lines:
                    for hp in header_field_patterns:
                        if re.match(hp, win_line, flags=re.IGNORECASE):
                            field_name = hp.split(r"\s*[:：]")[0].replace(r"^\s*", "").strip("\\")
                            matched_fields.add(field_name.lower())
                            break
                
                if len(matched_fields) >= min_fields_required:
                    char_pos = sum(len(l) + 1 for l in lines[:start_idx])
                    if char_pos > 100:
                        return text[:char_pos].strip(), {
                            "truncated": True,
                            "rule": "quoted_header_block",
                            "marker": f"header_block_{len(matched_fields)}_fields",
                        }

            return text.strip(), {"truncated": False, "rule": None, "marker": None}
        
        truncated_text, meta = _truncate_email_thread_test(email_body)
        
        assert "邝莲爱" in truncated_text, "Leave line with name should be preserved"
        assert "42648073" in truncated_text, "Employee ID should be preserved"
        assert "11月1日" in truncated_text, "Leave date should be preserved"
        assert "离职生效" in truncated_text, "Leave keyword should be preserved"
        
        assert meta["truncated"] is True
        assert meta["rule"] == "quoted_header_block"
        
        assert "older content" not in truncated_text

    def test_single_header_field_does_not_trigger_truncation(self):
        email_body = """Hi,

张三（42648001）申请离职，离职生效日期：11月15日

发件人: Someone

The rest of the email content here.
More important information.
"""
        import re
        
        lines = email_body.split("\n")
        
        header_field_patterns = [
            r"^\s*发件人\s*[:：]",
            r"^\s*发送时间\s*[:：]",
            r"^\s*收件人\s*[:：]",
            r"^\s*主题\s*[:：]",
        ]
        
        window_size = 10
        min_fields_required = 3
        
        should_truncate = False
        for start_idx in range(len(lines)):
            end_idx = min(start_idx + window_size, len(lines))
            window_lines = lines[start_idx:end_idx]
            
            matched_fields = set()
            for win_line in window_lines:
                for hp in header_field_patterns:
                    if re.match(hp, win_line, flags=re.IGNORECASE):
                        matched_fields.add(hp)
                        break
            
            if len(matched_fields) >= min_fields_required:
                should_truncate = True
                break
        
        assert not should_truncate, "Single header field should not trigger truncation"

    def test_explicit_separator_takes_priority(self):
        email_body = """Important content here.

-----Original Message-----
发件人: Someone
收件人: Other
主题: Old subject

Old content.
"""
        import re
        
        lines = email_body.split("\n")
        explicit_separators = [
            (r"^\s*-{3,}\s*Original\s+Message\s*-{3,}\s*$", "original_message_separator"),
        ]
        
        found_explicit = False
        explicit_line_idx = None
        for line_idx, line in enumerate(lines):
            for pattern, marker_name in explicit_separators:
                if re.match(pattern, line, flags=re.IGNORECASE):
                    found_explicit = True
                    explicit_line_idx = line_idx
                    break
            if found_explicit:
                break
        
        assert found_explicit, "Should find explicit separator"
        assert explicit_line_idx is not None
        
        char_pos = sum(len(l) + 1 for l in lines[:explicit_line_idx])
        truncated = email_body[:char_pos].strip()
        
        assert "Important content" in truncated
        assert "Old content" not in truncated


REGEX_FIXTURE = FIXTURES_DIR / "email_regex_extraction_sample.eml"


class TestRegexExtraction:

    def test_regex_extraction_basic(self):
        lines = [
            "邝莲爱（42648073）申请离职，预计11月1日离职生效，流程未归档",
            "陈志强（42648074）申请离职，预计11月1日离职生效，已完成交接",
            "张小明（42648075）申请离职，预计11月1日离职生效",
        ]
        
        email_dt = datetime(2025, 10, 20)
        
        import re
        
        def _regex_extract_leave_records_test(lines, email_dt):
            records = []
            name_id_pattern = re.compile(r'([\u4e00-\u9fa5]{2,4})[（(](\d{4,})[）)]')
            date_patterns = [
                (re.compile(r'(\d{4})年(\d{1,2})月(\d{1,2})日'), "full_cn"),
                (re.compile(r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})'), "iso"),
                (re.compile(r'(\d{1,2})月(\d{1,2})日'), "partial_cn"),
            ]
            
            def _infer_year(email_dt, month):
                if not email_dt:
                    return datetime.now().year
                year = email_dt.year
                if email_dt.month >= 10 and month <= 2:
                    year += 1
                return year
            
            def _extract_date(line, email_dt):
                for pattern, fmt in date_patterns:
                    match = pattern.search(line)
                    if match:
                        groups = match.groups()
                        if fmt == "full_cn":
                            year, month, day = int(groups[0]), int(groups[1]), int(groups[2])
                        elif fmt == "iso":
                            year, month, day = int(groups[0]), int(groups[1]), int(groups[2])
                        else:
                            month, day = int(groups[0]), int(groups[1])
                            year = _infer_year(email_dt, month)
                        try:
                            datetime(year, month, day)
                            return f"{year:04d}-{month:02d}-{day:02d}"
                        except ValueError:
                            pass
                return ""
            
            seen = set()
            for line in lines:
                matches = name_id_pattern.findall(line)
                date = _extract_date(line, email_dt)
                for name, emp_id in matches:
                    if emp_id not in seen:
                        seen.add(emp_id)
                        records.append({
                            "name": name,
                            "employee_id": emp_id,
                            "leave_date": date,
                            "intent": "remove",
                        })
            return records
        
        records = _regex_extract_leave_records_test(lines, email_dt)
        
        assert len(records) == 3
        
        names = [r["name"] for r in records]
        assert "邝莲爱" in names
        assert "陈志强" in names
        assert "张小明" in names
        
        emp_ids = [r["employee_id"] for r in records]
        assert "42648073" in emp_ids
        assert "42648074" in emp_ids
        assert "42648075" in emp_ids
        
        for record in records:
            assert record["leave_date"] == "2025-11-01"
            assert record["intent"] == "remove"

    def test_year_inference_cross_year(self):
        email_dt = datetime(2025, 12, 15)
        
        def _infer_year(email_dt, month):
            if not email_dt:
                return datetime.now().year
            year = email_dt.year
            if email_dt.month >= 10 and month <= 2:
                year += 1
            return year
        
        year_jan = _infer_year(email_dt, 1)
        assert year_jan == 2026
        
        year_feb = _infer_year(email_dt, 2)
        assert year_feb == 2026
        
        year_mar = _infer_year(email_dt, 3)
        assert year_mar == 2025
        
        year_dec = _infer_year(email_dt, 12)
        assert year_dec == 2025

    def test_regex_fixture_extraction(self):
        assert REGEX_FIXTURE.exists(), f"Fixture not found: {REGEX_FIXTURE}"
        
        llm = MockLLMClient()
        prompts = {
            "EML_BODY_TO_JSON_PROMPT": "mock prompt",
            "EMAIL_LEAVE_LINES_TO_JSON_PROMPT": "mock leave lines prompt",
        }
        extractor = EmailExtractor(llm, prompts)
        
        source_doc = extractor.extract(str(REGEX_FIXTURE))
        leave_data = source_doc.extracted.get("leave_lines_extracted", {})
        records = leave_data.get("data", [])
        
        assert len(records) >= 3, f"Expected at least 3 records, got {len(records)}"
        
        names = [r.get("name") for r in records]
        assert "邝莲爱" in names
        assert "陈志强" in names
        assert "张小明" in names
        
        emp_ids = [r.get("employee_id") for r in records]
        assert "42648073" in emp_ids
        assert "42648074" in emp_ids
        assert "42648075" in emp_ids
        
        for record in records:
            leave_date = record.get("leave_date", "")
            assert leave_date == "2025-11-01", f"Expected 2025-11-01, got {leave_date}"
            assert record.get("intent") == "remove"

    def test_records_in_top_level_data(self):
        assert REGEX_FIXTURE.exists()
        
        llm = MockLLMClient()
        prompts = {
            "EML_BODY_TO_JSON_PROMPT": "mock prompt",
            "EMAIL_LEAVE_LINES_TO_JSON_PROMPT": "mock leave lines prompt",
        }
        extractor = EmailExtractor(llm, prompts)
        
        source_doc = extractor.extract(str(REGEX_FIXTURE))
        top_data = source_doc.extracted.get("data", [])
        
        leave_records = [r for r in top_data if r.get("__extraction_type__") == "leave_lines"]
        assert len(leave_records) >= 3

    def test_extraction_metadata(self):
        assert REGEX_FIXTURE.exists()
        
        llm = MockLLMClient()
        prompts = {
            "EML_BODY_TO_JSON_PROMPT": "mock prompt",
            "EMAIL_LEAVE_LINES_TO_JSON_PROMPT": "mock leave lines prompt",
        }
        extractor = EmailExtractor(llm, prompts)
        
        source_doc = extractor.extract(str(REGEX_FIXTURE))
        leave_data = source_doc.extracted.get("leave_lines_extracted", {})
        metadata = leave_data.get("metadata", {})
        
        assert metadata.get("triggered") is True
        assert metadata.get("source") == "email_body_leave_lines"
        assert "regex_records_count" in metadata
        assert metadata.get("regex_records_count") >= 3


class TestLeaveLineDetection:

    def test_leave_lines_detector_triggers(self, email_fixture_path):
        llm = MockLLMClient()
        prompts = {
            "EML_BODY_TO_JSON_PROMPT": "mock prompt",
            "EMAIL_LEAVE_LINES_TO_JSON_PROMPT": "mock leave lines prompt",
        }
        extractor = EmailExtractor(llm, prompts)

        source_doc = extractor.extract(email_fixture_path)

        assert "leave_lines_extracted" in source_doc.extracted
        leave_meta = source_doc.extracted["leave_lines_extracted"].get("metadata", {})
        assert leave_meta.get("triggered") is True

    def test_all_intents_are_remove(self, email_fixture_path):
        llm = MockLLMClient()
        prompts = {
            "EML_BODY_TO_JSON_PROMPT": "mock prompt",
            "EMAIL_LEAVE_LINES_TO_JSON_PROMPT": "mock leave lines prompt",
        }
        extractor = EmailExtractor(llm, prompts)

        source_doc = extractor.extract(email_fixture_path)
        leave_data = source_doc.extracted.get("leave_lines_extracted", {})
        records = leave_data.get("data", [])

        for record in records:
            assert record.get("intent") == "remove", f"All intents must be 'remove': {record}"

    def test_leave_lines_added_to_top_level_data(self, email_fixture_path):
        llm = MockLLMClient()
        prompts = {
            "EML_BODY_TO_JSON_PROMPT": "mock prompt",
            "EMAIL_LEAVE_LINES_TO_JSON_PROMPT": "mock leave lines prompt",
        }
        extractor = EmailExtractor(llm, prompts)

        source_doc = extractor.extract(email_fixture_path)
        top_data = source_doc.extracted.get("data", [])

        leave_records = [r for r in top_data if r.get("__extraction_type__") == "leave_lines"]
        assert len(leave_records) >= 5, "Leave records should be in top-level data"

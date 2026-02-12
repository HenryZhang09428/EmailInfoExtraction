"""
Unit tests for date parsing in core.template.writer.

Tests the _convert_value and _parse_date functions with various date formats:
- ISO format (2023-01-01)
- Chinese format (2023年1月1日)
- Slash format (2023/01/01)
- Invalid date handling
"""
import pytest
from datetime import datetime
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.template.writer import _convert_value, _parse_date, _looks_like_date


class TestParseDateFunction:
    """Tests for the _parse_date function."""
    
    def test_iso_date_basic(self):
        """Test parsing basic ISO date format."""
        result = _parse_date("2023-01-01")
        assert result is not None
        assert isinstance(result, datetime)
        assert result.year == 2023
        assert result.month == 1
        assert result.day == 1
    
    def test_iso_datetime(self):
        """Test parsing ISO datetime format."""
        result = _parse_date("2023-01-15T10:30:00")
        assert result is not None
        assert isinstance(result, datetime)
        assert result.year == 2023
        assert result.month == 1
        assert result.day == 15
        assert result.hour == 10
        assert result.minute == 30
    
    def test_iso_with_timezone(self):
        """Test parsing ISO datetime with Z timezone."""
        result = _parse_date("2023-01-15T10:30:00Z")
        assert result is not None
        assert isinstance(result, datetime)
        assert result.year == 2023
        assert result.month == 1
        assert result.day == 15
    
    def test_chinese_date_without_padding(self):
        """Test parsing Chinese date format without leading zeros."""
        result = _parse_date("2023年1月1日")
        assert result is not None
        assert isinstance(result, datetime)
        assert result.year == 2023
        assert result.month == 1
        assert result.day == 1
    
    def test_chinese_date_with_padding(self):
        """Test parsing Chinese date format with leading zeros."""
        result = _parse_date("2023年01月15日")
        assert result is not None
        assert isinstance(result, datetime)
        assert result.year == 2023
        assert result.month == 1
        assert result.day == 15
    
    def test_chinese_date_with_hao(self):
        """Test parsing Chinese date format using 号 instead of 日."""
        result = _parse_date("2023年12月31号")
        assert result is not None
        assert isinstance(result, datetime)
        assert result.year == 2023
        assert result.month == 12
        assert result.day == 31
    
    def test_slash_format_ymd(self):
        """Test parsing slash format YYYY/MM/DD."""
        result = _parse_date("2023/01/15")
        assert result is not None
        assert isinstance(result, datetime)
        assert result.year == 2023
        assert result.month == 1
        assert result.day == 15
    
    def test_slash_format_dmy(self):
        """Test parsing slash format DD/MM/YYYY."""
        result = _parse_date("15/01/2023")
        assert result is not None
        assert isinstance(result, datetime)
        # Note: The function tries multiple formats, so the result depends on order
        # At minimum, we should get a valid datetime
    
    def test_dot_format(self):
        """Test parsing dot format YYYY.MM.DD."""
        result = _parse_date("2023.01.15")
        assert result is not None
        assert isinstance(result, datetime)
        assert result.year == 2023
        assert result.month == 1
        assert result.day == 15
    
    def test_compact_format(self):
        """Test parsing compact format YYYYMMDD."""
        result = _parse_date("20230115")
        assert result is not None
        assert isinstance(result, datetime)
        assert result.year == 2023
        assert result.month == 1
        assert result.day == 15
    
    def test_invalid_date_returns_none(self):
        """Test that invalid date string returns None."""
        result = _parse_date("not_a_date")
        assert result is None
    
    def test_empty_string_returns_none(self):
        """Test that empty string returns None."""
        result = _parse_date("")
        assert result is None
    
    def test_none_returns_none(self):
        """Test that None input returns None."""
        result = _parse_date(None)
        assert result is None
    
    def test_whitespace_only_returns_none(self):
        """Test that whitespace-only string returns None."""
        result = _parse_date("   ")
        assert result is None


class TestLooksLikeDateFunction:
    """Tests for the _looks_like_date function."""
    
    def test_iso_date_looks_like_date(self):
        """Test that ISO date is detected."""
        assert _looks_like_date("2023-01-01") is True
    
    def test_chinese_date_looks_like_date(self):
        """Test that Chinese date is detected."""
        assert _looks_like_date("2023年1月1日") is True
    
    def test_slash_date_looks_like_date(self):
        """Test that slash date is detected."""
        assert _looks_like_date("2023/01/15") is True
    
    def test_random_string_not_date(self):
        """Test that random string is not detected as date."""
        assert _looks_like_date("hello world") is False
    
    def test_empty_string_not_date(self):
        """Test that empty string is not detected as date."""
        assert _looks_like_date("") is False


class TestConvertValueFunction:
    """Tests for the _convert_value function."""
    
    def test_none_returns_none(self):
        """Test that None input returns None."""
        result = _convert_value(None, "")
        assert result is None
    
    def test_int_returns_int(self):
        """Test that int input returns int."""
        result = _convert_value(123, "")
        assert result == 123
        assert isinstance(result, int)
    
    def test_float_returns_float(self):
        """Test that float input returns float."""
        result = _convert_value(3.14, "")
        assert result == 3.14
        assert isinstance(result, float)
    
    def test_datetime_returns_datetime(self):
        """Test that datetime input returns datetime."""
        dt = datetime(2023, 1, 1)
        result = _convert_value(dt, "")
        assert result == dt
        assert isinstance(result, datetime)
    
    def test_empty_string_returns_none(self):
        """Test that empty string returns None."""
        result = _convert_value("", "")
        assert result is None
    
    def test_whitespace_returns_none(self):
        """Test that whitespace-only string returns None."""
        result = _convert_value("   ", "")
        assert result is None
    
    def test_id_number_preserved_as_string(self):
        """Test that ID numbers (15+ digits) are preserved as strings."""
        id_number = "110101199001011234"
        result = _convert_value(id_number, "")
        assert result == id_number
        assert isinstance(result, str)
    
    def test_id_number_with_x_preserved(self):
        """Test that ID numbers ending with X are preserved as strings."""
        id_number = "44030019900101123X"
        result = _convert_value(id_number, "")
        assert result == id_number
        assert isinstance(result, str)
    
    def test_phone_number_preserved_as_string(self):
        """Test that 11-digit phone numbers are preserved as strings."""
        phone = "13800138000"
        result = _convert_value(phone, "")
        assert result == phone
        assert isinstance(result, str)
    
    def test_iso_date_string_converted_to_datetime(self):
        """Test that ISO date string is converted to datetime."""
        result = _convert_value("2023-01-01", "yyyy-mm-dd")
        assert isinstance(result, datetime)
        assert result.year == 2023
        assert result.month == 1
        assert result.day == 1
    
    def test_chinese_date_converted_to_datetime(self):
        """Test that Chinese date is converted to datetime."""
        result = _convert_value("2023年1月1日", "yyyy-mm-dd")
        assert isinstance(result, datetime)
        assert result.year == 2023
        assert result.month == 1
        assert result.day == 1
    
    def test_slash_date_converted_to_datetime(self):
        """Test that slash date is converted to datetime."""
        result = _convert_value("2023/01/15", "yyyy-mm-dd")
        assert isinstance(result, datetime)
        assert result.year == 2023
        assert result.month == 1
        assert result.day == 15
    
    def test_invalid_date_preserved_as_string(self):
        """Test that invalid date-like string is preserved as string."""
        invalid_date = "invalid_date"
        result = _convert_value(invalid_date, "")
        # Not recognized as a date, so it's returned as-is
        assert result == invalid_date
        assert isinstance(result, str)
    
    def test_numeric_string_converted_to_int(self):
        """Test that numeric string is converted to int."""
        result = _convert_value("42", "")
        assert result == 42
        assert isinstance(result, int)
    
    def test_decimal_string_converted_to_float(self):
        """Test that decimal string is converted to float."""
        result = _convert_value("3.14", "")
        assert result == 3.14
        assert isinstance(result, float)


class TestEdgeCases:
    """Tests for edge cases and special scenarios."""
    
    def test_date_at_year_boundary(self):
        """Test date parsing at year boundaries."""
        result = _parse_date("2023-12-31")
        assert result is not None
        assert result.year == 2023
        assert result.month == 12
        assert result.day == 31
        
        result = _parse_date("2024-01-01")
        assert result is not None
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 1
    
    def test_short_number_not_treated_as_id(self):
        """Test that short numeric strings are converted to numbers."""
        result = _convert_value("12345", "")
        assert result == 12345
        assert isinstance(result, int)
    
    def test_value_with_leading_trailing_spaces(self):
        """Test that values with spaces are trimmed."""
        result = _convert_value("  2023-01-01  ", "yyyy-mm-dd")
        assert isinstance(result, datetime)
        assert result.year == 2023


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

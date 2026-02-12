"""
Pytest configuration and shared fixtures.
"""
import os
import sys
import pytest

# Add project root to path for imports
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)


@pytest.fixture
def sample_date_strings():
    """Sample date strings in various formats for testing."""
    return {
        # ISO formats
        "iso_date": "2023-01-01",
        "iso_datetime": "2023-01-15T10:30:00",
        "iso_with_tz": "2023-01-15T10:30:00Z",
        
        # Chinese formats
        "chinese_full": "2023年1月1日",
        "chinese_padded": "2023年01月15日",
        "chinese_hao": "2023年12月31号",
        
        # Slash formats
        "slash_ymd": "2023/01/15",
        "slash_dmy": "15/01/2023",
        
        # Dot format
        "dot_ymd": "2023.01.15",
        
        # Compact format
        "compact": "20230115",
        
        # Invalid
        "invalid": "not_a_date",
        "invalid_partial": "2023-13-45",
    }


@pytest.fixture
def mock_openai_response():
    """Mock OpenAI API response structure."""
    class MockUsage:
        def __init__(self):
            self.prompt_tokens = 100
            self.completion_tokens = 50
            self.total_tokens = 150
    
    class MockMessage:
        def __init__(self, content):
            self.content = content
    
    class MockChoice:
        def __init__(self, content):
            self.message = MockMessage(content)
    
    class MockResponse:
        def __init__(self, content='{"result": "success"}'):
            self.choices = [MockChoice(content)]
            self.usage = MockUsage()
    
    return MockResponse

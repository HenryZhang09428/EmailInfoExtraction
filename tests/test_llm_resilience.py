"""
Unit tests for LLM retry/resilience mechanism in core.llm.

Tests that the LLMClient's retry mechanism works as expected:
- Retries on transient failures
- Respects stop_after_attempt(3)
- Eventually raises after max retries
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import after path setup
from core.llm import LLMClient, reset_llm_client


class MockUsage:
    """Mock usage statistics from OpenAI response."""
    prompt_tokens = 100
    completion_tokens = 50
    total_tokens = 150


class MockMessage:
    """Mock message from OpenAI response."""
    def __init__(self, content='{"result": "success"}'):
        self.content = content


class MockChoice:
    """Mock choice from OpenAI response."""
    def __init__(self, content='{"result": "success"}'):
        self.message = MockMessage(content)


class MockResponse:
    """Mock OpenAI API response."""
    def __init__(self, content='{"result": "success"}'):
        self.choices = [MockChoice(content)]
        self.usage = MockUsage()


@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset the LLM client singleton before each test."""
    reset_llm_client()
    yield
    reset_llm_client()


@pytest.fixture
def mock_settings():
    """Mock settings for LLMClient initialization."""
    mock = Mock()
    mock.OPENAI_API_KEY = "test-api-key"
    mock.OPENAI_BASE_URL = "https://api.openai.com/v1"
    mock.OPENAI_MODEL = "gpt-4"
    mock.OPENAI_VISION_MODEL = "gpt-4-vision-preview"
    mock.TEMPERATURE = 0.7
    mock.REQUEST_TIMEOUT = 30.0
    return mock


class TestLLMClientRetryMechanism:
    """Tests for the LLMClient retry mechanism."""
    
    @patch('core.llm.get_settings')
    @patch('core.llm.OpenAI')
    def test_successful_request_no_retry(self, mock_openai_class, mock_get_settings, mock_settings):
        """Test that successful request doesn't trigger retries."""
        mock_get_settings.return_value = mock_settings
        
        # Create mock client with successful response
        mock_client = Mock()
        mock_client.chat.completions.create.return_value = MockResponse()
        mock_openai_class.return_value = mock_client
        
        # Create LLMClient and call chat_json
        llm = LLMClient()
        result = llm.chat_json("Test prompt")
        
        # Should only be called once (no retries needed)
        assert mock_client.chat.completions.create.call_count == 1
        assert result == {"result": "success"}
    
    @patch('core.llm.get_settings')
    @patch('core.llm.OpenAI')
    def test_retry_on_failure_then_success(self, mock_openai_class, mock_get_settings, mock_settings):
        """Test that failed requests are retried and eventually succeed."""
        mock_get_settings.return_value = mock_settings
        
        # Create mock client that fails twice then succeeds
        mock_client = Mock()
        mock_client.chat.completions.create.side_effect = [
            Exception("Temporary failure 1"),
            Exception("Temporary failure 2"),
            MockResponse()  # Third call succeeds
        ]
        mock_openai_class.return_value = mock_client
        
        # Create LLMClient and call chat_json
        llm = LLMClient()
        result = llm.chat_json("Test prompt")
        
        # Should be called 3 times (2 failures + 1 success)
        assert mock_client.chat.completions.create.call_count == 3
        assert result == {"result": "success"}
    
    @patch('core.llm.get_settings')
    @patch('core.llm.OpenAI')
    def test_max_retries_exceeded_raises_exception(self, mock_openai_class, mock_get_settings, mock_settings):
        """Test that exception is raised after max retries (3) are exhausted."""
        mock_get_settings.return_value = mock_settings
        
        # Create mock client that always fails
        mock_client = Mock()
        mock_client.chat.completions.create.side_effect = Exception("Persistent failure")
        mock_openai_class.return_value = mock_client
        
        # Create LLMClient
        llm = LLMClient()
        
        # Should raise exception after 3 attempts
        with pytest.raises(Exception) as exc_info:
            llm.chat_json("Test prompt")
        
        # Should be called exactly 3 times (max retries)
        assert mock_client.chat.completions.create.call_count == 3
        assert "Persistent failure" in str(exc_info.value)
    
    @patch('core.llm.get_settings')
    @patch('core.llm.OpenAI')
    def test_chat_text_retry_mechanism(self, mock_openai_class, mock_get_settings, mock_settings):
        """Test retry mechanism for chat_text method."""
        mock_get_settings.return_value = mock_settings
        
        # Create mock client that fails once then succeeds
        mock_client = Mock()
        mock_response = MockResponse()
        mock_response.choices[0].message.content = "Text response"
        
        mock_client.chat.completions.create.side_effect = [
            Exception("Temporary failure"),
            mock_response
        ]
        mock_openai_class.return_value = mock_client
        
        # Create LLMClient and call chat_text
        llm = LLMClient()
        result = llm.chat_text("Test prompt")
        
        # Should be called 2 times (1 failure + 1 success)
        assert mock_client.chat.completions.create.call_count == 2
        assert result == "Text response"
    
    @patch('core.llm.get_settings')
    @patch('core.llm.OpenAI')
    def test_chat_text_max_retries(self, mock_openai_class, mock_get_settings, mock_settings):
        """Test that chat_text raises after max retries."""
        mock_get_settings.return_value = mock_settings
        
        mock_client = Mock()
        mock_client.chat.completions.create.side_effect = Exception("Always fails")
        mock_openai_class.return_value = mock_client
        
        llm = LLMClient()
        
        with pytest.raises(Exception):
            llm.chat_text("Test prompt")
        
        assert mock_client.chat.completions.create.call_count == 3


class TestTokenUsageTracking:
    """Tests for token usage tracking functionality."""
    
    @patch('core.llm.get_settings')
    @patch('core.llm.OpenAI')
    def test_token_usage_updated_on_success(self, mock_openai_class, mock_get_settings, mock_settings):
        """Test that token usage is updated after successful request."""
        mock_get_settings.return_value = mock_settings
        
        mock_client = Mock()
        mock_client.chat.completions.create.return_value = MockResponse()
        mock_openai_class.return_value = mock_client
        
        llm = LLMClient()
        
        # Initial usage should be zero
        usage = llm.get_token_usage()
        assert usage["input_tokens"] == 0
        assert usage["output_tokens"] == 0
        assert usage["total_tokens"] == 0
        assert usage["requests_count"] == 0
        
        # Make a request
        llm.chat_json("Test prompt")
        
        # Usage should be updated
        usage = llm.get_token_usage()
        assert usage["input_tokens"] == 100
        assert usage["output_tokens"] == 50
        assert usage["total_tokens"] == 150
        assert usage["requests_count"] == 1
    
    @patch('core.llm.get_settings')
    @patch('core.llm.OpenAI')
    def test_token_usage_cumulative(self, mock_openai_class, mock_get_settings, mock_settings):
        """Test that token usage accumulates across multiple requests."""
        mock_get_settings.return_value = mock_settings
        
        mock_client = Mock()
        mock_client.chat.completions.create.return_value = MockResponse()
        mock_openai_class.return_value = mock_client
        
        llm = LLMClient()
        
        # Make multiple requests
        llm.chat_json("Prompt 1")
        llm.chat_json("Prompt 2")
        
        usage = llm.get_token_usage()
        assert usage["input_tokens"] == 200
        assert usage["output_tokens"] == 100
        assert usage["total_tokens"] == 300
        assert usage["requests_count"] == 2
    
    @patch('core.llm.get_settings')
    @patch('core.llm.OpenAI')
    def test_reset_token_usage(self, mock_openai_class, mock_get_settings, mock_settings):
        """Test that token usage can be reset."""
        mock_get_settings.return_value = mock_settings
        
        mock_client = Mock()
        mock_client.chat.completions.create.return_value = MockResponse()
        mock_openai_class.return_value = mock_client
        
        llm = LLMClient()
        llm.chat_json("Test prompt")
        
        # Verify usage was recorded
        assert llm.get_token_usage()["requests_count"] == 1
        
        # Reset and verify
        llm.reset_token_usage()
        usage = llm.get_token_usage()
        assert usage["input_tokens"] == 0
        assert usage["output_tokens"] == 0
        assert usage["total_tokens"] == 0
        assert usage["requests_count"] == 0


class TestJSONExtraction:
    """Tests for JSON extraction from LLM responses."""
    
    @patch('core.llm.get_settings')
    @patch('core.llm.OpenAI')
    def test_extract_json_from_markdown_block(self, mock_openai_class, mock_get_settings, mock_settings):
        """Test extracting JSON from markdown code block."""
        mock_get_settings.return_value = mock_settings
        
        json_in_markdown = '```json\n{"key": "value"}\n```'
        mock_response = MockResponse(json_in_markdown)
        
        mock_client = Mock()
        mock_client.chat.completions.create.return_value = mock_response
        mock_openai_class.return_value = mock_client
        
        llm = LLMClient()
        result = llm.chat_json("Test prompt")
        
        assert result == {"key": "value"}
    
    @patch('core.llm.get_settings')
    @patch('core.llm.OpenAI')
    def test_extract_json_array(self, mock_openai_class, mock_get_settings, mock_settings):
        """Test extracting JSON array."""
        mock_get_settings.return_value = mock_settings
        
        mock_response = MockResponse('[{"id": 1}, {"id": 2}]')
        
        mock_client = Mock()
        mock_client.chat.completions.create.return_value = mock_response
        mock_openai_class.return_value = mock_client
        
        llm = LLMClient()
        result = llm.chat_json("Test prompt")
        
        assert result == [{"id": 1}, {"id": 2}]
    
    @patch('core.llm.get_settings')
    @patch('core.llm.OpenAI')
    def test_extract_json_with_surrounding_text(self, mock_openai_class, mock_get_settings, mock_settings):
        """Test extracting JSON when surrounded by explanatory text."""
        mock_get_settings.return_value = mock_settings
        
        response_with_text = 'Here is the result:\n{"data": "test"}\nHope this helps!'
        mock_response = MockResponse(response_with_text)
        
        mock_client = Mock()
        mock_client.chat.completions.create.return_value = mock_response
        mock_openai_class.return_value = mock_client
        
        llm = LLMClient()
        result = llm.chat_json("Test prompt")
        
        assert result == {"data": "test"}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

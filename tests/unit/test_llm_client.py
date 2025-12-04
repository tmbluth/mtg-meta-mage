"""Unit tests for LLM client functionality"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from src.clients.llm_client import get_llm_client, LLMClient


class TestGetLLMClient:
    """Tests for get_llm_client function"""
    
    @patch('src.clients.llm_client.ChatOpenAI')
    @patch.dict('os.environ', {'OPENAI_API_KEY': 'test-key'})
    def test_openai_provider_explicit(self, mock_chat_openai):
        """Test explicit OpenAI provider"""
        mock_model = Mock()
        mock_chat_openai.return_value = mock_model
        
        result = get_llm_client('custom-model', model_provider='openai')
        
        mock_chat_openai.assert_called_once_with(
            model='custom-model',
            temperature=0.1,
            api_key='test-key'
        )
        assert isinstance(result, LLMClient)
    
    @patch.dict('os.environ', {}, clear=True)
    def test_openai_missing_api_key(self):
        """Test that missing OpenAI API key raises ValueError"""
        with pytest.raises(ValueError, match="OPENAI_API_KEY environment variable not set"):
            get_llm_client('gpt-4o-mini', model_provider='openai')
    
    @patch('src.clients.llm_client.ChatAnthropic')
    @patch.dict('os.environ', {'ANTHROPIC_API_KEY': 'test-key'})
    def test_anthropic_provider_explicit(self, mock_chat_anthropic):
        """Test explicit Anthropic provider"""
        mock_model = Mock()
        mock_chat_anthropic.return_value = mock_model
        
        result = get_llm_client('custom-model', model_provider='anthropic')
        
        mock_chat_anthropic.assert_called_once_with(
            model='custom-model',
            temperature=0.1,
            api_key='test-key'
        )
        assert isinstance(result, LLMClient)
    
    @patch.dict('os.environ', {}, clear=True)
    def test_anthropic_missing_api_key(self):
        """Test that missing Anthropic API key raises ValueError"""
        with pytest.raises(ValueError, match="ANTHROPIC_API_KEY environment variable not set"):
            get_llm_client('claude-3-5-sonnet', model_provider='anthropic')
    
    @patch('src.clients.llm_client.ChatBedrock')
    @patch.dict('os.environ', {'AWS_REGION': 'eu-west-1'})
    def test_bedrock_provider_explicit(self, mock_chat_bedrock):
        """Test explicit Bedrock provider"""
        mock_model = Mock()
        mock_chat_bedrock.return_value = mock_model
        
        result = get_llm_client('custom-model-id', model_provider='aws_bedrock')
        
        mock_chat_bedrock.assert_called_once_with(
            model_id='custom-model-id',
            model_kwargs={"temperature": 0.1},
            region_name='eu-west-1'
        )
        assert isinstance(result, LLMClient)
    
    @patch('src.clients.llm_client.AzureChatOpenAI')
    @patch.dict('os.environ', {
        'AZURE_OPENAI_API_KEY': 'test-key',
        'AZURE_OPENAI_LLM_ENDPOINT': 'https://{}.openai.azure.com/{}/v1',
        'AZURE_OPENAI_API_VERSION': '2024-02-15-preview',
        'LLM_MODEL': 'gpt-4'
    })
    def test_azure_openai_with_endpoint_template(self, mock_azure_chat):
        """Test Azure OpenAI with endpoint template"""
        mock_model = Mock()
        mock_azure_chat.return_value = mock_model
        
        result = get_llm_client('gpt-4', model_provider='azure_openai')
        
        # Verify endpoint was constructed correctly
        # Template format: model_name goes in first {}, azure_openai_api_version in second {}
        expected_endpoint = 'https://gpt-4.openai.azure.com/2024-02-15-preview/v1'
        mock_azure_chat.assert_called_once_with(
            azure_deployment='gpt-4',
            temperature=0.1,
            api_key='test-key',
            azure_endpoint=expected_endpoint,
            api_version='2024-02-15-preview'
        )
        assert isinstance(result, LLMClient)
    
    @patch.dict('os.environ', {
        'AZURE_OPENAI_API_KEY': 'test-key',
        'AZURE_OPENAI_LLM_ENDPOINT': 'https://{}.openai.azure.com/{}/v1',
    }, clear=True)
    def test_azure_openai_missing_template_vars(self):
        """Test Azure OpenAI with missing template variables"""
        with pytest.raises(ValueError, match="LLM_MODEL and AZURE_OPENAI_API_VERSION"):
            get_llm_client('gpt-4', model_provider='azure_openai')
    
    @patch.dict('os.environ', {}, clear=True)
    def test_azure_openai_missing_api_key(self):
        """Test Azure OpenAI with missing API key"""
        with pytest.raises(ValueError, match="AZURE_OPENAI_API_KEY environment variable must be set"):
            get_llm_client('gpt-4', model_provider='azure_openai')
    
    @patch.dict('os.environ', {
        'AZURE_OPENAI_API_KEY': 'test-key',
    }, clear=True)
    def test_azure_openai_missing_endpoint_config(self):
        """Test Azure OpenAI with missing endpoint configuration"""
        with pytest.raises(ValueError, match="AZURE_OPENAI_LLM_ENDPOINT"):
            get_llm_client('gpt-4', model_provider='azure_openai')
    
    def test_unknown_provider_explicit(self):
        """Test that explicit unknown provider raises ValueError"""
        with pytest.raises(ValueError, match="Unknown provider"):
            get_llm_client('model-name', model_provider='unknown_provider')
    
    @patch('src.clients.llm_client.ChatOpenAI')
    @patch.dict('os.environ', {'OPENAI_API_KEY': 'test-key'})
    def test_client_creation_with_correct_config(self, mock_chat_openai):
        """Test that LLMClient is created with correct system instruction"""
        mock_model = Mock()
        mock_chat_openai.return_value = mock_model
        
        result = get_llm_client('gpt-4o-mini', model_provider='openai')
        
        # Verify LLMClient was created with correct parameters
        assert isinstance(result, LLMClient)
        assert 'archetype' in result.system_instruction.lower()
        assert result.model == mock_model
    
    @patch('src.clients.llm_client.ChatOpenAI')
    @patch.dict('os.environ', {'OPENAI_API_KEY': 'test-key'})
    def test_provider_case_insensitive(self, mock_chat_openai):
        """Test that provider parameter is case-insensitive"""
        mock_model = Mock()
        mock_chat_openai.return_value = mock_model
        
        # Test uppercase
        result1 = get_llm_client('model', model_provider='OPENAI')
        mock_chat_openai.reset_mock()
        
        # Test mixed case
        result2 = get_llm_client('model', model_provider='OpenAI')
        
        # Both should work
        assert mock_chat_openai.call_count == 1
        assert isinstance(result1, LLMClient)
        assert isinstance(result2, LLMClient)

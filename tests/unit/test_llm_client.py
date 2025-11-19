"""Unit tests for LLM client functionality"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from src.etl.api_clients.llm_client import get_llm_client


class TestGetLLMClient:
    """Tests for get_llm_client function"""
    
    @patch('src.etl.api_clients.llm_client.OpenAILLM')
    @patch('src.etl.api_clients.llm_client.Agent')
    @patch.dict('os.environ', {'OPENAI_API_KEY': 'test-key'})
    def test_openai_provider_auto_detect(self, mock_agent, mock_openai_llm):
        """Test auto-detection of OpenAI provider from model name"""
        mock_model = Mock()
        mock_openai_llm.return_value = mock_model
        
        result = get_llm_client('gpt-4o-mini')
        
        mock_openai_llm.assert_called_once_with(
            model='gpt-4o-mini',
            temperature=0.1,
            api_key='test-key'
        )
        mock_agent.assert_called_once()
        assert result == mock_agent.return_value
    
    @patch('src.etl.api_clients.llm_client.OpenAILLM')
    @patch('src.etl.api_clients.llm_client.Agent')
    @patch.dict('os.environ', {'OPENAI_API_KEY': 'test-key'})
    def test_openai_provider_explicit(self, mock_agent, mock_openai_llm):
        """Test explicit OpenAI provider"""
        mock_model = Mock()
        mock_openai_llm.return_value = mock_model
        
        result = get_llm_client('custom-model', model_provider='openai')
        
        mock_openai_llm.assert_called_once_with(
            model='custom-model',
            temperature=0.1,
            api_key='test-key'
        )
        mock_agent.assert_called_once()
    
    @patch.dict('os.environ', {}, clear=True)
    def test_openai_missing_api_key(self):
        """Test that missing OpenAI API key raises ValueError"""
        with pytest.raises(ValueError, match="OPENAI_API_KEY environment variable not set"):
            get_llm_client('gpt-4o-mini')
    
    @patch('src.etl.api_clients.llm_client.AnthropicLLM')
    @patch('src.etl.api_clients.llm_client.Agent')
    @patch.dict('os.environ', {'ANTHROPIC_API_KEY': 'test-key'})
    def test_anthropic_provider_auto_detect(self, mock_agent, mock_anthropic_llm):
        """Test auto-detection of Anthropic provider from model name"""
        mock_model = Mock()
        mock_anthropic_llm.return_value = mock_model
        
        result = get_llm_client('claude-3-5-sonnet')
        
        mock_anthropic_llm.assert_called_once_with(
            model='claude-3-5-sonnet',
            temperature=0.1,
            api_key='test-key'
        )
        mock_agent.assert_called_once()
    
    @patch('src.etl.api_clients.llm_client.AnthropicLLM')
    @patch('src.etl.api_clients.llm_client.Agent')
    @patch.dict('os.environ', {'ANTHROPIC_API_KEY': 'test-key'})
    def test_anthropic_provider_explicit(self, mock_agent, mock_anthropic_llm):
        """Test explicit Anthropic provider"""
        mock_model = Mock()
        mock_anthropic_llm.return_value = mock_model
        
        result = get_llm_client('custom-model', model_provider='anthropic')
        
        mock_anthropic_llm.assert_called_once_with(
            model='custom-model',
            temperature=0.1,
            api_key='test-key'
        )
        mock_agent.assert_called_once()
    
    @patch.dict('os.environ', {}, clear=True)
    def test_anthropic_missing_api_key(self):
        """Test that missing Anthropic API key raises ValueError"""
        with pytest.raises(ValueError, match="ANTHROPIC_API_KEY environment variable not set"):
            get_llm_client('claude-3-5-sonnet', model_provider='anthropic')
    
    @patch('src.etl.api_clients.llm_client.BedrockLLM')
    @patch('src.etl.api_clients.llm_client.Agent')
    @patch.dict('os.environ', {'AWS_REGION': 'us-west-2'})
    def test_bedrock_provider_auto_detect_bedrock(self, mock_agent, mock_bedrock_llm):
        """Test auto-detection of Bedrock provider from 'bedrock' in model name"""
        mock_model = Mock()
        mock_bedrock_llm.return_value = mock_model
        
        result = get_llm_client('bedrock-model-id')
        
        mock_bedrock_llm.assert_called_once_with(
            model='bedrock-model-id',
            temperature=0.1,
            region='us-west-2'
        )
        mock_agent.assert_called_once()
    
    @patch('src.etl.api_clients.llm_client.BedrockLLM')
    @patch('src.etl.api_clients.llm_client.Agent')
    @patch.dict('os.environ', {})
    def test_bedrock_provider_auto_detect_arn(self, mock_agent, mock_bedrock_llm):
        """Test auto-detection of Bedrock provider from ARN"""
        mock_model = Mock()
        mock_bedrock_llm.return_value = mock_model
        
        result = get_llm_client('arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-v2')
        
        mock_bedrock_llm.assert_called_once_with(
            model='arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-v2',
            temperature=0.1,
            region='us-east-1'  # Default region
        )
        mock_agent.assert_called_once()
    
    @patch('src.etl.api_clients.llm_client.BedrockLLM')
    @patch('src.etl.api_clients.llm_client.Agent')
    @patch.dict('os.environ', {'AWS_REGION': 'eu-west-1'})
    def test_bedrock_provider_explicit(self, mock_agent, mock_bedrock_llm):
        """Test explicit Bedrock provider"""
        mock_model = Mock()
        mock_bedrock_llm.return_value = mock_model
        
        result = get_llm_client('custom-model-id', model_provider='aws_bedrock')
        
        mock_bedrock_llm.assert_called_once_with(
            model='custom-model-id',
            temperature=0.1,
            region='eu-west-1'
        )
        mock_agent.assert_called_once()
    
    @patch('src.etl.api_clients.llm_client.OpenAILLM')
    @patch('src.etl.api_clients.llm_client.Agent')
    @patch.dict('os.environ', {
        'AZURE_OPENAI_API_KEY': 'test-key',
        'AZURE_OPENAI_LLM_ENDPOINT': 'https://{}.openai.azure.com/{}/v1',
        'AZURE_OPENAI_API_VERSION': '2024-02-15-preview',
        'LLM_MODEL': 'gpt-4',
        'AZURE_API_ENDPOINT': 'test-endpoint'
    })
    def test_azure_openai_with_endpoint_template(self, mock_agent, mock_openai_llm):
        """Test Azure OpenAI with endpoint template"""
        mock_model = Mock()
        mock_openai_llm.return_value = mock_model
        
        result = get_llm_client('gpt-4', model_provider='azure_openai')
        
        # Verify endpoint was constructed correctly
        expected_endpoint = 'https://gpt-4.openai.azure.com/test-endpoint/v1'
        mock_openai_llm.assert_called_once_with(
            model='gpt-4',
            temperature=0.1,
            api_key='test-key',
            base_url=expected_endpoint,
            api_version='2024-02-15-preview'
        )
        mock_agent.assert_called_once()
    
    @patch.dict('os.environ', {
        'AZURE_OPENAI_API_KEY': 'test-key',
        'AZURE_OPENAI_LLM_ENDPOINT': 'https://{}.openai.azure.com/{}/v1',
    }, clear=True)
    def test_azure_openai_missing_template_vars(self):
        """Test Azure OpenAI with missing template variables"""
        with pytest.raises(ValueError, match="LLM_MODEL and AZURE_API_ENDPOINT"):
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
    
    @patch('src.etl.api_clients.llm_client.OpenAILLM')
    @patch('src.etl.api_clients.llm_client.Agent')
    @patch.dict('os.environ', {'OPENAI_API_KEY': 'test-key'})
    def test_unknown_provider_defaults_to_openai(self, mock_agent, mock_openai_llm):
        """Test that unknown model name defaults to OpenAI"""
        mock_model = Mock()
        mock_openai_llm.return_value = mock_model
        
        with patch('src.etl.api_clients.llm_client.logger') as mock_logger:
            result = get_llm_client('unknown-model-name')
        
        # Should log warning and default to OpenAI
        mock_logger.warning.assert_called_once()
        mock_openai_llm.assert_called_once()
        mock_agent.assert_called_once()
    
    def test_unknown_provider_explicit(self):
        """Test that explicit unknown provider raises ValueError"""
        with pytest.raises(ValueError, match="Unknown provider"):
            get_llm_client('model-name', model_provider='unknown_provider')
    
    @patch('src.etl.api_clients.llm_client.OpenAILLM')
    @patch('src.etl.api_clients.llm_client.Agent')
    @patch.dict('os.environ', {'OPENAI_API_KEY': 'test-key'})
    def test_agent_creation_with_correct_config(self, mock_agent, mock_openai_llm):
        """Test that Agent is created with correct name and instruction"""
        mock_model = Mock()
        mock_openai_llm.return_value = mock_model
        
        get_llm_client('gpt-4o-mini')
        
        # Verify Agent was created with correct parameters
        mock_agent.assert_called_once()
        call_args = mock_agent.call_args
        assert call_args.kwargs['name'] == 'archetype_classifier'
        assert 'archetype' in call_args.kwargs['instruction'].lower()
        assert call_args.kwargs['model'] == mock_model
    
    @patch('src.etl.api_clients.llm_client.OpenAILLM')
    @patch('src.etl.api_clients.llm_client.Agent')
    @patch.dict('os.environ', {'OPENAI_API_KEY': 'test-key'})
    def test_provider_case_insensitive(self, mock_agent, mock_openai_llm):
        """Test that provider parameter is case-insensitive"""
        mock_model = Mock()
        mock_openai_llm.return_value = mock_model
        
        # Test uppercase
        result1 = get_llm_client('model', model_provider='OPENAI')
        mock_openai_llm.reset_mock()
        
        # Test mixed case
        result2 = get_llm_client('model', model_provider='OpenAI')
        
        # Both should work
        assert mock_openai_llm.call_count == 1

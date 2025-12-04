"""LLM client for using Langchain ecosystem"""

import os
import logging
from typing import Any

from langchain_openai import ChatOpenAI, AzureChatOpenAI
from langchain_anthropic import ChatAnthropic
from langchain_aws import ChatBedrock
from langchain_core.language_models.chat_models import BaseChatModel

logger = logging.getLogger(__name__)


class LLMClient:
    """Wrapper for Langchain chat models that provides a simple interface"""
    
    def __init__(self, model: BaseChatModel, system_instruction: str):
        """
        Initialize LLM client with a Langchain chat model
        
        Args:
            model: Langchain chat model instance
            system_instruction: System instruction to prepend to queries
        """
        self.model = model
        self.system_instruction = system_instruction
    
    def run(self, prompt: str) -> Any:
        """
        Run the LLM with the given prompt
        
        Args:
            prompt: User prompt to send to LLM
            
        Returns:
            Response object with 'text' attribute containing the LLM response
        """
        from langchain_core.messages import HumanMessage, SystemMessage
        
        messages = [
            SystemMessage(content=self.system_instruction),
            HumanMessage(content=prompt)
        ]
        
        response = self.model.invoke(messages)
        
        # Create a simple response object with text attribute
        class Response:
            def __init__(self, content):
                self.text = content
        
        return Response(response.content)


def get_llm_client(model_name: str, model_provider: str) -> LLMClient:
    """
    Get LLM client based on model name and provider configuration.
    
    Supports:
    - OpenAI models (gpt-4o, gpt-4o-mini, gpt-4-turbo)
    - Azure OpenAI models (deployments)
    - Anthropic models (claude-3-5-sonnet, claude-3-opus, claude-3-haiku)
    - AWS Bedrock models (via model ID)
    
    Args:
        model_name: Name or identifier of the LLM model
        model_provider: Required to differentiate between providers with the same models (azure_openai, anthropic, openai, aws_bedrock)
        
    Returns:
        Configured LLMClient instance
        
    Raises:
        ValueError: If model configuration is invalid
    """
    
    # Determine provider
    provider = model_provider.lower()
    
    if provider == 'azure_openai':
        api_key = os.getenv('AZURE_OPENAI_API_KEY')
        endpoint_template = os.getenv('AZURE_OPENAI_LLM_ENDPOINT')
        api_version = os.getenv('AZURE_OPENAI_API_VERSION')
        azure_openai_api_version = os.getenv('AZURE_OPENAI_API_VERSION')
        
        if not api_key:
            raise ValueError("AZURE_OPENAI_API_KEY environment variable must be set")
        
        if endpoint_template:
            # Construct endpoint from template: LLM_MODEL goes in first {}, azure_openai_api_version in second {}
            if not model_name or not azure_openai_api_version:
                raise ValueError(
                    "LLM_MODEL and AZURE_OPENAI_API_VERSION environment variables must be set "
                    "when using AZURE_OPENAI_LLM_ENDPOINT template"
                )
            endpoint = endpoint_template.format(model_name, azure_openai_api_version)
        else:
            raise ValueError(
                "AZURE_OPENAI_LLM_ENDPOINT, AZURE_OPENAI_API_KEY, and AZURE_OPENAI_API_VERSION environment variables must be set"
            )
        
        model = AzureChatOpenAI(
            azure_deployment=model_name,
            temperature=0.1,
            api_key=api_key,
            azure_endpoint=endpoint,
            api_version=api_version
        )
    
    elif provider == 'openai':
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable not set")
        
        model = ChatOpenAI(
            model=model_name,
            temperature=0.1,
            api_key=api_key
        )
    
    elif provider == 'anthropic':
        api_key = os.getenv('ANTHROPIC_API_KEY')
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY environment variable not set")
        
        model = ChatAnthropic(
            model=model_name,
            temperature=0.1,
            api_key=api_key
        )
    
    elif provider == 'aws_bedrock':
        region = os.getenv('AWS_REGION', 'us-east-1')
        model = ChatBedrock(
            model_id=model_name,
            model_kwargs={"temperature": 0.1},
            region_name=region
        )
    
    else:
        raise ValueError(
            f"Unknown provider: {provider}. Must be one of: "
            "openai, azure_openai, anthropic, aws_bedrock"
        )
    
    # Create client with the model
    client = LLMClient(
        model=model,
        system_instruction="You are an expert Magic: The Gathering deck analyst. Classify decklists into archetypes based on card synergies and strategies."
    )
    
    return client


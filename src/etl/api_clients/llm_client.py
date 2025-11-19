"""LLM client for using strands-agents"""

import os
import logging
from typing import Optional

from strands_agents import Agent
from strands_agents.models import BedrockLLM, OpenAILLM, AnthropicLLM

logger = logging.getLogger(__name__)


def get_llm_client(model_name: str, model_provider: str) -> Agent:
    """
    Get LLM client based on model name and provider configuration.
    
    Supports:
    - OpenAI models (gpt-4o, gpt-4o-mini, gpt-4-turbo)
    - Azure OpenAI models (deployments)
    - Anthropic models (claude-3-5-sonnet, claude-3-opus, claude-3-haiku)
    - AWS Bedrock models (via model ID)
    
    Args:
        model_name: Name or identifier of the LLM model
        model_provider: Optional explicit provider (azure_openai, anthropic, openai, aws_bedrock)
                       If not provided, infers from model_name
        
    Returns:
        Configured Agent instance
        
    Raises:
        ValueError: If model configuration is invalid
    """
    # Determine provider
    provider = model_provider.lower()
    
    if provider == 'azure_openai':
        api_key = os.getenv('AZURE_OPENAI_API_KEY')
        endpoint_template = os.getenv('AZURE_OPENAI_LLM_ENDPOINT')
        api_version = os.getenv('AZURE_OPENAI_API_VERSION')
        azure_api_endpoint = os.getenv('AZURE_API_ENDPOINT')
        
        if not api_key:
            raise ValueError("AZURE_OPENAI_API_KEY environment variable must be set")
        
        if endpoint_template:
            # Construct endpoint from template: LLM_MODEL goes in first {}, AZURE_API_ENDPOINT in second {}
            if not model_name or not azure_api_endpoint:
                raise ValueError(
                    "LLM_MODEL and AZURE_API_ENDPOINT environment variables must be set "
                    "when using AZURE_OPENAI_LLM_ENDPOINT template"
                )
            endpoint = endpoint_template.format(model_name, azure_api_endpoint)
        else:
                raise ValueError(
                    "AZURE_OPENAI_LLM_ENDPOINT, AZURE_OPENAI_API_KEY, and AZURE_OPENAI_API_VERSION environment variables must be set"
                )
        
        model = OpenAILLM(
            model=model_name,
            temperature=0.1,
            api_key=api_key,
            base_url=endpoint,
            api_version=api_version
        )
    
    elif provider == 'anthropic':
        api_key = os.getenv('ANTHROPIC_API_KEY')
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY environment variable not set")
        
        model = AnthropicLLM(
            model=model_name,
            temperature=0.1,
            api_key=api_key
        )
    
    elif provider == 'aws_bedrock':
        region = os.getenv('AWS_REGION', 'us-east-1')
        model = BedrockLLM(
            model=model_name,
            temperature=0.1,
            region=region
        )
    
    else:
        raise ValueError(
            f"Unknown provider: {provider}. Must be one of: "
            "openai, azure_openai, anthropic, aws_bedrock"
        )
    
    # Create agent with the model
    agent = Agent(
        name="archetype_classifier",
        model=model,
        instruction="You are an expert Magic: The Gathering deck analyst. Classify decklists into archetypes based on card synergies and strategies."
    )
    
    return agent


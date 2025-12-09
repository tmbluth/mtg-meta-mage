"""LLM prompts for generating agent responses and welcome messages."""

import json
import logging
import os
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def get_llm_client():
    """Get an LLM client configured from environment variables."""
    from src.clients.llm_client import get_llm_client as _get_llm_client
    
    model = os.getenv("LARGE_LANGUAGE_MODEL")
    provider = os.getenv("LLM_PROVIDER")
    
    if not model or not provider:
        raise RuntimeError("LARGE_LANGUAGE_MODEL and LLM_PROVIDER environment variables must be set")
    
    return _get_llm_client(model, provider)


AGENT_RESPONSE_PROMPT = """You are MTG Meta Mage, an AI-powered Magic: The Gathering coach and meta analyst.

## Conversation Context
- Format: {format}
- Days window: {days}
- User's archetype: {archetype}

## Recent Conversation
{conversation_history}

## User's Latest Message
{user_message}

## Tool Results
{tool_results}

## Available Tools for Next Steps
{tool_catalog}

## Instructions
1. Respond naturally to the user's message
2. If tool results are present, interpret and explain them conversationally
3. Highlight key insights and actionable recommendations
4. Suggest relevant next steps using available tools when appropriate
5. Use Magic: The Gathering terminology appropriately
6. Keep the response focused and avoid overwhelming detail
7. Do NOT return raw JSON - always provide a conversational response

Generate your response:"""


def generate_agent_response(
    user_message: str,
    conversation_history: List[Dict[str, str]],
    tool_results: List[Dict[str, Any]],
    conversation_context: Dict[str, Any],
    tool_catalog: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """
    Generate a natural language response considering all context.
    
    This is the single entry point for LLM response generation after
    any tool calls (or no tool calls) have completed.
    
    Args:
        user_message: The user's latest message
        conversation_history: List of prior messages with 'role' and 'content'
        tool_results: List of dicts with 'tool_name' and 'response' keys (can be empty)
        conversation_context: Current state (format, days, archetype, etc.)
        tool_catalog: Optional list of available tools for suggesting next steps
    
    Returns:
        Natural language response to the user
    """
    # Format conversation history
    if conversation_history:
        history_lines = []
        for msg in conversation_history[-5:]:  # Last 5 messages for context
            role = msg.get("role", "unknown")
            content = msg.get("content", "")
            history_lines.append(f"{role}: {content}")
        history_text = "\n".join(history_lines)
    else:
        history_text = "(No prior conversation)"
    
    # Format tool results
    if tool_results:
        results_lines = []
        for result in tool_results:
            tool_name = result.get("tool_name", "unknown")
            response = result.get("response", {})
            results_lines.append(f"### {tool_name}")
            results_lines.append(json.dumps(response, indent=2))
            results_lines.append("")
        results_text = "\n".join(results_lines)
    else:
        results_text = "(No tool results)"
    
    # Format tool catalog
    if tool_catalog:
        catalog_lines = [f"- {t['name']}: {t.get('description', 'No description')}" for t in tool_catalog]
        catalog_text = "\n".join(catalog_lines)
    else:
        catalog_text = "None available"
    
    prompt = AGENT_RESPONSE_PROMPT.format(
        format=conversation_context.get("format", "Not specified"),
        days=conversation_context.get("days", "Not specified"),
        archetype=conversation_context.get("archetype", "Not specified"),
        conversation_history=history_text,
        user_message=user_message,
        tool_results=results_text,
        tool_catalog=catalog_text,
    )
    
    client = get_llm_client()
    response = client.run(prompt)
    return response.text


WELCOME_MESSAGE_PROMPT = """You are MTG Meta Mage, an AI-powered Magic: The Gathering coach and meta analyst.

Generate a warm, helpful welcome message for a user who just opened the app. The message should:
1. Introduce yourself and your capabilities
2. Briefly explain the two main workflows available
3. Mention some example queries users can try
4. Be conversational and friendly
5. Keep it concise (2-3 short paragraphs)

## Available Formats
{formats}

## Available Workflows
{workflows}

## Available Tools
{tool_catalog}

Generate a welcoming message:"""


def generate_welcome_message(
    tool_catalog: List[Dict[str, Any]],
    workflows: List[Dict[str, Any]],
    available_formats: List[str],
) -> str:
    """
    Generate an LLM-interpreted welcome message for new sessions.
    
    Args:
        tool_catalog: List of available MCP tools with names and descriptions
        workflows: List of workflow definitions with descriptions and examples
        available_formats: List of supported tournament formats
    
    Returns:
        Natural language welcome message
    """
    try:
        # Format workflows for prompt
        workflow_lines = []
        for w in workflows:
            workflow_lines.append(f"- {w['name']}: {w.get('description', '')}")
            examples = w.get("example_queries", [])
            if examples:
                workflow_lines.append(f"  Examples: {', '.join(examples[:2])}")
        workflows_text = "\n".join(workflow_lines) if workflow_lines else "None defined"
        
        # Format tool catalog for prompt
        catalog_lines = [f"- {t['name']}: {t.get('description', 'No description')}" for t in tool_catalog]
        catalog_text = "\n".join(catalog_lines) if catalog_lines else "None available"
        
        prompt = WELCOME_MESSAGE_PROMPT.format(
            formats=", ".join(available_formats) if available_formats else "None available",
            workflows=workflows_text,
            tool_catalog=catalog_text,
        )
        
        client = get_llm_client()
        response = client.run(prompt)
        return response.text
    except Exception as exc:
        logger.warning("Failed to generate LLM welcome message: %s", exc)
        # Return fallback static message
        return (
            "Welcome to MTG Meta Mage! I'm your AI-powered Magic: The Gathering coach and meta analyst. "
            "I can help you explore the competitive meta and optimize your deck. "
            "Try asking about the top decks in a format, or paste your deck list for personalized coaching."
        )

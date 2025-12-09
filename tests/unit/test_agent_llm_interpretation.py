"""Tests for LLM response generation and welcome messages."""

from unittest.mock import MagicMock, patch

import pytest

from src.app.agent_api.state import create_initial_state


class TestGenerateAgentResponse:
    """Tests for the unified agent response generation."""

    @patch("src.app.agent_api.prompts.get_llm_client")
    def test_generate_response_with_tool_results(self, mock_get_client):
        from src.app.agent_api.prompts import generate_agent_response
        
        mock_client = MagicMock()
        mock_client.run.return_value.text = "Based on the Modern meta, Boros Energy leads with 12.3% share."
        mock_get_client.return_value = mock_client
        
        tool_results = [
            {
                "tool_name": "get_format_meta_rankings",
                "response": {
                    "archetypes": [
                        {"name": "Boros Energy", "meta_share": 12.3, "win_rate": 54.2},
                        {"name": "Golgari Yawgmoth", "meta_share": 8.7, "win_rate": 52.1},
                    ]
                }
            }
        ]
        
        result = generate_agent_response(
            user_message="What's the Modern meta?",
            conversation_history=[],
            tool_results=tool_results,
            conversation_context={"format": "Modern", "days": 30}
        )
        
        assert isinstance(result, str)
        assert len(result) > 0
        mock_client.run.assert_called_once()

    @patch("src.app.agent_api.prompts.get_llm_client")
    def test_generate_response_without_tool_results(self, mock_get_client):
        from src.app.agent_api.prompts import generate_agent_response
        
        mock_client = MagicMock()
        mock_client.run.return_value.text = "I'd be happy to help! What format are you interested in?"
        mock_get_client.return_value = mock_client
        
        result = generate_agent_response(
            user_message="Hello!",
            conversation_history=[],
            tool_results=[],  # No tool results
            conversation_context={"format": None}
        )
        
        assert isinstance(result, str)
        assert len(result) > 0

    @patch("src.app.agent_api.prompts.get_llm_client")
    def test_generate_response_uses_conversation_context(self, mock_get_client):
        from src.app.agent_api.prompts import generate_agent_response
        
        mock_client = MagicMock()
        mock_client.run.return_value.text = "Your Burn deck has good matchups against control."
        mock_get_client.return_value = mock_client
        
        tool_results = [{"tool_name": "get_deck_matchup_stats", "response": {"matchups": []}}]
        context = {"format": "Modern", "archetype": "Burn", "days": 30}
        
        generate_agent_response(
            user_message="What are my matchups?",
            conversation_history=[],
            tool_results=tool_results,
            conversation_context=context
        )
        
        # Verify context is passed to the LLM prompt
        call_args = mock_client.run.call_args[0][0]
        assert "Modern" in call_args
        assert "Burn" in call_args

    @patch("src.app.agent_api.prompts.get_llm_client")
    def test_generate_response_includes_conversation_history(self, mock_get_client):
        from src.app.agent_api.prompts import generate_agent_response
        
        mock_client = MagicMock()
        mock_client.run.return_value.text = "Here are the top decks you asked about."
        mock_get_client.return_value = mock_client
        
        history = [
            {"role": "user", "content": "What format should I play?"},
            {"role": "assistant", "content": "Modern is very popular right now."},
        ]
        
        generate_agent_response(
            user_message="Show me the top decks",
            conversation_history=history,
            tool_results=[],
            conversation_context={"format": "Modern"}
        )
        
        # Verify history is included in prompt
        call_args = mock_client.run.call_args[0][0]
        assert "What format should I play?" in call_args
        assert "Modern is very popular" in call_args

    @patch("src.app.agent_api.prompts.get_llm_client")
    def test_generate_response_includes_tool_catalog(self, mock_get_client):
        from src.app.agent_api.prompts import generate_agent_response
        
        mock_client = MagicMock()
        mock_client.run.return_value.text = "Here are the results. You can also use optimize_sideboard."
        mock_get_client.return_value = mock_client
        
        tool_catalog = [
            {"name": "get_format_meta_rankings", "description": "Get meta rankings"},
            {"name": "optimize_sideboard", "description": "Optimize sideboard"},
        ]
        
        generate_agent_response(
            user_message="What's the meta?",
            conversation_history=[],
            tool_results=[{"tool_name": "get_format_meta_rankings", "response": {}}],
            conversation_context={"format": "Modern"},
            tool_catalog=tool_catalog
        )
        
        # Verify tool catalog is in prompt
        call_args = mock_client.run.call_args[0][0]
        assert "optimize_sideboard" in call_args

    @patch("src.app.agent_api.prompts.get_llm_client")
    def test_generate_response_with_multiple_tool_results(self, mock_get_client):
        from src.app.agent_api.prompts import generate_agent_response
        
        mock_client = MagicMock()
        mock_client.run.return_value.text = "The meta is led by Boros Energy, and your Burn deck has a 45% matchup against them."
        mock_get_client.return_value = mock_client
        
        tool_results = [
            {
                "tool_name": "get_format_meta_rankings",
                "response": {"archetypes": [{"name": "Boros Energy", "meta_share": 12.3}]}
            },
            {
                "tool_name": "get_deck_matchup_stats",
                "response": {"matchups": [{"opponent": "Boros Energy", "win_rate": 45.0}]}
            }
        ]
        
        result = generate_agent_response(
            user_message="How do I match up against the meta?",
            conversation_history=[],
            tool_results=tool_results,
            conversation_context={"format": "Modern", "archetype": "Burn"}
        )
        
        assert isinstance(result, str)
        assert len(result) > 0
        
        # Verify both tool results are in prompt
        call_args = mock_client.run.call_args[0][0]
        assert "get_format_meta_rankings" in call_args
        assert "get_deck_matchup_stats" in call_args


class TestLLMGenerateWelcomeMessage:
    """Tests for LLM-generated welcome messages."""

    @patch("src.app.agent_api.prompts.get_llm_client")
    def test_generate_welcome_message_returns_natural_language(self, mock_get_client):
        from src.app.agent_api.prompts import generate_welcome_message
        
        mock_client = MagicMock()
        mock_client.run.return_value.text = "Welcome to MTG Meta Mage! I can help you analyze the meta and coach your deck."
        mock_get_client.return_value = mock_client
        
        tool_catalog = [
            {"name": "get_format_meta_rankings", "description": "Get format-wide meta rankings"},
            {"name": "optimize_sideboard", "description": "Optimize sideboard"},
        ]
        workflows = [
            {"name": "meta_research", "description": "Format-wide analytics", "example_queries": ["What are the top decks?"]},
            {"name": "deck_coaching", "description": "Personalized coaching", "example_queries": ["Optimize my sideboard"]},
        ]
        formats = ["Modern", "Pioneer", "Legacy"]
        
        result = generate_welcome_message(
            tool_catalog=tool_catalog,
            workflows=workflows,
            available_formats=formats
        )
        
        assert isinstance(result, str)
        assert len(result) > 0
        mock_client.run.assert_called_once()

    @patch("src.app.agent_api.prompts.get_llm_client")
    def test_generate_welcome_message_includes_formats(self, mock_get_client):
        from src.app.agent_api.prompts import generate_welcome_message
        
        mock_client = MagicMock()
        mock_client.run.return_value.text = "Welcome! I support Modern, Pioneer, and Legacy."
        mock_get_client.return_value = mock_client
        
        formats = ["Modern", "Pioneer", "Legacy"]
        
        generate_welcome_message(
            tool_catalog=[],
            workflows=[],
            available_formats=formats
        )
        
        call_args = mock_client.run.call_args[0][0]
        assert "Modern" in call_args
        assert "Pioneer" in call_args
        assert "Legacy" in call_args

    @patch("src.app.agent_api.prompts.get_llm_client")
    def test_generate_welcome_message_includes_workflow_descriptions(self, mock_get_client):
        from src.app.agent_api.prompts import generate_welcome_message
        
        mock_client = MagicMock()
        mock_client.run.return_value.text = "I can help with meta research and deck coaching."
        mock_get_client.return_value = mock_client
        
        workflows = [
            {"name": "meta_research", "description": "Format-wide analytics", "example_queries": []},
            {"name": "deck_coaching", "description": "Personalized coaching", "example_queries": []},
        ]
        
        generate_welcome_message(
            tool_catalog=[],
            workflows=workflows,
            available_formats=[]
        )
        
        call_args = mock_client.run.call_args[0][0]
        assert "meta_research" in call_args or "meta research" in call_args.lower()
        assert "deck_coaching" in call_args or "deck coaching" in call_args.lower()

    @patch("src.app.agent_api.prompts.get_llm_client")
    def test_generate_welcome_message_handles_llm_error_gracefully(self, mock_get_client):
        from src.app.agent_api.prompts import generate_welcome_message
        
        mock_get_client.side_effect = RuntimeError("LLM unavailable")
        
        # Should return a fallback message, not raise
        result = generate_welcome_message(
            tool_catalog=[],
            workflows=[],
            available_formats=["Modern"]
        )
        
        assert isinstance(result, str)
        assert len(result) > 0

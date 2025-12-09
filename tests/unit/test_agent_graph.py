"""Tests for LangGraph routing and blocking logic."""

from unittest.mock import MagicMock, patch

from src.app.agent_api import graph
from src.app.agent_api.graph import (
    classify_intent,
    enforce_blocking,
    update_workflow,
    generate_response,
)
from src.app.agent_api.state import create_initial_state


def test_classify_meta_research_intent(monkeypatch):
    state = create_initial_state()
    monkeypatch.setattr(graph, "_classify_with_llm", lambda prompt: '{"intent": "meta_research"}')
    intent = classify_intent("What's the meta look like right now?", state)
    assert intent == "meta_research"


def test_classify_deck_coaching_intent_with_deck_text(monkeypatch):
    state = create_initial_state()
    monkeypatch.setattr(graph, "_classify_with_llm", lambda prompt: '{"intent": "deck_coaching"}')
    intent = classify_intent("Here is my deck list: 4 Lightning Bolt", state)
    assert intent == "deck_coaching"


def test_unknown_intent_requests_clarification(monkeypatch):
    state = create_initial_state()
    monkeypatch.setattr(graph, "_classify_with_llm", lambda prompt: '{"intent": "unknown_value"}')
    intent = classify_intent("???", state)
    assert intent == "unknown"


def test_blocking_requires_format():
    state = create_initial_state()
    allowed, reason = enforce_blocking(state, intent="meta_research")
    assert allowed is False
    assert "format" in reason.lower()


def test_interleaving_preserves_state_between_workflows():
    state = create_initial_state()
    state["format"] = "Modern"
    state["days"] = 30

    state = update_workflow(state, "meta_research")
    assert state["current_workflow"] == "meta_research"

    state = update_workflow(state, "deck_coaching")
    assert state["current_workflow"] == "deck_coaching"
    assert state["format"] == "Modern"
    assert state["days"] == 30


class TestGenerateResponse:
    """Tests for unified LLM response generation."""

    @patch("src.app.agent_api.graph.generate_agent_response")
    def test_generate_response_with_tool_results(self, mock_generate):
        """Test that generate_response calls the agent response function."""
        mock_generate.return_value = "The Modern meta is dominated by Boros Energy at 12%."
        
        state = create_initial_state()
        state["format"] = "Modern"
        state["days"] = 30
        state["tool_catalog"] = [{"name": "get_format_meta_rankings", "description": "Get rankings"}]
        
        tool_results = [
            {
                "tool_name": "get_format_meta_rankings",
                "response": {"archetypes": [{"name": "Boros Energy", "meta_share": 12.0}]}
            }
        ]
        
        result = generate_response(state, "What's the meta?", tool_results)
        
        assert isinstance(result, str)
        mock_generate.assert_called_once()

    @patch("src.app.agent_api.graph.generate_agent_response")
    def test_generate_response_uses_tool_catalog_from_state(self, mock_generate):
        """Test that tool_catalog from state is passed to response generation."""
        mock_generate.return_value = "Analysis complete."
        
        tool_catalog = [
            {"name": "get_format_meta_rankings", "description": "Get rankings"},
            {"name": "optimize_sideboard", "description": "Optimize sideboard"}
        ]
        
        state = create_initial_state()
        state["format"] = "Modern"
        state["tool_catalog"] = tool_catalog
        
        generate_response(state, "What's the meta?", [])
        
        # Verify tool_catalog was passed
        call_kwargs = mock_generate.call_args.kwargs
        assert call_kwargs.get("tool_catalog") == tool_catalog

    @patch("src.app.agent_api.graph.generate_agent_response")
    def test_generate_response_passes_conversation_context(self, mock_generate):
        """Test that conversation context is passed to response generation."""
        mock_generate.return_value = "Your Burn deck has good matchups."
        
        state = create_initial_state()
        state["format"] = "Modern"
        state["days"] = 30
        state["archetype"] = "Burn"
        
        generate_response(state, "What are my matchups?", [])
        
        # Verify context was passed
        call_kwargs = mock_generate.call_args.kwargs
        context = call_kwargs.get("conversation_context", {})
        assert context.get("format") == "Modern"
        assert context.get("days") == 30
        assert context.get("archetype") == "Burn"

    @patch("src.app.agent_api.graph.generate_agent_response")
    def test_generate_response_without_tool_results(self, mock_generate):
        """Test that generate_response works with empty tool results."""
        mock_generate.return_value = "I'd be happy to help!"
        
        state = create_initial_state()
        state["format"] = "Modern"
        
        result = generate_response(state, "Hello!", [])
        
        assert isinstance(result, str)
        # Verify empty tool_results was passed
        call_kwargs = mock_generate.call_args.kwargs
        assert call_kwargs.get("tool_results") == []


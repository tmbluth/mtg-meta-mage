"""Tests for LangGraph routing and blocking logic."""

from src.app.agent_api import graph
from src.app.agent_api.graph import classify_intent, enforce_blocking, update_workflow
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


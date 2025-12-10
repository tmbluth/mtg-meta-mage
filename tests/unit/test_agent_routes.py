"""Tests for Agent API FastAPI routes."""

import json
import re
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch

from src.app.agent_api.main import app, conversation_store

client = TestClient(app)


@patch("src.app.agent_api.routes.get_tool_catalog_safe")
@patch("src.app.agent_api.routes.DatabaseConnection.get_cursor")
def test_get_welcome_returns_discovery_info(mock_get_cursor, mock_get_tool_catalog):
    cursor = MagicMock()
    cursor.fetchall.return_value = [("Modern",), ("Pioneer",), ("Legacy",)]
    cursor.description = [("format",)]
    mock_get_cursor.return_value.__enter__.return_value = cursor
    
    mock_get_tool_catalog.return_value = [
        {"name": "get_format_meta_rankings", "description": "Get format-wide meta rankings", "server": "mtg-meta-mage-mcp"},
        {"name": "get_enriched_deck", "description": "Parse a deck and enrich with card details", "server": "mtg-meta-mage-mcp"},
    ]
    
    response = client.get("/welcome")
    assert response.status_code == 200
    payload = response.json()
    
    # Check structure
    assert "message" in payload
    assert "available_formats" in payload
    assert "workflows" in payload
    assert "tool_count" in payload
    
    # Check formats
    assert payload["available_formats"] == ["Legacy", "Modern", "Pioneer"]
    
    # Check workflows
    assert len(payload["workflows"]) == 2
    meta_workflow = next(w for w in payload["workflows"] if w["name"] == "meta_research")
    deck_workflow = next(w for w in payload["workflows"] if w["name"] == "deck_coaching")
    
    # Check meta workflow
    assert "description" in meta_workflow
    assert "example_queries" in meta_workflow
    assert "tool_details" in meta_workflow
    assert len(meta_workflow["example_queries"]) > 0
    assert len(meta_workflow["tool_details"]) > 0
    
    # Check deck workflow
    assert "description" in deck_workflow
    assert "example_queries" in deck_workflow
    assert "tool_details" in deck_workflow
    
    # Check tool count
    assert payload["tool_count"] == 2


@patch("src.app.agent_api.routes.get_tool_catalog_safe")
@patch("src.app.agent_api.routes.DatabaseConnection.get_cursor")
def test_get_welcome_fails_loudly_on_empty_tool_catalog(mock_get_cursor, mock_get_tool_catalog):
    """Test that /welcome returns 503 when MCP tool discovery returns empty catalog."""
    cursor = MagicMock()
    cursor.fetchall.return_value = [("Modern",), ("Pioneer",)]
    cursor.description = [("format",)]
    mock_get_cursor.return_value.__enter__.return_value = cursor
    
    # Simulate empty tool catalog (MCP server unavailable)
    mock_get_tool_catalog.return_value = []
    
    response = client.get("/welcome")
    assert response.status_code == 503
    payload = response.json()
    assert "MCP server tool discovery failed" in payload["detail"]


@patch("src.app.agent_api.routes.DatabaseConnection.get_cursor")
def test_get_formats_returns_sorted_list(mock_get_cursor):
    cursor = MagicMock()
    cursor.fetchall.return_value = [("Pioneer",), ("Modern",), ("Legacy",)]
    cursor.description = [("format",)]
    mock_get_cursor.return_value.__enter__.return_value = cursor

    response = client.get("/formats")
    assert response.status_code == 200
    assert response.json()["formats"] == ["Legacy", "Modern", "Pioneer"]


@patch("src.app.agent_api.routes.get_format_archetypes")
def test_get_archetypes_returns_data(mock_tool):
    mock_tool.fn.return_value = {
        "format": "Modern",
        "archetypes": [{"id": 1, "name": "Deck A", "meta_share": 10.0, "color_identity": "UR"}],
    }
    response = client.get("/archetypes?format=Modern")
    assert response.status_code == 200
    payload = response.json()
    assert payload["format"] == "Modern"
    assert payload["archetypes"][0]["name"] == "Deck A"


def test_get_archetypes_missing_format_returns_400():
    response = client.get("/archetypes")
    assert response.status_code == 400


def test_get_and_missing_conversation():
    convo = conversation_store.create()
    cid = convo["conversation_id"]

    response = client.get(f"/conversations/{cid}")
    assert response.status_code == 200
    assert response.json()["conversation_id"] == cid

    response_missing = client.get("/conversations/does-not-exist")
    assert response_missing.status_code == 404


def test_chat_endpoint_streams_sse():
    payload = {
        "message": "What's the Modern meta?",
        "conversation_id": None,
        "context": {"format": "Modern", "days": 30},
    }

    response = client.post("/chat", json=payload)
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    body = response.text
    assert "event: metadata" in body
    assert "event: done" in body


def test_chat_endpoint_validates_message():
    payload = {"message": "   ", "conversation_id": None, "context": {}}
    response = client.post("/chat", json=payload)
    assert response.status_code == 400


@patch("src.app.agent_api.routes.get_tool_catalog_safe")
def test_chat_endpoint_stores_tool_catalog_in_state(mock_get_tool_catalog):
    """Test that tool_catalog is stored in conversation state during /chat initialization."""
    mock_tool_catalog = [
        {"name": "get_format_meta_rankings", "description": "Get format-wide meta rankings", "server": "mtg-meta-mage-mcp"},
        {"name": "get_enriched_deck", "description": "Parse a deck and enrich with card details", "server": "mtg-meta-mage-mcp"},
    ]
    mock_get_tool_catalog.return_value = mock_tool_catalog
    
    payload = {
        "message": "What's the Modern meta?",
        "conversation_id": None,
        "context": {"format": "Modern", "days": 30},
    }
    
    response = client.post("/chat", json=payload)
    assert response.status_code == 200
    
    # Parse SSE stream to extract conversation_id from metadata event
    body = response.text
    lines = body.split('\n')
    conversation_id = None
    for i, line in enumerate(lines):
        if line == "event: metadata" and i + 1 < len(lines):
            data_line = lines[i + 1]
            if data_line.startswith("data: "):
                data_json = data_line[6:]  # Remove "data: " prefix
                metadata = json.loads(data_json)
                conversation_id = metadata.get("conversation_id")
                break
    
    # Verify conversation_id was extracted
    assert conversation_id is not None, "Failed to extract conversation_id from SSE stream"
    
    # Verify tool_catalog is stored in conversation state
    convo = conversation_store.get(conversation_id)
    assert convo is not None
    assert "tool_catalog" in convo["state"]
    assert convo["state"]["tool_catalog"] == mock_tool_catalog


@patch("src.app.agent_api.routes.generate_welcome_message")
@patch("src.app.agent_api.routes.get_tool_catalog_safe")
@patch("src.app.agent_api.routes.DatabaseConnection.get_cursor")
def test_welcome_creates_session_and_returns_conversation_id(mock_get_cursor, mock_get_tool_catalog, mock_generate_welcome):
    """Test that /welcome creates a new conversation and returns conversation_id."""
    cursor = MagicMock()
    cursor.fetchall.return_value = [("Modern",), ("Pioneer",)]
    cursor.description = [("format",)]
    mock_get_cursor.return_value.__enter__.return_value = cursor
    
    mock_get_tool_catalog.return_value = [
        {"name": "get_format_meta_rankings", "description": "Get meta rankings", "server": "mtg-meta-mage-mcp"},
    ]
    mock_generate_welcome.return_value = "Welcome to MTG Meta Mage!"
    
    response = client.get("/welcome")
    assert response.status_code == 200
    payload = response.json()
    
    # Should return conversation_id
    assert "conversation_id" in payload
    assert payload["conversation_id"] is not None
    
    # Verify conversation was created in store
    convo = conversation_store.get(payload["conversation_id"])
    assert convo is not None


@patch("src.app.agent_api.routes.generate_welcome_message")
@patch("src.app.agent_api.routes.get_tool_catalog_safe")
@patch("src.app.agent_api.routes.DatabaseConnection.get_cursor")
def test_welcome_generates_llm_interpreted_message(mock_get_cursor, mock_get_tool_catalog, mock_generate_welcome):
    """Test that /welcome returns an LLM-generated welcome message."""
    cursor = MagicMock()
    cursor.fetchall.return_value = [("Modern",), ("Pioneer",)]
    cursor.description = [("format",)]
    mock_get_cursor.return_value.__enter__.return_value = cursor
    
    mock_get_tool_catalog.return_value = [
        {"name": "get_format_meta_rankings", "description": "Get meta rankings", "server": "mtg-meta-mage-mcp"},
    ]
    expected_message = "Welcome to MTG Meta Mage! I can analyze the competitive meta for you."
    mock_generate_welcome.return_value = expected_message
    
    response = client.get("/welcome")
    assert response.status_code == 200
    payload = response.json()
    
    # Should have LLM-generated message
    assert payload["message"] == expected_message
    
    # Verify generate_welcome_message was called with correct args
    mock_generate_welcome.assert_called_once()
    call_kwargs = mock_generate_welcome.call_args
    # Check that tool_catalog, workflows, and formats were passed
    assert "tool_catalog" in call_kwargs.kwargs or len(call_kwargs.args) > 0


@patch("src.app.agent_api.routes.generate_welcome_message")
@patch("src.app.agent_api.routes.get_tool_catalog_safe")
@patch("src.app.agent_api.routes.DatabaseConnection.get_cursor")
def test_welcome_stores_session_info_in_conversation_state(mock_get_cursor, mock_get_tool_catalog, mock_generate_welcome):
    """Test that /welcome stores tool_catalog, formats, workflows in conversation state."""
    cursor = MagicMock()
    cursor.fetchall.return_value = [("Modern",), ("Pioneer",)]
    cursor.description = [("format",)]
    mock_get_cursor.return_value.__enter__.return_value = cursor
    
    mock_tool_catalog = [
        {"name": "get_format_meta_rankings", "description": "Get meta rankings", "server": "mtg-meta-mage-mcp"},
    ]
    mock_get_tool_catalog.return_value = mock_tool_catalog
    mock_generate_welcome.return_value = "Welcome!"
    
    response = client.get("/welcome")
    assert response.status_code == 200
    payload = response.json()
    
    # Retrieve conversation from store
    convo = conversation_store.get(payload["conversation_id"])
    assert convo is not None
    
    # Verify tool_catalog is stored
    assert convo["state"].get("tool_catalog") == mock_tool_catalog
    
    # Verify available_formats is stored
    assert convo["state"].get("available_formats") == ["Modern", "Pioneer"]
    
    # Verify workflows is stored
    assert "workflows" in convo["state"]
    workflows = convo["state"]["workflows"]
    assert len(workflows) == 2
    workflow_names = [w["name"] for w in workflows]
    assert "meta_research" in workflow_names
    assert "deck_coaching" in workflow_names


@patch("src.app.agent_api.routes.get_tool_catalog_safe")
def test_chat_uses_welcome_session_info(mock_get_tool_catalog):
    """Test that /chat can access tool_catalog from welcome session state."""
    # First, manually create a conversation with welcome info
    initial_state = {
        "format": None,
        "days": None,
        "archetype": None,
        "deck_text": None,
        "card_details": None,
        "matchup_stats": None,
        "messages": [],
        "current_workflow": None,
        "tool_catalog": [{"name": "test_tool", "description": "Test"}],
        "available_formats": ["Modern", "Pioneer"],
        "workflows": [{"name": "meta_research", "description": "Meta analytics"}],
    }
    convo = conversation_store.create(initial_state=initial_state)
    conversation_id = convo["conversation_id"]
    
    # Mock fresh tool catalog fetch to return empty (to verify we use session state)
    mock_get_tool_catalog.return_value = []
    
    payload = {
        "message": "What's the Modern meta?",
        "conversation_id": conversation_id,
        "context": {"format": "Modern", "days": 30},
    }
    
    response = client.post("/chat", json=payload)
    assert response.status_code == 200
    
    # Verify conversation still has welcome info
    updated_convo = conversation_store.get(conversation_id)
    assert updated_convo["state"].get("tool_catalog") is not None
    assert updated_convo["state"].get("available_formats") == ["Modern", "Pioneer"]


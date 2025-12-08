"""Tests for Agent API FastAPI routes."""

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


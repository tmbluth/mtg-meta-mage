"""Tests for SSE formatting helpers."""

import json

from src.app.agent_api import streaming


def test_metadata_event_includes_conversation_and_state():
    payload = streaming.metadata_event("abc123", {"format": "Modern", "archetype": None})
    assert payload.startswith("event: metadata")
    data_line = payload.splitlines()[1]
    assert data_line.startswith("data: ")
    data = json.loads(data_line.replace("data: ", ""))
    assert data["conversation_id"] == "abc123"
    assert data["format"] == "Modern"


def test_content_and_done_events():
    content = streaming.content_event("hello")
    assert "event: content" in content
    assert "hello" in content

    done = streaming.done_event()
    assert done.strip() == "event: done\ndata: {}"


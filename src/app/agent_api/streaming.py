"""Helpers to format Server-Sent Events payloads."""

import json
from typing import Any, Dict, Optional

from .state import summarize_state_for_ui


def _event(event_type: str, payload: Dict[str, Any]) -> str:
    return f"event: {event_type}\ndata: {json.dumps(payload)}\n\n"


def metadata_event(conversation_id: str, state: Dict[str, Any], tool_catalog: Optional[list] = None) -> str:
    data = {
        "conversation_id": conversation_id,
        "format": state.get("format"),
        "archetype": state.get("archetype"),
    }
    if tool_catalog:
        data["tool_catalog"] = tool_catalog
    return _event("metadata", data)


def thinking_event(content: str) -> str:
    return _event("thinking", {"content": content})


def tool_call_event(tool: str, status: str, arguments: Optional[Dict[str, Any]] = None, summary: str = "") -> str:
    payload = {"tool": tool, "status": status}
    if arguments:
        payload["arguments"] = arguments
    if summary:
        payload["summary"] = summary
    return _event("tool_call", payload)


def content_event(text: str) -> str:
    return _event("content", {"text": text})


def state_event(state: Dict[str, Any]) -> str:
    return _event("state", summarize_state_for_ui(state))


def done_event() -> str:
    return _event("done", {})


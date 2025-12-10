"""Conversation state schema for the agent API."""

from typing import Annotated, Any, Dict, List, Literal, Optional, TypedDict

from langgraph.graph.message import add_messages


class ConversationMessage(TypedDict):
    role: Literal["user", "assistant"]
    content: str


class ConversationState(TypedDict, total=False):
    format: Optional[str]
    days: Optional[int]
    archetype: Optional[str]
    deck_text: Optional[str]
    card_details: Optional[list]
    matchup_stats: Optional[dict]
    # Use Annotated with add_messages reducer to properly append messages
    messages: Annotated[list, add_messages]
    current_workflow: Optional[str]
    # Session context from /welcome
    tool_catalog: Optional[List[Dict[str, Any]]]
    available_formats: Optional[List[str]]
    workflows: Optional[List[Dict[str, Any]]]


def create_initial_state() -> ConversationState:
    """Return a fresh, empty conversation state."""
    return {
        "format": None,
        "days": None,
        "archetype": None,
        "deck_text": None,
        "card_details": None,
        "matchup_stats": None,
        "messages": [],
        "current_workflow": None,
        "tool_catalog": None,
        "available_formats": None,
        "workflows": None,
    }


def summarize_state_for_ui(state: ConversationState) -> Dict[str, Any]:
    """Return a slim snapshot for SSE state events."""
    return {
        # has_deck is true if user provided deck_text OR we have enriched card_details
        "has_deck": bool(state.get("deck_text") or state.get("card_details")),
        "format": state.get("format"),
        "archetype": state.get("archetype"),
        "days": state.get("days"),
    }


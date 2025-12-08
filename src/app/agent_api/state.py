"""Conversation state schema for the agent API."""

from typing import Any, Dict, List, Literal, Optional, TypedDict


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
    messages: List[ConversationMessage]
    current_workflow: Optional[str]


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
    }


def summarize_state_for_ui(state: ConversationState) -> Dict[str, Any]:
    """Return a slim snapshot for SSE state events."""
    return {
        "has_deck": bool(state.get("card_details")),
        "format": state.get("format"),
        "archetype": state.get("archetype"),
        "days": state.get("days"),
    }


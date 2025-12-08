"""LangGraph workflow skeleton for Meta Mage agent."""

import json
import logging
import os
from typing import Literal

from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, StateGraph

from src.clients.llm_client import get_llm_client
from .state import ConversationState

logger = logging.getLogger(__name__)

WorkflowIntent = Literal["meta_research", "deck_coaching", "unknown"]

TOOL_OPTIONS = [
    {
        "name": "meta_research",
        "description": "Format-wide analytics: meta rankings, matchup spreads, archetype lists",
        "tools": [
            "get_format_meta_rankings",
            "get_format_matchup_stats",
            "get_format_archetypes",
        ],
    },
    {
        "name": "deck_coaching",
        "description": "User deck coaching: enrich deck, matchup stats, sideboard/mainboard optimization",
        "tools": [
            "get_enriched_deck",
            "get_deck_matchup_stats",
            "generate_deck_matchup_strategy",
            "optimize_mainboard",
            "optimize_sideboard",
        ],
    },
]

_tool_catalog_override = None

_intent_client = None


def set_tool_catalog(catalog):
    """Override tool catalog used for clarification prompts."""
    global _tool_catalog_override
    _tool_catalog_override = catalog


def _get_intent_client():
    global _intent_client
    if _intent_client is not None:
        return _intent_client

    model = os.getenv("LLM_MODEL")
    provider = os.getenv("LLM_PROVIDER")
    try:
        _intent_client = get_llm_client(model, provider)
    except Exception as exc:  # pragma: no cover - defensive
        logger.error("Failed to initialize intent LLM client: %s", exc)
        _intent_client = None
    return _intent_client


INTENT_PROMPT_TEMPLATE = """You are the routing brain for MTG Meta Mage.
Decide whether to route the user's latest message to:
- meta_research: tools about format-wide analytics (get_format_meta_rankings, get_format_matchup_stats, get_format_archetypes)
- deck_coaching: tools about a user's specific deck (get_enriched_deck, get_deck_matchup_stats, generate_deck_matchup_strategy, optimize_mainboard, optimize_sideboard)

Rules:
- Output ONLY a JSON object: {{"intent": "meta_research"}} or {{"intent": "deck_coaching"}}.
- Never add extra text or code fencing.
- If the user included deck text or asks for coaching/optimization for "my deck", choose deck_coaching.
- If they ask about the meta, top decks, format overview, or matchup spread without providing a specific deck, choose meta_research.

State:
- format: {format}
- days: {days}
- archetype: {archetype}
- deck_text_present: {has_deck_text}

Recent messages (latest last):
{recent_messages}"""


def _classify_with_llm(prompt: str) -> str:
    client = _get_intent_client()
    if not client:
        raise RuntimeError("Intent LLM client not available")
    return client.run(prompt).text


def _clarify_message() -> str:
    """Return a clarification message with tool options."""
    options = _tool_catalog_override or TOOL_OPTIONS
    lines = [
        "I couldn't tell if you want meta research or deck coaching.",
        "Please choose one of these options and provide any missing details:",
    ]
    for option in options:
        lines.append(f"- {option['name']}: {option['description']}")
        tools = option.get("tools")
        if tools:
            lines.append(f"  tools: {', '.join(tools)}")
    lines.append("Tell me which path to take (meta_research or deck_coaching).")
    return "\n".join(lines)


def classify_intent(message: str, state: ConversationState) -> WorkflowIntent:
    """Classify intent via LLM; fall back to meta_research on errors."""
    recent_messages = state.get("messages", [])[-3:]
    formatted_history = "\n".join(
        f"{m.get('role')}: {m.get('content')}" for m in recent_messages
    ) or "(none)"
    prompt = INTENT_PROMPT_TEMPLATE.format(
        format=state.get("format"),
        days=state.get("days"),
        archetype=state.get("archetype"),
        has_deck_text=bool(state.get("deck_text")),
        recent_messages=formatted_history,
    )
    try:
        raw = _classify_with_llm(prompt)
        parsed = json.loads(raw)
        intent = parsed.get("intent")
        if intent in ("meta_research", "deck_coaching"):
            return intent  # type: ignore[return-value]
        raise ValueError("intent missing or invalid")
    except Exception as exc:  # pragma: no cover - robust fallback
        logger.warning("Intent classification failed (%s); asking user to clarify", exc)
        return "unknown"


def enforce_blocking(state: ConversationState, intent: WorkflowIntent):
    """Apply blocking rules before tool execution."""
    if not state.get("format"):
        return False, "Format is required before proceeding."

    if intent == "meta_research" and state.get("days") is None:
        return False, "Please provide a days window for meta analysis."

    if intent == "deck_coaching":
        if not state.get("card_details"):
            return False, "Please provide your deck list so I can enrich it first."
        if state.get("card_details") and not state.get("archetype"):
            return False, "Select an archetype before continuing deck coaching."

    return True, None


def update_workflow(state: ConversationState, intent: WorkflowIntent) -> ConversationState:
    """Set current_workflow while preserving existing state."""
    state["current_workflow"] = intent
    return state


def _router_node(state: ConversationState) -> ConversationState:
    """Route to appropriate subgraph based on intent."""
    state.setdefault("messages", [])
    last_message = ""
    if state.get("messages"):
        last_message = state["messages"][-1]["content"]
    intent = classify_intent(last_message, state)
    if intent == "unknown":
        state["messages"].append(
            {
                "role": "assistant",
                "content": _clarify_message(),
            }
        )
        return state
    state = update_workflow(state, intent)
    allowed, reason = enforce_blocking(state, intent)
    if not allowed:
        # Record a gentle assistant message explaining the block
        state["messages"].append({"role": "assistant", "content": reason})
        return state
    # In a full implementation, we would branch into tool-calling nodes
    state["messages"].append({"role": "assistant", "content": f"Routed to {intent} workflow."})
    return state


def create_agent_graph() -> StateGraph:
    """Create a simple StateGraph with routing and checkpointing."""
    graph = StateGraph(dict)
    graph.add_node("router", _router_node)
    graph.set_entry_point("router")
    graph.add_edge("router", END)
    return graph.compile(checkpointer=MemorySaver())


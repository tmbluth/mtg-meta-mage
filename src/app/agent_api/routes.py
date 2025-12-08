"""FastAPI routes for the agent API."""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from src.etl.database.connection import DatabaseConnection
from src.app.mcp.tools.meta_research_tools import get_format_archetypes

from .graph import create_agent_graph
from .graph import set_tool_catalog
from .state import ConversationState, create_initial_state
from .store import InMemoryConversationStore
from .streaming import (
    content_event,
    done_event,
    metadata_event,
    state_event,
    thinking_event,
)
from .tool_catalog import get_tool_catalog_safe

router = APIRouter()
conversation_store = InMemoryConversationStore()
agent_graph = create_agent_graph()


@router.get("/welcome")
async def get_welcome():
    """
    Get welcome information including available formats, workflows, and tools.
    
    Use this endpoint before starting a conversation to understand system capabilities.
    """
    # Get formats
    query = """
        SELECT DISTINCT format 
        FROM tournaments 
        WHERE format NOT IN ('Commander', 'EDH')
    """
    with DatabaseConnection.get_cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
    formats = sorted([row[0] for row in rows if row and row[0]])
    
    # Get tool catalog dynamically from MCP server
    tool_catalog = await get_tool_catalog_safe()
    
    # Define workflows with example queries and tool mappings
    workflow_tool_mappings = {
        "meta_research": ["get_format_meta_rankings", "get_format_matchup_stats", "get_format_archetypes"],
        "deck_coaching": ["get_enriched_deck", "get_deck_matchup_stats", "generate_deck_matchup_strategy", "optimize_mainboard", "optimize_sideboard"]
    }
    
    workflows = [
        {
            "name": "meta_research",
            "description": "Format-wide analytics: meta rankings, matchup spreads, archetype lists",
            "example_queries": [
                "What are the top decks in Modern?",
                "Show me the Pioneer meta",
                "What's the matchup spread for Rakdos in Standard?"
            ]
        },
        {
            "name": "deck_coaching",
            "description": "Personalized coaching for your specific deck",
            "example_queries": [
                "How should I play against Tron? [provide deck]",
                "Optimize my sideboard [provide deck]",
                "What are my deck's bad matchups?"
            ]
        }
    ]
    
    # Enrich workflows with tool details from MCP catalog
    tool_lookup = {tool["name"]: tool for tool in tool_catalog}
    for workflow in workflows:
        workflow_name = workflow["name"]
        tool_names = workflow_tool_mappings.get(workflow_name, [])
        workflow["tool_details"] = [
            {"name": name, "description": tool_lookup[name]["description"]}
            for name in tool_names
            if name in tool_lookup
        ]
    
    return {
        "message": "Welcome to MTG Meta Mage! Here's what I can help you with:",
        "available_formats": formats,
        "workflows": workflows,
        "tool_count": len(tool_catalog)
    }


class ChatContext(BaseModel):
    format: Optional[str] = None
    archetype: Optional[str] = None
    days: Optional[int] = None
    deck_text: Optional[str] = None


class ChatRequest(BaseModel):
    message: str
    conversation_id: Optional[str] = None
    context: ChatContext = Field(default_factory=ChatContext)


@router.get("/formats")
async def list_formats():
    """Return available formats derived from tournaments table."""
    query = """
        SELECT DISTINCT format 
        FROM tournaments 
        WHERE format NOT IN ('Commander', 'EDH')
    """
    with DatabaseConnection.get_cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    formats = sorted([row[0] for row in rows if row and row[0]])
    return {"formats": formats}


@router.get("/archetypes")
async def list_archetypes(format: Optional[str] = Query(default=None)):
    if not format:
        raise HTTPException(status_code=400, detail="format is required")

    result = get_format_archetypes.fn(format=format, days=30)
    if result.get("archetypes") is None:
        raise HTTPException(status_code=404, detail="format not found")
    return result


@router.get("/conversations/{conversation_id}")
async def get_conversation(conversation_id: str):
    convo = conversation_store.get(conversation_id)
    if not convo:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return {
        "conversation_id": conversation_id,
        "state": {
            "format": convo["state"].get("format"),
            "archetype": convo["state"].get("archetype"),
            "days": convo["state"].get("days"),
            "has_deck": bool(convo["state"].get("card_details")),
        },
        "messages": convo["state"].get("messages", []),
    }


def _apply_context(state: ConversationState, context: ChatContext) -> ConversationState:
    if context.format:
        state["format"] = context.format
    if context.archetype:
        state["archetype"] = context.archetype
    if context.days is not None:
        state["days"] = context.days
    if context.deck_text:
        state["deck_text"] = context.deck_text
    return state


@router.post("/chat")
async def chat(request: ChatRequest):
    if not request.message or not request.message.strip():
        raise HTTPException(status_code=400, detail="message is required")

    tool_catalog = await get_tool_catalog_safe()
    if tool_catalog:
        set_tool_catalog(tool_catalog)

    if request.conversation_id and conversation_store.exists(request.conversation_id):
        convo = conversation_store.get(request.conversation_id)
    else:
        convo = conversation_store.create(conversation_id=request.conversation_id, initial_state=create_initial_state())

    state = convo["state"].copy()
    state = _apply_context(state, request.context)
    conversation_store.update(convo["conversation_id"], state_updates=state, messages=[{"role": "user", "content": request.message.strip()}])

    def event_stream():
        current = conversation_store.get(convo["conversation_id"])["state"]
        yield metadata_event(convo["conversation_id"], current, tool_catalog=tool_catalog)
        yield thinking_event("Routing your request...")
        updated_state = agent_graph.invoke(
            current, config={"configurable": {"thread_id": convo["conversation_id"]}}
        )
        conversation_store.update(convo["conversation_id"], state_updates=updated_state)
        yield content_event(updated_state["messages"][-1]["content"])
        yield state_event(updated_state)
        yield done_event()

    return StreamingResponse(event_stream(), media_type="text/event-stream")


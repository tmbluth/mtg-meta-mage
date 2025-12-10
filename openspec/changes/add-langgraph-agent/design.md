## Context

The MTG Meta Mage has MCP tools for meta research and deck coaching but lacks a conversational interface that can:
- Route between meta research (format-wide analytics) and deck coaching (personalized advice)
- Maintain state across multi-turn conversations
- Enforce dependencies between tool calls (e.g., can't optimize deck without enriched card data)
- Stream responses in real-time for better UX

This design introduces a LangGraph-based agent exposed via FastAPI that orchestrates the existing MCP tools.

## Goals / Non-Goals

### Goals
- Provide conversational chat interface at POST /chat with SSE streaming
- Distinguish between meta research and deck coaching workflows based on user intent
- Manage conversation state (format, days, archetype, card_details, matchup_stats) across turns
- Enforce blocking dependencies before tool execution
- Support workflow interleaving (meta → deck, deck → meta)
- Provide dropdown data endpoints for format and archetype selection

### Non-Goals
- Building a custom frontend (UI is separate concern)
- Persisting conversations to database (in-memory for MVP)
- Supporting multiple simultaneous conversations per user
- Real-time tournament data updates during conversation

## Decisions

### Decision 1: LangGraph for Workflow Orchestration
**Choice**: Use LangGraph `StateGraph` for workflow definition

**Why**: 
- Native support for conditional routing based on state
- Built-in checkpointing for conversation memory
- Compatible with existing LangChain/LangChain-based LLM client
- Cleaner than raw function chains for complex multi-step workflows

**Alternatives Considered**:
- Raw LangChain chains: Less flexible for conditional routing
- Custom state machine: More code, less tested
- CrewAI/AutoGen: Overkill for two-workflow system

### Decision 2: SSE for Streaming Responses
**Choice**: Use Server-Sent Events via `sse-starlette`

**Why**:
- Native browser support (no WebSocket complexity)
- Works with FastAPI's async patterns
- Allows incremental content delivery (thinking, tool calls, content)
- Client can render progress in real-time

**Alternatives Considered**:
- WebSockets: More complex bidirectional protocol not needed
- Long polling: Poor UX for streaming text
- HTTP/2 Server Push: Limited browser support

### Decision 3: In-Memory Conversation Store (MVP)
**Choice**: Store conversations in Python dict keyed by conversation_id

**Why**:
- Simplest implementation for MVP
- No database schema changes needed
- Sufficient for single-instance deployment
- Easy to replace with Redis/PostgreSQL later

**Alternatives Considered**:
- PostgreSQL: Adds migration complexity
- Redis: Adds infrastructure dependency
- File-based: Poor concurrency

### Decision 4: Intent Classification via LLM
**Choice**: Use LLM to classify user intent into meta_research vs deck_coaching

**Why**:
- More flexible than keyword matching
- Handles ambiguous queries naturally
- Can understand context from conversation history

**Alternatives Considered**:
- Keyword matching: Brittle, misses nuance
- Separate classifier model: Overhead not justified

### Decision 5: Blocking Dependencies Enforced in Agent Logic
**Choice**: Agent checks state and prompts for missing inputs before tool calls

**Why**:
- Clear error messages guide users
- Prevents wasted API calls
- State validation centralized in agent

**Blocking Rules**:
1. All tools require `format` - first interaction should establish format
2. `optimize_mainboard`, `optimize_sideboard`, `generate_deck_matchup_strategy` require `card_details` from prior `get_enriched_deck`
3. Deck coaching tools require `archetype` - prompt after deck is provided
4. `generate_deck_matchup_strategy` requires `matchup_stats` from prior `get_deck_matchup_stats`
5. Meta tools require `days` - user must specify

### Decision 6: MCP Client via langchain-mcp-adapters
**Choice**: Use `langchain-mcp-adapters` `MultiServerMCPClient` with `streamable_http` transport to call MCP tools.

**Why**: 
- Aligns with LangChain MCP quickstart guidance (MultiServerMCPClient; stateless by default)
- Matches existing MCP server transport (`streamable_http`)
- Keeps MCP client logic simple to swap servers or add auth headers later

**Notes**:
- Client is stateless by default; each tool call creates a fresh MCP session. If we need per-conversation reuse, we can adopt the stateful session pattern from the LangChain docs.
- Dependency already present in `pyproject.toml`; keep version ≥0.1.0 (bump if needed).

### Decision 7: LLM Interpretation of MCP Data
**Choice**: All MCP tool responses are processed through an LLM to generate natural language responses for users.

**Why**:
- Raw JSON/structured data from MCP tools is not user-friendly
- LLM can synthesize multiple data points into coherent narratives
- Enables contextual responses that consider conversation history
- Users expect conversational interactions, not API dumps

**Implementation**:
- After any MCP tool call completes, results are passed to the LLM with conversation context
- LLM generates natural language summary/analysis of the data
- Both /welcome and /chat endpoints return LLM-interpreted responses
- The LLM has access to tool_catalog from welcome info to explain capabilities naturally

### Decision 8: Session Initialization via /welcome
**Choice**: /welcome endpoint creates a new conversation session and stores welcome context for the entire session.

**Why**:
- Ensures every session starts with consistent tool/format awareness
- Welcome info (tool_catalog, available_formats, workflows) is expensive to fetch and doesn't change mid-session
- Enables LLM to reference available capabilities when routing or explaining options
- Provides natural onboarding experience for new users

**Flow**:
1. Client calls GET /welcome when user opens the app
2. Server creates new conversation, fetches MCP tool catalog, stores in session state
3. LLM generates personalized welcome message explaining capabilities
4. Response includes conversation_id for subsequent /chat calls
5. All /chat calls can reference stored welcome info without re-fetching

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                     FastAPI Application                       │
│  POST /chat ─────────────────────────────────────────────────│
│  GET /formats                                                 │
│  GET /archetypes?format=X                                     │
│  GET /conversations/{id}                                      │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────┐
│                    LangGraph Workflow                         │
│  ┌─────────────┐    ┌─────────────────┐    ┌──────────────┐  │
│  │   Router    │───►│  Meta Research  │    │ Deck Coaching│  │
│  │   (LLM)     │    │    Subgraph     │    │   Subgraph   │  │
│  └─────────────┘    └────────┬────────┘    └──────┬───────┘  │
│                              │                     │          │
│                              ▼                     ▼          │
│                    ┌─────────────────────────────────────┐   │
│                    │        MCP Tool Executor            │   │
│                    │  (get_format_meta_rankings, etc.)   │   │
│                    └─────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────┐
│                    Conversation Store                         │
│  { conversation_id: { state, messages, created_at } }        │
└──────────────────────────────────────────────────────────────┘
```

## State Schema

```python
class ConversationState(TypedDict):
    # User-provided inputs
    format: Optional[str]          # e.g., "Modern"
    days: Optional[int]            # e.g., 30
    archetype: Optional[str]       # e.g., "Burn"
    deck_text: Optional[str]       # Raw deck input
    
    # Derived/computed state
    card_details: Optional[list]   # From get_enriched_deck
    matchup_stats: Optional[dict]  # From get_deck_matchup_stats
    
    # Session context from /welcome
    tool_catalog: Optional[list]   # MCP tool definitions for LLM context
    available_formats: Optional[list]  # Available tournament formats
    workflows: Optional[list]      # Workflow definitions with examples
    
    # Conversation tracking
    messages: list                 # Chat history
    current_workflow: str          # "meta_research" | "deck_coaching" | None
```

## SSE Event Types

| Event Type | Payload | Purpose |
|------------|---------|---------|
| `metadata` | `{conversation_id, format, archetype}` | Session info at start |
| `thinking` | `{content: string}` | Agent reasoning (optional) |
| `tool_call` | `{tool, status, arguments, summary}` | Tool execution progress |
| `content` | `{text: string}` | Response text chunks |
| `state` | `{has_deck, format, archetype, days}` | State snapshot for UI |
| `done` | `{}` | Stream complete |

## API Endpoint Details

### GET /welcome
```json
// Response
{
  "conversation_id": "abc123",
  "message": "Welcome to MTG Meta Mage! I'm your AI-powered Magic: The Gathering coach...",
  "available_formats": ["Modern", "Pioneer", "Legacy", "Vintage", "Standard", "Pauper"],
  "workflows": [
    {
      "name": "meta_research",
      "description": "Format-wide analytics: meta rankings, matchup spreads, archetype lists",
      "example_queries": [
        "What are the top decks in Modern?",
        "Show me the Pioneer meta",
        "What's the matchup spread for Rakdos in Standard?"
      ],
      "tool_details": [
        {
          "name": "get_format_meta_rankings",
          "description": "Get format-wide meta rankings showing which archetypes are most played..."
        },
        {
          "name": "get_format_matchup_stats",
          "description": "Get format-wide head-to-head matchup statistics between archetypes..."
        },
        {
          "name": "get_format_archetypes",
          "description": "Get all archetypes competing in a format with their IDs, names, meta shares..."
        }
      ]
    },
    {
      "name": "deck_coaching",
      "description": "Personalized coaching for your specific deck",
      "example_queries": [
        "How should I play against Tron? [provide deck]",
        "Optimize my sideboard [provide deck]",
        "What are my deck's bad matchups?"
      ],
      "tool_details": [
        {
          "name": "get_enriched_deck",
          "description": "Parse a deck and enrich with card details from the database..."
        },
        {
          "name": "get_deck_matchup_stats",
          "description": "Get matchup statistics for a specific archetype vs the meta..."
        },
        {
          "name": "generate_deck_matchup_strategy",
          "description": "Generate AI-powered coaching for a specific matchup given a user's deck..."
        },
        {
          "name": "optimize_mainboard",
          "description": "Optimize a deck's mainboard by identifying flex spots and recommending replacements..."
        },
        {
          "name": "optimize_sideboard",
          "description": "Optimize a deck's sideboard to better answer the top N meta archetypes..."
        }
      ]
    }
  ],
  "tool_count": 8
}
```

**Purpose**: Session initialization endpoint that creates a new conversation and returns an LLM-generated welcome message. Dynamically retrieves tool catalog from MCP server and stores it in session state for use throughout the conversation.

**Flow**: 
1. Client calls GET /welcome when user opens the app
2. Server creates new conversation with unique ID
3. Server fetches MCP tool catalog and available formats
4. Server stores tool_catalog, available_formats, and workflows in conversation state
5. LLM generates personalized welcome message explaining capabilities in natural language
6. Response includes conversation_id for subsequent /chat calls
7. All subsequent /chat calls can reference stored welcome info without re-fetching

**Note**: The `message` field contains an LLM-interpreted natural language welcome, not static text. The LLM uses the tool_catalog and workflows to craft a contextual introduction.

### POST /chat
```json
// Request
{
  "message": "What's the Modern meta looking like?",
  "conversation_id": null,  // or existing ID
  "context": {
    "format": "Modern",     // From dropdown
    "archetype": null,
    "days": 30,
    "deck_text": null
  }
}

// Response: SSE stream
event: metadata
data: {"conversation_id": "abc123", "format": "Modern"}

event: thinking  
data: {"content": "Analyzing Modern meta rankings..."}

event: tool_call
data: {"tool": "get_format_meta_rankings", "status": "calling", "arguments": {"format": "Modern", "current_days": 30}}

event: tool_call
data: {"tool": "get_format_meta_rankings", "status": "complete", "summary": "Found 15 archetypes"}

event: content
data: {"text": "Based on the last 30 days of Modern data, here are the top archetypes:\n\n"}

event: content
data: {"text": "1. **Boros Energy** - 12.3% meta share, 54.2% win rate\n"}

event: state
data: {"has_deck": false, "format": "Modern", "archetype": null, "days": 30}

event: done
data: {}
```

### GET /formats
```json
// Response
{
  "formats": ["Modern", "Pioneer", "Legacy", "Vintage", "Standard", "Pauper"]
}
```

### GET /archetypes?format=Modern
```json
// Response
{
  "format": "Modern",
  "archetypes": [
    {"id": 1, "name": "Boros Energy", "meta_share": 12.3, "color_identity": "RW"},
    {"id": 2, "name": "Golgari Yawgmoth", "meta_share": 8.7, "color_identity": "BG"}
  ]
}
```

### GET /conversations/{conversation_id}
```json
// Response
{
  "conversation_id": "abc123",
  "state": {
    "format": "Modern",
    "archetype": "Burn",
    "days": 30,
    "has_deck": true
  },
  "messages": [
    {"role": "user", "content": "What's the Modern meta?"},
    {"role": "assistant", "content": "Based on..."}
  ]
}
```

## Workflow Routing Logic

```python
def route_intent(state: ConversationState, message: str) -> str:
    """
    Route to appropriate workflow based on message content.
    
    Meta Research signals:
    - "the meta", "top decks", "format overview"
    - Matchup spreads between archetypes generally
    - NO reference to "my deck" or deck text provided
    
    Deck Coaching signals:
    - "my deck", "my sideboard"
    - Deck list provided in message
    - Optimization or coaching requests
    """
    # LLM classification with examples
    ...
```

## Risks / Trade-offs

| Risk | Mitigation |
|------|------------|
| In-memory store loses conversations on restart | Document as MVP limitation; plan Redis migration |
| LLM routing might misclassify intent | Include confidence threshold; fallback to asking user |
| Long-running tool calls block SSE | Use async tool execution with status updates |
| State grows unbounded for long conversations | Implement message window (last N messages) |

## Migration Plan

1. Add new dependencies (langgraph, sse-starlette)
2. Implement `get_format_archetypes` MCP tool
3. Create agent module with state management
4. Add FastAPI routes
5. Write integration tests
6. Deploy alongside existing MCP server

No breaking changes to existing functionality.

## Open Questions

1. **Conversation TTL**: How long to keep conversations in memory: 3 hours
2. **Max message history**: How many messages to keep for context: 10
3. **Tool timeout**: How long to wait for tool execution: 60 seconds
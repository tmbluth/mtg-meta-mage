## Context
MTG Meta Mage has tournament data, card data, archetype classifications, and meta analytics. The next step is enabling conversational AI coaching—helping users understand their deck's strengths, weaknesses, and how to pilot it against the current meta. This requires:
1. A way to expose analytical tools to an LLM (MCP)
2. An agent framework for multi-step reasoning (LangGraph)
3. API endpoints for chat interactions

## Goals / Non-Goals

**Goals:**
- Enable users to submit a decklist and receive comprehensive coaching
- Leverage existing meta analytics and matchup data
- Create modular, discoverable tools via MCP protocol
- Support conversational follow-ups about specific matchups
- Extract shared utilities to avoid code duplication between ETL and agent services

**Non-Goals:**
- Building a full chat UI (future scope)
- Real-time deck modification suggestions (separate feature)
- Storing chat history/sessions (v1 is stateless per request)
- Supporting non-constructed formats

## Decisions

### Decision 1: Use FastMCP for MCP Server
**What:** Use the FastMCP Python library to create an MCP server exposing deck analysis tools.
**Why:** FastMCP provides a clean, decorator-based API for defining tools, resources, and prompts. It integrates well with Python and is actively maintained by Anthropic.
**Alternatives:**
- Raw MCP SDK: More verbose, lower-level control not needed
- Custom tool protocol: Would reinvent what MCP standardizes

### Decision 2: Use LangChain MCP Adapters for Tool Integration
**What:** Use `langchain-mcp-adapters` to convert MCP tools into LangChain/LangGraph-compatible tools.
**Why:** This is the [official LangChain package](https://github.com/langchain-ai/langchain-mcp-adapters) for MCP integration. It handles tool conversion, multiple server connections, and seamlessly integrates with the hundreds of existing MCP tool servers.
**Alternatives:**
- Custom MCP client: Would reinvent what langchain-mcp-adapters already provides
- Direct tool implementation: Loses modularity and MCP ecosystem benefits

### Decision 3: Use LangGraph StateGraph for Agent Orchestration
**What:** Use LangGraph's `StateGraph` with `ToolNode` and `tools_condition` to build the agent workflow.
**Why:** LangGraph provides graph-based workflows that are easier to debug and modify than chain-based approaches. The `langchain-mcp-adapters` package provides direct integration with LangGraph's `ToolNode`.
**Alternatives:**
- LangChain AgentExecutor: Less flexible for complex workflows
- Custom agent loop: More maintenance burden

### Decision 4: MCP Server as Business Logic Layer (No Service Layer)
**What:** Delete `meta_analysis.py` service. Move all MetaService logic into MCP tools. FastAPI routes call MCP directly via `MultiServerMCPClient`.
**Why:** 
- MCP is the source of truth—both REST API and agents use same capabilities
- Service layer would be trivial (just forwarding calls to MCP)
- Routes already handle HTTP concerns (validation, errors, responses)
- Simpler: fewer files, clearer data flow
- Future-proof: Easy to deploy MCP separately later
**Alternatives:**
- Keep thin service layer: Adds ceremony without value, extra indirection
- Both layers have logic: Duplication, unclear source of truth

### Decision 5: Extract Shared Utils to `src/core_utils.py`
**What:** Move `normalize_card_name`, `parse_decklist`, and `find_fuzzy_card_match` from `src/etl/utils.py` to a new `src/core_utils.py`. Rename remaining file to `src/etl/etl_utils.py`.
**Why:** These functions are needed by both ETL pipelines and MCP tools. Avoids circular imports and clarifies which utilities are ETL-specific vs. shared.
**Alternatives:**
- Keep in etl/utils.py: Would create dependency from app→etl, breaking layering
- Duplicate code: Violates DRY, maintenance burden

### Decision 6: Required User Inputs for Analysis
**What:** Users must provide: (1) time range for meta context, (2) decklist text, (3) archetype classification (from dropdown populated with format's archetypes + "other").
**Why:** Time range scopes the meta data. Decklist is the subject of analysis. Archetype selection enables matchup lookup without LLM classification step (faster, cheaper).
**Alternatives:**
- Auto-classify archetype: Adds LLM call, latency, cost
- No archetype: Can't provide matchup-specific advice

### Decision 7: MCP Tool Structure
**What:** Single `deck_analysis_tool` exposes multiple operations: `get_card_details`, `get_deck_synergies`, `get_meta_matchups`, `get_piloting_advice`.
**Why:** Groups related functionality logically. MCP server advertises capabilities that the agent selects from.
**Alternatives:**
- Separate tools per function: More granular but chattier MCP protocol
- Single monolithic analysis: Less flexible for partial queries

## Architecture (MCP-First Design)

```
┌─────────────────────────────────────────────────────────────────┐
│                    FastAPI Application                           │
│  Routes call MCP tools directly (no service layer)               │
├─────────────────────────────────────────────────────────────────┤
│  /api/v1/meta/archetypes  → MCP meta_analytics_tool             │
│  /api/v1/meta/matchups    → MCP meta_analytics_tool             │
│  /api/v1/agent/capabilities → MCP capabilities list             │
│  /api/v1/agent/analyze    → LangGraph agent → MCP tools         │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            │ (Both use langchain-mcp-adapters)
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              langchain-mcp-adapters (MultiServerMCPClient)       │
│  - REST routes: call_tool() directly                             │
│  - Agent: loads tools for StateGraph/ToolNode                    │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                  MCP Server (FastMCP) - Business Logic           │
├─────────────────────────────────────────────────────────────────┤
│  tools/meta_analytics_tool.py (@mcp.tool):                       │
│    ├─ get_archetype_rankings() → meta share, win rates          │
│    └─ get_matchup_matrix() → head-to-head matchups              │
│                                                                   │
│  tools/deck_analysis_tool.py (@mcp.tool):                        │
│    ├─ get_card_details(decklist) → enriched card data           │
│    ├─ get_meta_matchups(archetype, format) → matchup data       │
│    └─ get_piloting_advice(deck, matchups) → LLM coaching        │
│                                                                   │
│  prompts/coaching_prompt.py → LLM prompt template                │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Database & Core Utils                         │
│  - DatabaseConnection (PostgreSQL queries)                       │
│  - core_utils (parse_decklist, normalize_card_name)              │
└─────────────────────────────────────────────────────────────────┘
```

**Key Points:**
1. **MCP server is the source of truth** - All business logic lives in MCP tools
2. **No service layer** - FastAPI routes call MCP directly via `MultiServerMCPClient`
3. **Single codebase deployment** - Both servers in same codebase, easily separable later
4. **Protocol-first** - Everything exposed via standard MCP protocol

## Code Patterns

### Pattern 1: REST Routes Call MCP Directly

```python
# src/app/api/routes/meta_routes.py
from langchain_mcp_adapters.client import MultiServerMCPClient

# Module-level MCP client (initialized once)
_mcp_client = MultiServerMCPClient({
    "meta_analytics": {
        "url": "http://localhost:8000/mcp",
        "transport": "streamable_http",
    }
})

@router.get("/archetypes")
async def get_archetype_rankings(
    format: str = Query(...),
    current_days: int = Query(14),
    ...
) -> ArchetypeRankingsResponse:
    """Get archetype rankings with meta share and win rate."""
    result = await _mcp_client.call_tool(
        "meta_analytics",
        "get_archetype_rankings",
        arguments={"format": format, "current_days": current_days, ...}
    )
    
    if not result["data"]:
        raise HTTPException(status_code=404, detail="No data found")
    
    return ArchetypeRankingsResponse(**result)
```

### Pattern 2: MCP Tools Contain Business Logic

```python
# src/app/mcp/tools/meta_analytics_tool.py
from fastmcp import FastMCP
import polars as pl
from src.database.connection import DatabaseConnection

mcp = FastMCP("meta-analytics")

@mcp.tool()
def get_archetype_rankings(
    format: str,
    current_days: int = 14,
    previous_days: int = 14,
    ...
) -> dict:
    """Calculate archetype rankings with meta share and win rate."""
    # All the polars DataFrame logic from MetaService goes here
    # Query database, calculate meta share, win rates, etc.
    return {"data": [...], "metadata": {...}}
```

### Pattern 3: LangGraph Agent Uses MCP Tools

```python
# src/app/api/services/agent.py
from langchain_mcp_adapters.client import MultiServerMCPClient
from langgraph.graph import StateGraph, MessagesState, START
from langgraph.prebuilt import ToolNode, tools_condition

async def create_agent():
    client = MultiServerMCPClient({
        "deck_analysis": {
            "url": "http://localhost:8000/mcp",
            "transport": "streamable_http",
        }
    })
    tools = await client.get_tools()
    
    builder = StateGraph(MessagesState)
    builder.add_node(call_model)
    builder.add_node(ToolNode(tools))
    builder.add_conditional_edges("call_model", tools_condition)
    
    return builder.compile()
```

## Data Flow

### REST API Flow (Existing Endpoints)
1. Client calls `/api/v1/meta/archetypes?format=Modern`
2. Route calls `_mcp_client.call_tool("meta_analytics", "get_archetype_rankings", ...)`
3. MCP tool queries database, calculates metrics with polars
4. Returns JSON directly to route → client

### Agent Flow (New)
1. **Initialization**: Client calls `/api/v1/agent/capabilities` → lists MCP tools
2. **Analysis Request**: User submits decklist + time range + archetype to `/api/v1/agent/analyze`
3. **Agent Orchestration**: LangGraph StateGraph executes:
   - `get_card_details` tool → parses decklist, enriches from cards table
   - `get_meta_matchups` tool → calls meta_analytics tool for matchup matrix
   - `get_piloting_advice` tool → LLM analyzes deck + matchups, returns coaching
4. **Response**: Structured analysis returned to client

**Key Point**: Both flows use same MCP tools, ensuring consistent business logic.

## Risks / Trade-offs

| Risk | Mitigation |
|------|------------|
| MCP adds complexity | Start with single tool, expand incrementally |
| LLM costs for synergy/coaching | Cache common analyses, use smaller models for classification |
| Import refactoring breaks ETL | Update all import paths atomically, run full test suite |
| Agent hallucinations | Ground responses in DB data, include sources |

## Migration Plan

### Phase 1: Utility Refactoring (Breaking)
1. Create `src/core_utils.py` with shared functions
2. Rename `src/etl/utils.py` → `src/etl/etl_utils.py`
3. Update all ETL imports to use new paths
4. Run existing tests to verify no breakage

### Phase 2: MCP Server Setup
1. Create MCP server structure (`src/app/mcp/`)
2. Implement `meta_analytics_tool.py` - move all MetaService logic here
3. Implement `deck_analysis_tool.py` - new deck analysis logic
4. Start MCP server on port 8000

### Phase 3: Rewrite FastAPI Routes (Breaking)
1. Update `meta_routes.py` to call MCP directly (async)
2. Delete `src/app/api/services/meta_analysis.py`
3. Update existing tests for async routes
4. Delete `tests/unit/test_meta_service.py`

### Phase 4: Add Agent Capabilities
1. Implement LangGraph agent service
2. Add `agent_routes.py` with capabilities and analyze endpoints
3. Register new routes in `main.py`
4. Add integration tests

### Rollback Strategy
If MCP approach fails, MetaService logic is preserved in git history and can be restored. Utility refactoring is independent and low-risk.

## Open Questions

1. Should the MCP server run in-process with FastAPI or as a separate process?
   - **Proposed**: In-process for v1 simplicity, can extract later
2. How should we handle rate limiting for the coaching LLM calls?
   - **Proposed**: Rely on LLM client's built-in retry logic initially
3. Should archetype list for dropdown include all archetypes or filter by meta presence?
   - **Proposed**: Filter to archetypes with >1% meta share in time window for cleaner UX


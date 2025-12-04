## Context
MTG Meta Mage has tournament data, card data, archetype classifications, and meta analytics. The next step is enabling AI-powered deck coaching by exposing capabilities through standardized MCP tools. This requires:
1. A way to expose analytical tools to any MCP client or LLM (MCP server)
2. Modular, discoverable tools that can be composed by AI agents
3. Seamless integration with existing FastAPI routes

## Goals / Non-Goals

**Goals:**
- Expose deck analysis capabilities through standardized MCP tools
- Leverage existing meta analytics and matchup data
- Create modular, discoverable tools via MCP protocol
- Enable any MCP client to use these tools (Claude Desktop, custom agents, app API, etc.)
- Extract shared utilities to avoid code duplication between ETL and application services

**Non-Goals:**
- Building a specific agent implementation
- Creating a chat UI (out of scope)
- Real-time deck modification suggestions (separate feature)
- Storing analysis history/sessions (tools are stateless)
- Supporting non-constructed formats

## Decisions

### Decision 1: Use FastMCP for MCP Server
**What:** Use the FastMCP Python library to create an MCP server exposing deck analysis tools.
**Why:** FastMCP provides a clean, decorator-based API for defining tools, resources, and prompts. It integrates well with Python and is actively maintained by Anthropic.
**Alternatives:**
- Raw MCP SDK: More verbose, lower-level control not needed
- Custom tool protocol: Would reinvent what MCP standardizes

### Decision 2: Use LangChain MCP Adapters for FastAPI Integration
**What:** Use `langchain-mcp-adapters` `MultiServerMCPClient` to call MCP tools from FastAPI routes over HTTP.
**Why:** Provides clean async interface for calling MCP tools from Python. Handles connection management, error handling, and JSON serialization. Part of official LangChain ecosystem. HTTP-only approach simplifies code and prepares for future separation of MCP server into its own repository.
**Alternatives:**
- Raw MCP client: More verbose, lower-level than needed
- Custom wrapper: Would reinvent connection pooling and error handling
- Direct FastMCP calls: Would couple API routes to MCP server implementation, making future separation difficult

### Decision 3: MCP Server as Business Logic Layer (No Service Layer)
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

### Decision 4: Extract Shared Utils to `src/core_utils.py`
**What:** Move `normalize_card_name`, `parse_decklist`, and `find_fuzzy_card_match` from `src/etl/utils.py` to a new `src/core_utils.py`. Rename remaining file to `src/etl/etl_utils.py`.
**Why:** These functions are needed by both ETL pipelines and MCP tools. Avoids circular imports and clarifies which utilities are ETL-specific vs. shared.
**Alternatives:**
- Keep in etl/utils.py: Would create dependency from app→etl, breaking layering
- Duplicate code: Violates DRY, maintenance burden

### Decision 6: Move API Clients to Shared `src/clients/` Directory
**What:** Move `src/etl/api_clients/` (LLM, Scryfall, TopDeck clients) to `src/clients/` as shared infrastructure.
**Why:** Both ETL pipelines and MCP tools need these clients. Moving them to a shared location prevents app→etl dependency and prepares for future separation of MCP server. Clean dependency graph: both app and etl depend on clients, not each other.
**Alternatives:**
- Keep in etl/api_clients: Creates app→etl dependency, violates layering
- Duplicate clients: Violates DRY, maintenance burden

### Decision 5: MCP Tool Structure
**What:** Separate tool modules for distinct concerns: `meta_research_tools` (format-wide queries) and `deck_coaching_tools` (deck-specific analysis with operations: `parse_and_validate_decklist`, `get_deck_matchup_stats`, `generate_matchup_strategy`).
**Why:** Groups related functionality logically. MCP server advertises capabilities that clients can discover and compose.
**Alternatives:**
- Separate tools per function: More granular but chattier MCP protocol
- Single monolithic analysis: Less flexible for partial queries

## Architecture (MCP-First Design)

```
┌─────────────────────────────────────────────────────────────────┐
│                    FastAPI Application                           │
│  Routes call MCP tools directly (no service layer)               │
├─────────────────────────────────────────────────────────────────┤
│  /api/v1/meta/archetypes  → MCP meta_research_tools             │
│  /api/v1/meta/matchups    → MCP meta_research_tools             │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            │ (langchain-mcp-adapters)
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              langchain-mcp-adapters (MultiServerMCPClient)       │
│  - call_tool() interface for async MCP calls                     │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                  MCP Server (FastMCP) - Business Logic           │
├─────────────────────────────────────────────────────────────────┤
│  tools/meta_research_tools.py (@mcp.tool):                       │
│    ├─ get_format_meta_rankings() → meta share, win rates        │
│    └─ get_format_matchup_stats() → head-to-head matchups        │
│                                                                   │
│  tools/deck_coaching_tools.py (@mcp.tool):                       │
│    ├─ parse_and_validate_decklist(decklist) → enriched cards    │
│    ├─ get_deck_matchup_stats(archetype, format) → matchup data  │
│    └─ generate_matchup_strategy(deck, matchups) → LLM coaching  │
│                                                                   │
│  prompts/coaching_prompt.py → LLM prompt template                │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Database & Core Utils                         │
│  - DatabaseConnection (PostgreSQL queries)                       │
│  - core_utils (parse_decklist, normalize_card_name)              │
│  - clients/ (LLM, Scryfall, TopDeck API clients)                 │
└─────────────────────────────────────────────────────────────────┘

External MCP Clients (Claude Desktop, custom agents, etc.)
     │
     └─→ Can connect to MCP server and use tools directly
```

**Key Points:**
1. **MCP server is the source of truth** - All business logic lives in MCP tools
2. **No service layer** - FastAPI routes call MCP directly via `MultiServerMCPClient`
3. **Single codebase deployment** - Both servers in same codebase, easily separable later
4. **Protocol-first** - Everything exposed via standard MCP protocol
5. **Client-agnostic** - Any MCP client can use these tools (not just our FastAPI app)

## Code Patterns

### Pattern 1: REST Routes Call MCP via HTTP Client

```python
# src/app/api/services/mcp_client.py
from langchain_mcp_adapters.client import MultiServerMCPClient

_mcp_client: Optional[MultiServerMCPClient] = None
_tools_cache: Optional[dict] = None

def get_mcp_client() -> MultiServerMCPClient:
    """Get or create the shared MCP client instance."""
    global _mcp_client
    if _mcp_client is None:
        _mcp_client = MultiServerMCPClient({
            "meta_analytics": {
                "transport": "streamable_http",
                "url": os.getenv("MCP_SERVER_URL", "http://localhost:8000/mcp"),
            }
        })
    return _mcp_client

async def _get_tools() -> dict:
    """Get all MCP tools and cache them by name."""
    global _tools_cache
    if _tools_cache is None:
        client = get_mcp_client()
        tools = await client.get_tools(server_name="meta_analytics")
        if len(tools) == 0:
            # Try session-based discovery as fallback
            async with client.session("meta_analytics") as session:
                from langchain_mcp_adapters.tools import load_mcp_tools
                tools = await load_mcp_tools(session)
        _tools_cache = {tool.name: tool for tool in tools}
    return _tools_cache

async def call_mcp_tool(tool_name: str, arguments: dict) -> dict:
    """Call an MCP tool via the HTTP client."""
    tools = await _get_tools()
    tool = tools.get(tool_name)
    if tool is None:
        raise KeyError(f"Tool '{tool_name}' not found")
    return await tool.ainvoke(arguments)

# src/app/api/routes/meta_routes.py
from src.app.api.services.mcp_client import call_mcp_tool

@router.get("/archetypes")
async def get_archetype_rankings(
    format: str = Query(...),
    current_days: int = Query(14),
    ...
) -> ArchetypeRankingsResponse:
    """Get archetype rankings with meta share and win rate."""
    result = await call_mcp_tool(
        "get_format_meta_rankings",
        arguments={"format": format, "current_days": current_days, ...}
    )
    
    if not result["data"]:
        raise HTTPException(status_code=404, detail="No data found")
    
    return ArchetypeRankingsResponse(**result)
```

### Pattern 2: MCP Tools Contain Business Logic

```python
# src/app/mcp/tools/meta_research_tools.py
from fastmcp import FastMCP
import polars as pl
from src.database.connection import DatabaseConnection

mcp = FastMCP("meta-analytics")

@mcp.tool()
def get_format_meta_rankings(
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

### Pattern 3: MCP Tools Are Discoverable

```python
# Any MCP client can discover available tools
# Example: Claude Desktop, custom agent, etc.
{
  "tools": [
    {
      "name": "get_format_meta_rankings",
      "description": "Get format-wide meta rankings showing which archetypes are most played",
      "inputSchema": {...}
    },
    {
      "name": "get_format_matchup_stats",
      "description": "Get complete matchup statistics for ALL archetypes in a format",
      "inputSchema": {...}
    },
    {
      "name": "parse_and_validate_decklist",
      "description": "Parse a decklist and enrich with card details",
      "inputSchema": {...}
    }
  ]
}
```

## Data Flow

### REST API Flow
1. Client calls `/api/v1/meta/archetypes?format=Modern`
2. Route calls `_mcp_client.call_tool("meta_analytics", "get_format_meta_rankings", ...)`
3. MCP tool queries database, calculates metrics with polars
4. Returns JSON directly to route → client

### External MCP Client Flow (e.g., Claude Desktop)
1. **Discovery**: Client lists available MCP tools from server
2. **Tool Invocation**: User asks "What are the top Modern archetypes?"
3. **AI Agent**: Decides to call `get_format_meta_rankings` tool
4. **MCP Server**: Executes tool, queries database, returns data
5. **AI Agent**: Synthesizes natural language response for user

**Key Point**: Same tools serve both REST API and external clients.

## Risks / Trade-offs

| Risk | Mitigation |
|------|------------|
| MCP adds complexity | Start with simple tools, expand incrementally |
| LLM costs for coaching prompts | Tools are stateless, clients control LLM usage |
| Import refactoring breaks ETL | Update all import paths atomically, run full test suite |
| External clients misuse tools | Clear documentation, validate inputs in tools |

## Migration Plan

### Phase 1: Utility Refactoring (Breaking)
1. Create `src/core_utils.py` with shared functions
2. Rename `src/etl/utils.py` → `src/etl/etl_utils.py`
3. Update all ETL imports to use new paths
4. Run existing tests to verify no breakage

### Phase 2: MCP Server Setup
1. Create MCP server structure (`src/app/mcp/`)
2. Implement `meta_research_tools.py` - move all MetaService logic here
3. Implement `deck_coaching_tools.py` - new deck analysis logic
4. Start MCP server on port 8000

### Phase 3: Rewrite FastAPI Routes (Breaking)
1. Update `meta_routes.py` to call MCP directly (async)
2. Delete `src/app/api/services/meta_analysis.py`
3. Update existing tests for async routes
4. Delete `tests/unit/test_meta_service.py`

### Phase 4: Documentation & Client Examples
1. Document MCP tools in README
2. Provide example MCP client configurations
3. Add integration tests for MCP workflows

### Rollback Strategy
If MCP approach fails, MetaService logic is preserved in git history and can be restored. Utility refactoring is independent and low-risk.

## Open Questions

1. Should the MCP server run in-process with FastAPI or as a separate process?
   - **Resolved**: In-process for v1 simplicity, HTTP-based client prepares for future separation
2. How should we handle rate limiting for MCP tool calls?
   - **Proposed**: No rate limiting in tools, clients are responsible for managing their own usage
3. Should we expose raw database queries or only computed metrics?
   - **Proposed**: Only computed metrics (rankings, win rates)

## Post-Implementation Updates

### Refactoring: Clients Directory and MCP Client Simplification
After initial implementation, the following refactoring was completed:

1. **API Clients Moved to Shared Location**: `src/etl/api_clients/` → `src/clients/`
   - All external API clients (LLM, Scryfall, TopDeck) are now shared infrastructure
   - Updated imports in ETL pipelines and MCP tools to use `src.clients.*`
   - Clean dependency graph: both app and etl depend on clients, not each other

2. **MCP Client Simplified**: Removed FastMCP direct call fallback
   - Removed ~60 lines of fallback logic from `mcp_client.py`
   - HTTP-only approach via `MultiServerMCPClient` with session-based discovery fallback
   - Prepares for future separation of MCP server into its own repository
   - Simpler codebase, easier to debug, no in-process coupling


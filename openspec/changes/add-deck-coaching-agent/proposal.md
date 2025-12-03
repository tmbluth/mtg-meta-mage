# Change: Add Deck Coaching Agent with LangGraph and MCP

## Why
Enable AI-powered deck analysis and matchup coaching through a conversational interface. Users need guidance on deck synergies, weaknesses, meta positioning, and piloting strategies against top-tier decks. A LangGraph agent with MCP tools provides modular, discoverable capabilities that can be extended.

## What Changes
- **BREAKING**: Rename `src/etl/utils.py` → `src/etl/etl_utils.py` and extract shared functions (`normalize_card_name`, `parse_decklist`, `find_fuzzy_card_match`) to new `src/core_utils.py`
- **BREAKING**: Delete `src/app/api/services/meta_analysis.py` - business logic moves to MCP server
- **BREAKING**: Rewrite `src/app/api/routes/meta_routes.py` to call MCP tools directly (async)
- Add MCP server (`src/app/mcp/`) with FastMCP containing all business logic:
  - `meta_analytics_tool.py` - Meta share, win rates, matchup matrix (moved from MetaService)
  - `deck_analysis_tool.py` - Deck analysis, card details, piloting advice
- Add LangGraph agent service (`src/app/api/services/agent.py`) for orchestrating analysis workflows
- Add new API routes (`src/app/api/routes/agent_routes.py`) for agent interactions
- Add coaching prompt template (`src/app/mcp/prompts/coaching_prompt.py`)
- Add new dependencies: `langgraph`, `fastmcp`, `langchain-mcp-adapters`

## Impact
- Affected specs: `meta-analytics` (modified - routes call MCP directly), new spec: `deck-coaching-agent`
- Affected code:
  - **Deleted**: `src/app/api/services/meta_analysis.py`
  - **Major Rewrites**:
    - `src/app/api/routes/meta_routes.py` → async routes calling MCP directly
  - **Renamed/Refactored**:
    - `src/etl/utils.py` → `src/etl/etl_utils.py`
  - **Import Updates**:
    - `src/etl/archetype_pipeline.py`, `src/etl/cards_pipeline.py`, `src/etl/tournaments_pipeline.py`, `src/etl/__init__.py`
    - `tests/unit/test_etl_utils.py`, `tests/unit/test_meta_routes.py`, `tests/unit/test_meta_service.py` (delete)
  - **New Files**:
    - `src/app/mcp/tools/meta_analytics_tool.py` (MetaService logic moves here)
    - `src/app/mcp/tools/deck_analysis_tool.py`
  - **Configuration**:
    - `src/app/api/main.py` → register new routes
    - `pyproject.toml` → add dependencies


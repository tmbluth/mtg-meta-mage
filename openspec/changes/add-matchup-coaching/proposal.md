# Change: Add MCP Server Tools for Deck Coaching

## Why
Enable AI-powered deck analysis and matchup coaching by exposing meta analytics and deck analysis capabilities through standardized MCP tools. Provides modular, discoverable tools that can be used by any MCP client or LLM agent.

## What Changes
- **BREAKING**: Rename `src/etl/utils.py` → `src/etl/etl_utils.py` and extract shared functions (`normalize_card_name`, `parse_decklist`, `find_fuzzy_card_match`) to new `src/core_utils.py`
- **BREAKING**: Move `src/etl/api_clients/` → `src/clients/` (shared infrastructure for LLM, Scryfall, TopDeck clients)
- **BREAKING**: Delete `src/app/api/services/meta_analysis.py` - business logic moves to MCP server
- **BREAKING**: Rewrite `src/app/api/routes/meta_routes.py` to call MCP tools directly (async)
- Add MCP server (`src/app/mcp/`) with FastMCP containing all business logic:
  - `meta_research_tools.py` - Format-wide meta rankings and matchup statistics (moved from MetaService)
  - `deck_coaching_tools.py` - Deck-specific analysis, card parsing, and personalized coaching
- Add coaching prompt template (`src/app/mcp/prompts/coaching_prompt.py`)
- Simplify `src/app/api/services/mcp_client.py` - HTTP-only approach, removed FastMCP fallback
- Add new dependencies: `fastmcp`, `langchain-mcp-adapters`

## Impact
- Affected specs: `meta-analytics` (modified - routes call MCP directly), new capability: `matchup-coaching`
- Affected code:
  - **Deleted**: `src/app/api/services/meta_analysis.py`
  - **Major Rewrites**:
    - `src/app/api/routes/meta_routes.py` → async routes calling MCP directly
  - **Renamed/Refactored**:
    - `src/etl/utils.py` → `src/etl/etl_utils.py`
  - **Import Updates**:
    - `src/etl/archetype_pipeline.py`, `src/etl/cards_pipeline.py`, `src/etl/tournaments_pipeline.py`, `src/etl/__init__.py` (use `src.clients.*`)
    - `src/app/mcp/tools/deck_coaching_tools.py` (use `src.clients.llm_client`)
    - `tests/unit/test_etl_utils.py`, `tests/unit/test_meta_routes.py`, `tests/unit/test_meta_service.py` (delete)
  - **New Files**:
    - `src/app/mcp/tools/meta_research_tools.py` (MetaService logic moves here)
    - `src/app/mcp/tools/deck_coaching_tools.py`
  - **Configuration**:
    - `pyproject.toml` → add dependencies


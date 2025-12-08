## 1. Utility Refactoring (Breaking Change)
- [x] 1.1 Create `src/core_utils.py` and move `normalize_card_name`, `parse_decklist`, `find_fuzzy_card_match` from `src/etl/utils.py` to `src/core_utils.py`
- [x] 1.2 Rename `src/etl/utils.py` → `src/etl/etl_utils.py`
- [x] 1.3 Update `src/etl/etl_utils.py` to import shared functions from `src.core_utils`
- [x] 1.4 Update `src/etl/archetype_pipeline.py` imports to use `src.etl.etl_utils`
- [x] 1.5 Update `src/etl/cards_pipeline.py` imports to use `src.etl.etl_utils`
- [x] 1.6 Update `src/etl/tournaments_pipeline.py` imports to use `src.etl.etl_utils` and `src.core_utils`
- [x] 1.7 Update `src/etl/__init__.py` imports to use `src.core_utils`
- [x] 1.8 Update `tests/unit/test_etl_utils.py` imports and test paths
- [x] 1.9 Run existing tests to verify no breakage: `pytest tests/`

## 2. Dependencies
- [x] 2.1 Add `fastmcp` to `pyproject.toml` dependencies
- [x] 2.2 Add `langchain-mcp-adapters` to `pyproject.toml` dependencies 
- [x] 2.3 Run `uv sync` to install new dependencies

## 3. MCP Server Setup
- [x] 3.1 Create `src/app/mcp/__init__.py`
- [x] 3.2 Create `src/app/mcp/server.py` with FastMCP server initialization (port 8000, streamable_http)
- [x] 3.3 Create `src/app/mcp/tools/__init__.py`
- [x] 3.4 Create `src/app/mcp/resources/` directory (empty placeholder)
- [x] 3.5 Create `src/app/mcp/prompts/__init__.py`

## 4. Meta Research Tools (Move MetaService Logic)
- [x] 4.1 Create `src/app/mcp/tools/meta_research_tools.py` with FastMCP instance
- [x] 4.2 Implement `@mcp.tool() get_format_meta_rankings` - move all logic from `MetaService.get_archetype_rankings()`
- [x] 4.3 Implement `@mcp.tool() get_format_matchup_stats` - move all logic from `MetaService.get_matchup_matrix()`
- [x] 4.4 Move helper methods: `_fetch_archetype_data`, `_fetch_match_data`, `_calculate_meta_share`, `_calculate_win_rate`, etc.
- [x] 4.5 Ensure tool functions use DatabaseConnection and polars as before

## 5. Deck Coaching Tools
- [x] 5.1 Create `src/app/mcp/tools/deck_coaching_tools.py` with FastMCP instance
- [x] 5.2 Implement `@mcp.tool() parse_and_validate_decklist` operation (uses `parse_decklist`, queries DB)
- [x] 5.3 Implement `@mcp.tool() get_deck_matchup_stats` operation (calls meta_research_tools)
- [x] 5.4 Implement `@mcp.tool() generate_deck_matchup_strategy` operation (LLM call with coaching prompt)
- [x] 5.5 Register all tools with MCP server

## 6. Coaching Prompt
- [x] 6.1 Create `src/app/mcp/prompts/coaching_prompt.py` with prompt template
- [x] 6.2 Define placeholders for card_details, matchup_stats, archetype
- [x] 6.3 Include instructions for structured coaching output

## 7. Rewrite Meta Routes to Call MCP (Breaking Change)
- [x] 7.1 Update `src/app/api/routes/meta_routes.py` - call MCP tools directly
- [x] 7.2 Rewrite `get_archetype_rankings` to call MCP `get_format_meta_rankings` tool
- [x] 7.3 Rewrite `get_matchup_matrix` to call MCP `get_format_matchup_stats` tool
- [x] 7.4 Update error handling for MCP connection failures
- [x] 7.5 Delete `src/app/api/services/meta_analysis.py`
- [x] 7.6 Update imports in meta_routes.py (remove MetaService)

## 8. Testing
- [x] 8.1 Core utils tests already exist and pass
- [x] 8.2 Update `tests/unit/test_meta_routes.py` for MCP tool calls
- [x] 8.3 Delete `tests/unit/test_meta_service.py` (MetaService deleted)
- [x] 8.4 Add unit tests for meta_research_tools operations
- [x] 8.5 Add unit tests for deck_coaching_tools operations
- [x] 8.6 Add integration test for MCP tool workflows
- [x] 8.7 Create Postman collection for MCP server endpoints
- [x] 8.8 Add Postman requests for `get_format_meta_rankings` tool
- [x] 8.9 Add Postman requests for `get_format_matchup_stats` tool
- [x] 8.10 Add Postman requests for deck coaching tools
- [x] 8.11 Document MCP server URL and authentication in Postman collection

## 9. Validation
- [x] 9.1 MCP tools registered and available
- [x] 9.2 FastAPI server configured with MCP integration
- [x] 9.3 Verify `/health` endpoint works
- [x] 9.4 Test `/api/v1/meta/archetypes` calls MCP tools
- [x] 9.5 Test `/api/v1/meta/matchups` calls MCP tools
- [x] 9.6 Run full test suite: existing unit tests pass


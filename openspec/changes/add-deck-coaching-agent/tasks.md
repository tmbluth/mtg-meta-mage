## 1. Utility Refactoring (Breaking Change)
- [ ] 1.1 Create `src/core_utils.py` and move `normalize_card_name`, `parse_decklist`, `find_fuzzy_card_match` from `src/etl/utils.py` to `src/core_utils.py`
- [ ] 1.2 Rename `src/etl/utils.py` → `src/etl/etl_utils.py`
- [ ] 1.3 Update `src/etl/etl_utils.py` to import shared functions from `src.core_utils`
- [ ] 1.4 Update `src/etl/archetype_pipeline.py` imports to use `src.etl.etl_utils`
- [ ] 1.5 Update `src/etl/cards_pipeline.py` imports to use `src.etl.etl_utils`
- [ ] 1.6 Update `src/etl/tournaments_pipeline.py` imports to use `src.etl.etl_utils` and `src.core_utils`
- [ ] 1.7 Update `src/etl/__init__.py` imports to use `src.core_utils`
- [ ] 1.8 Update `tests/unit/test_etl_utils.py` imports and test paths
- [ ] 1.9 Run existing tests to verify no breakage: `pytest tests/`

## 2. Dependencies
- [ ] 2.1 Add `langgraph` to `pyproject.toml` dependencies
- [ ] 2.2 Add `fastmcp` to `pyproject.toml` dependencies
- [ ] 2.3 Add `langchain-mcp-adapters` to `pyproject.toml` dependencies 
- [ ] 2.4 Run `uv sync` to install new dependencies

## 3. MCP Server Setup
- [ ] 3.1 Create `src/app/mcp/__init__.py`
- [ ] 3.2 Create `src/app/mcp/server.py` with FastMCP server initialization (port 8000, streamable_http)
- [ ] 3.3 Create `src/app/mcp/tools/__init__.py`
- [ ] 3.4 Create `src/app/mcp/resources/` directory (empty placeholder)
- [ ] 3.5 Create `src/app/mcp/prompts/__init__.py`

## 4. Meta Analytics MCP Tool (Move MetaService Logic)
- [ ] 4.1 Create `src/app/mcp/tools/meta_analytics_tool.py` with FastMCP instance
- [ ] 4.2 Implement `@mcp.tool() get_archetype_rankings` - move all logic from `MetaService.get_archetype_rankings()`
- [ ] 4.3 Implement `@mcp.tool() get_matchup_matrix` - move all logic from `MetaService.get_matchup_matrix()`
- [ ] 4.4 Move helper methods: `_fetch_archetype_data`, `_fetch_match_data`, `_calculate_meta_share`, `_calculate_win_rate`, etc.
- [ ] 4.5 Ensure tool functions use DatabaseConnection and polars as before

## 5. Deck Analysis MCP Tool
- [ ] 5.1 Create `src/app/mcp/tools/deck_analysis_tool.py` with FastMCP instance
- [ ] 5.2 Implement `@mcp.tool() get_card_details` operation (uses `parse_decklist`, queries DB)
- [ ] 5.3 Implement `@mcp.tool() get_meta_matchups` operation (calls meta_analytics_tool)
- [ ] 5.4 Implement `@mcp.tool() get_piloting_advice` operation (LLM call with coaching prompt)
- [ ] 5.5 Register all tools with MCP server

## 6. Coaching Prompt
- [ ] 6.1 Create `src/app/mcp/prompts/coaching_prompt.py` with prompt template
- [ ] 6.2 Define placeholders for deck_cards, matchup_data, archetype
- [ ] 6.3 Include instructions for structured coaching output

## 7. Rewrite Meta Routes to Call MCP (Breaking Change)
- [ ] 7.1 Update `src/app/api/routes/meta_routes.py` - add module-level `MultiServerMCPClient`
- [ ] 7.2 Rewrite `get_archetype_rankings` to async, call MCP `get_archetype_rankings` tool
- [ ] 7.3 Rewrite `get_matchup_matrix` to async, call MCP `get_matchup_matrix` tool
- [ ] 7.4 Update error handling for MCP connection failures
- [ ] 7.5 Delete `src/app/api/services/meta_analysis.py`
- [ ] 7.6 Update imports in meta_routes.py (remove MetaService)

## 8. LangGraph Agent Service (using langchain-mcp-adapters)
- [ ] 8.1 Create `src/app/api/services/agent.py` with agent class
- [ ] 8.2 Implement `MultiServerMCPClient` setup with deck_analysis server config
- [ ] 8.3 Implement `StateGraph` with `MessagesState`, `ToolNode`, and `tools_condition`
- [ ] 8.4 Implement `create_agent()` function that builds and compiles the graph
- [ ] 8.5 Implement `analyze_deck()` method that invokes the graph
- [ ] 8.6 Implement error handling and partial response support

## 9. Agent API Routes
- [ ] 9.1 Create `src/app/api/routes/agent_routes.py` with router
- [ ] 9.2 Implement `GET /api/v1/agent/capabilities` endpoint
- [ ] 9.3 Implement `GET /api/v1/agent/archetypes` endpoint with format filter
- [ ] 9.4 Implement `POST /api/v1/agent/analyze` endpoint
- [ ] 9.5 Add Pydantic models for request/response in `src/app/api/models.py`
- [ ] 9.6 Register agent routes in `src/app/api/main.py`

## 10. Testing
- [ ] 10.1 Add unit tests for `src/core_utils.py` functions
- [ ] 10.2 Update `tests/unit/test_meta_routes.py` for async routes and MCP calls
- [ ] 10.3 Delete `tests/unit/test_meta_service.py` (MetaService no longer exists)
- [ ] 10.4 Add unit tests for meta_analytics_tool operations
- [ ] 10.5 Add unit tests for deck_analysis_tool operations
- [ ] 10.6 Add unit tests for agent service
- [ ] 10.7 Add unit tests for agent routes
- [ ] 10.8 Add integration test for full analysis workflow

## 11. Validation
- [ ] 11.1 Start MCP server on port 8000
- [ ] 11.2 Start FastAPI server on port 8001
- [ ] 11.3 Verify `/health` endpoint works
- [ ] 11.4 Test `/api/v1/meta/archetypes?format=Modern` calls MCP and returns data
- [ ] 11.5 Test `/api/v1/meta/matchups?format=Modern` calls MCP and returns matrix
- [ ] 11.6 Test `/api/v1/agent/capabilities` returns MCP feature list
- [ ] 11.7 Test `/api/v1/agent/archetypes?format=Modern` returns archetype dropdown
- [ ] 11.8 Test `/api/v1/agent/analyze` with sample decklist
- [ ] 11.9 Run full test suite: `pytest tests/`


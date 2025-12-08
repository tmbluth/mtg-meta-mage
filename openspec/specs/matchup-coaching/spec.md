# matchup-coaching Specification

## Purpose
TBD - created by archiving change add-matchup-coaching. Update Purpose after archive.
## Requirements
### Requirement: Shared Utility Functions Location
The system SHALL provide shared utility functions in `src/core_utils.py` that are used by both ETL pipelines and application services.

#### Scenario: Parse deck from core utils
- **WHEN** any module needs to parse a deck string into cards
- **THEN** it imports `parse_deck` from `src.core_utils`
- **AND** the function returns a list of dicts with keys: `quantity`, `card_name`, `section`

#### Scenario: Normalize card name from core utils
- **WHEN** any module needs to normalize a card name for matching
- **THEN** it imports `normalize_card_name` from `src.core_utils`
- **AND** the function handles unicode, quotes, dashes, whitespace, and escape sequences

#### Scenario: Fuzzy match card name from core utils
- **WHEN** any module needs to find a fuzzy match for a card name
- **THEN** it imports `find_fuzzy_card_match` from `src.core_utils`
- **AND** the function uses Levenshtein distance with configurable threshold

### Requirement: ETL-Specific Utilities Location
The system SHALL provide ETL-specific utility functions in `src/etl/etl_utils.py` that are only used by ETL pipelines.

#### Scenario: Get last load timestamp from etl utils
- **WHEN** an ETL pipeline needs to get the last load timestamp
- **THEN** it imports `get_last_load_timestamp` from `src.etl.etl_utils`
- **AND** the function queries `load_metadata` table for the specified data type

#### Scenario: Update load metadata from etl utils
- **WHEN** an ETL pipeline completes a load
- **THEN** it imports `update_load_metadata` from `src.etl.etl_utils`
- **AND** the function inserts a new row into `load_metadata`

### Requirement: MCP Server for All Business Logic
The system SHALL provide an MCP server using FastMCP that contains all business logic for meta analytics and deck analysis.

#### Scenario: List available tools
- **WHEN** a client requests the list of available MCP tools
- **THEN** the server returns tool definitions including `meta_research_tools` and `deck_coaching_tools`
- **AND** each tool includes name, description, and parameter schema

#### Scenario: MCP server runs as separate service
- **WHEN** the MCP server starts
- **THEN** it runs on port 8000 with streamable_http transport
- **AND** FastAPI application connects via `MultiServerMCPClient`
- **AND** both can run in same codebase but are independently deployable

### Requirement: Meta Research Tools
The system SHALL provide a `meta_research_tools` MCP tool module containing business logic for format-wide archetype rankings and matchup statistics (moved from MetaService).

#### Scenario: Calculate archetype rankings via MCP
- **WHEN** `get_format_meta_rankings` is called with format, current_days, previous_days parameters
- **THEN** the tool queries the database for tournament/archetype/match data
- **AND** calculates meta share using polars DataFrames
- **AND** calculates win rates with minimum match threshold
- **AND** returns structured data with current and previous period metrics
- **AND** supports optional filters (color_identity, strategy, group_by)

#### Scenario: Calculate matchup matrix via MCP
- **WHEN** `get_format_matchup_stats` is called with format and days parameters
- **THEN** the tool queries match data from database
- **AND** calculates head-to-head win rates using polars operations
- **AND** returns nested dictionary: {player_archetype: {opponent_archetype: {win_rate, match_count}}}
- **AND** returns null for matchups with < 3 matches

### Requirement: Deck Coaching Tool Operations
The system SHALL provide a `deck_coaching_tools` MCP tool module with operations for card parsing, deck-specific matchups, and personalized piloting advice.

#### Scenario: Get card details for deck
- **WHEN** `get_card_details` is called with a deck string
- **THEN** the tool parses the deck using `parse_deck`
- **AND** normalizes card names using `normalize_card_name`
- **AND** queries the `cards` table for each card
- **AND** returns card details: name, oracle_text, type_line, mana_cost, cmc, color_identity
- **AND** attempts fuzzy matching for cards not found exactly

#### Scenario: Identify deck synergies
- **WHEN** `get_deck_synergies` is called with enriched card data
- **THEN** the tool sends card list to LLM with synergy analysis prompt
- **AND** returns identified synergies: card combinations, strategy themes, win conditions

#### Scenario: Get meta matchups for archetype
- **WHEN** `get_meta_matchups` is called with archetype, format, and days parameters
- **THEN** the tool calls `MetaService.get_matchup_matrix`
- **AND** extracts the row for the specified archetype
- **AND** returns matchup win rates against top meta decks

#### Scenario: Get piloting advice for matchup
- **WHEN** `get_piloting_advice` is called with deck details and opponent archetype
- **THEN** the tool sends context to LLM with coaching prompt
- **AND** returns piloting guidance: mulligans, key cards, game plan phases

### Requirement: Coaching Prompt Template
The system SHALL provide a coaching prompt template at `src/app/mcp/prompts/coaching_prompt.py` for guiding LLM analysis.

#### Scenario: Load coaching prompt
- **WHEN** the agent needs to generate coaching advice
- **THEN** it loads the coaching prompt template
- **AND** the prompt instructs the LLM to analyze deck vs meta context
- **AND** the prompt includes placeholders for: card_details, synergies, matchup_stats, opponent_archetype

### Requirement: LangChain MCP Adapters for FastAPI Integration
The system SHALL use `langchain-mcp-adapters` `MultiServerMCPClient` to call MCP tools from FastAPI routes.

#### Scenario: Connect to MCP server via streamable HTTP
- **WHEN** the `MultiServerMCPClient` is configured
- **THEN** it uses `transport: "streamable_http"` for the meta_analytics server
- **AND** connects to the MCP server URL (e.g., `http://localhost:8000/mcp`)
- **AND** the client is initialized at module level in `src/app/api/services/mcp_client.py`
- **AND** the MCP server URL is configurable via `MCP_SERVER_URL` environment variable (default: `http://localhost:8000/mcp`)

#### Scenario: Call MCP tool from route
- **WHEN** a FastAPI route needs to call an MCP tool
- **THEN** it uses `await call_mcp_tool(tool_name, arguments={...})` from `mcp_client` module
- **AND** the `call_mcp_tool` function wraps `client.call_tool(server_name, tool_name, arguments={...})`
- **AND** the call returns the tool result as JSON
- **AND** the route handles errors appropriately
- **AND** the route is async (uses `async def`)

### Requirement: REST Routes Call MCP Directly
The system SHALL update existing meta analytics routes to call MCP tools directly via `MultiServerMCPClient` (no service layer).

#### Scenario: Archetype rankings route calls MCP
- **WHEN** GET `/api/v1/meta/archetypes` is requested
- **THEN** the route calls `await call_mcp_tool("get_format_meta_rankings", arguments={...})`
- **AND** the `call_mcp_tool` function uses the shared `MultiServerMCPClient` instance
- **AND** the route is async (uses `async def`)
- **AND** returns the MCP tool result directly as HTTP response

#### Scenario: Matchup matrix route calls MCP
- **WHEN** GET `/api/v1/meta/matchups` is requested
- **THEN** the route calls `await call_mcp_tool("get_format_matchup_stats", arguments={...})`
- **AND** returns the MCP tool result directly as HTTP response

#### Scenario: MCP client initialization
- **WHEN** the `mcp_client` module is imported
- **THEN** the `get_mcp_client()` function creates a `MultiServerMCPClient` instance lazily on first access
- **AND** the client is cached in a module-level variable and reused across all route invocations
- **AND** the client uses `streamable_http` transport to MCP server URL
- **AND** the client connects to server name "meta_analytics"

### Requirement: Dependencies Addition
The system SHALL add required dependencies to `pyproject.toml` for MCP support.

#### Scenario: Install new dependencies
- **WHEN** `uv sync` is run after the change
- **THEN** `fastmcp` package is installed (MCP server with decorator-based tools)
- **AND** `langchain-mcp-adapters` package is installed (MCP client for Python)
- **AND** existing functionality remains unaffected


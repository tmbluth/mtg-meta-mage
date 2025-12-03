## ADDED Requirements

### Requirement: Shared Utility Functions Location
The system SHALL provide shared utility functions in `src/core_utils.py` that are used by both ETL pipelines and application services.

#### Scenario: Parse decklist from core utils
- **WHEN** any module needs to parse a decklist string into cards
- **THEN** it imports `parse_decklist` from `src.core_utils`
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
- **THEN** the server returns tool definitions including `meta_analytics_tool` and `deck_analysis_tool`
- **AND** each tool includes name, description, and parameter schema

#### Scenario: MCP server runs as separate service
- **WHEN** the MCP server starts
- **THEN** it runs on port 8000 with streamable_http transport
- **AND** FastAPI application connects via `MultiServerMCPClient`
- **AND** both can run in same codebase but are independently deployable

### Requirement: Meta Analytics MCP Tool
The system SHALL provide a `meta_analytics_tool` MCP tool containing business logic for archetype rankings and matchup matrices (moved from MetaService).

#### Scenario: Calculate archetype rankings via MCP
- **WHEN** `get_archetype_rankings` is called with format, current_days, previous_days parameters
- **THEN** the tool queries the database for tournament/archetype/match data
- **AND** calculates meta share using polars DataFrames
- **AND** calculates win rates with minimum match threshold
- **AND** returns structured data with current and previous period metrics
- **AND** supports optional filters (color_identity, strategy, group_by)

#### Scenario: Calculate matchup matrix via MCP
- **WHEN** `get_matchup_matrix` is called with format and days parameters
- **THEN** the tool queries match data from database
- **AND** calculates head-to-head win rates using polars pivot operations
- **AND** returns nested dictionary: {player_archetype: {opponent_archetype: {win_rate, match_count}}}
- **AND** returns null for matchups with < 3 matches

### Requirement: Deck Analysis Tool Operations
The system SHALL provide a `deck_analysis_tool` MCP tool with operations for card details, synergies, matchups, and piloting advice.

#### Scenario: Get card details for decklist
- **WHEN** `get_card_details` is called with a decklist string
- **THEN** the tool parses the decklist using `parse_decklist`
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
- **AND** the prompt includes placeholders for: deck_cards, synergies, matchup_data, opponent_archetype

### Requirement: LangChain MCP Adapters Integration
The system SHALL use `langchain-mcp-adapters` to convert MCP tools into LangChain/LangGraph-compatible tools.

#### Scenario: Load MCP tools via MultiServerMCPClient
- **WHEN** the agent service initializes
- **THEN** it creates a `MultiServerMCPClient` with the deck analysis MCP server configuration
- **AND** calls `client.get_tools()` to load MCP tools as LangChain-compatible tools
- **AND** the tools are usable with LangGraph's `ToolNode`

#### Scenario: Connect to MCP server via streamable HTTP
- **WHEN** the `MultiServerMCPClient` is configured
- **THEN** it uses `transport: "streamable_http"` for the deck_analysis server
- **AND** connects to the MCP server URL (e.g., `http://localhost:8000/mcp`)

### Requirement: LangGraph Agent Service
The system SHALL provide a LangGraph-based agent service at `src/app/api/services/agent.py` using `StateGraph`, `ToolNode`, and `tools_condition`.

#### Scenario: Build StateGraph with tool routing
- **WHEN** the agent graph is constructed
- **THEN** it uses `StateGraph` with `MessagesState` for conversation state
- **AND** adds a `call_model` node that binds MCP tools to the LLM
- **AND** adds a `ToolNode` with the loaded MCP tools
- **AND** uses `tools_condition` to route between model and tool execution
- **AND** edges loop from tools back to call_model for multi-turn tool use

#### Scenario: Execute analysis workflow
- **WHEN** the agent receives an analysis request with decklist, format, days, and archetype
- **THEN** it invokes the compiled graph with the user message
- **AND** the graph orchestrates tool calls automatically based on LLM decisions
- **AND** returns a structured analysis response

#### Scenario: Handle tool errors gracefully
- **WHEN** an MCP tool call fails
- **THEN** the agent logs the error with context
- **AND** continues with available data
- **AND** indicates incomplete analysis in response

### Requirement: REST Routes Call MCP Directly
The system SHALL update existing meta analytics routes to call MCP tools directly via `MultiServerMCPClient` (no service layer).

#### Scenario: Archetype rankings route calls MCP
- **WHEN** GET `/api/v1/meta/archetypes` is requested
- **THEN** the route creates/reuses a `MultiServerMCPClient` instance
- **AND** calls `client.call_tool("meta_analytics", "get_archetype_rankings", arguments={...})`
- **AND** the route is async (uses `async def`)
- **AND** returns the MCP tool result directly as HTTP response

#### Scenario: Matchup matrix route calls MCP
- **WHEN** GET `/api/v1/meta/matchups` is requested
- **THEN** the route calls `client.call_tool("meta_analytics", "get_matchup_matrix", arguments={...})`
- **AND** returns the MCP tool result directly as HTTP response

#### Scenario: MCP client initialization
- **WHEN** the meta_routes module is imported
- **THEN** a module-level `MultiServerMCPClient` is created with meta_analytics server config
- **AND** the client is reused across all route invocations
- **AND** the client uses `streamable_http` transport to MCP server URL

### Requirement: Agent API Endpoints
The system SHALL provide REST API endpoints for agent interactions at `/api/v1/agent/`.

#### Scenario: Get agent capabilities
- **WHEN** GET `/api/v1/agent/capabilities` is requested
- **THEN** return JSON with available features in plain language
- **AND** include: deck_analysis, synergy_identification, meta_matchups, piloting_advice
- **AND** each feature includes description and required inputs

#### Scenario: Submit deck for analysis
- **WHEN** POST `/api/v1/agent/analyze` is requested with body:
  - `decklist` (string, required): Raw decklist text
  - `format` (string, required): Tournament format
  - `days` (integer, default 14): Time range for meta context
  - `archetype` (string, required): Selected archetype classification
- **THEN** validate all required fields are present
- **AND** execute the analysis workflow via agent service
- **AND** return structured analysis response

#### Scenario: Get archetypes for format dropdown
- **WHEN** GET `/api/v1/agent/archetypes?format={format}&days={days}` is requested
- **THEN** return list of archetypes for the format in the time window
- **AND** filter to archetypes with meta share > 1%
- **AND** include "other" option at end of list
- **AND** each archetype includes: main_title, color_identity, strategy, meta_share

#### Scenario: Validate analysis request
- **WHEN** POST `/api/v1/agent/analyze` is missing required fields
- **THEN** return 400 Bad Request
- **AND** response includes list of missing/invalid fields

#### Scenario: Handle invalid archetype
- **WHEN** POST `/api/v1/agent/analyze` has archetype not in format's list
- **THEN** accept the request (allows "other" or custom archetypes)
- **AND** matchup data returns empty/null for unknown archetypes
- **AND** analysis continues with deck-only insights

### Requirement: Analysis Response Structure
The system SHALL return deck analysis in a structured format with sections for different insights.

#### Scenario: Successful analysis response
- **WHEN** deck analysis completes successfully
- **THEN** return 200 OK with JSON body containing:
  - `deck_overview`: card count, color identity, mana curve summary
  - `synergies`: list of identified card synergies with descriptions
  - `weaknesses`: list of identified weaknesses with explanations
  - `meta_positioning`: win rates against top meta decks
  - `piloting_guides`: matchup-specific advice for top 5 meta decks
  - `metadata`: format, time_range, archetype, timestamp

#### Scenario: Partial analysis response
- **WHEN** some analysis steps fail (e.g., matchup data unavailable)
- **THEN** return 200 OK with available sections populated
- **AND** failed sections include null value and error message
- **AND** metadata includes `warnings` array listing issues

### Requirement: Dependencies Addition
The system SHALL add required dependencies to `pyproject.toml` for LangGraph and MCP support.

#### Scenario: Install new dependencies
- **WHEN** `uv sync` is run after the change
- **THEN** `langgraph` package is installed (agent workflow orchestration)
- **AND** `fastmcp` package is installed (MCP server with decorator-based tools)
- **AND** `langchain-mcp-adapters` package is installed (converts MCP tools to LangChain tools)
- **AND** existing functionality remains unaffected


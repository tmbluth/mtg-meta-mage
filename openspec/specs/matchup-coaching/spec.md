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

### Requirement: Mainboard Optimization
The system SHALL provide an MCP tool that optimizes a user's maindeck by identifying flex spots (non-critical cards) and recommending replacement cards that improve performance against the top N most frequent archetypes.

#### Scenario: Optimize mainboard against top 5 archetypes
- **WHEN** `optimize_mainboard` is called with card_details, archetype, format, and top_n=5
- **THEN** the tool calls `get_format_meta_rankings` to retrieve the top 5 archetypes by meta share
- **AND** for each of the top 5 archetypes, queries the database for up to 5 most recent decklists
- **AND** parses the decklist data to extract mainboard card lists
- **AND** sends the user's maindeck card list and sample decklists from top 5 archetypes to the LLM with mainboard optimization prompt
- **AND** returns identified flex spots (cards not crucial for deck consistency/synergy)
- **AND** returns recommended replacement cards with justifications
- **AND** justifications explain why those cards improve matchups against top N archetypes based on actual cards played
- **AND** justifications explain why other common alternatives were not recommended

#### Scenario: Handle empty or insufficient meta data
- **WHEN** `optimize_mainboard` is called but `get_format_meta_rankings` returns no archetypes
- **THEN** the tool returns an error message indicating insufficient meta data
- **AND** does not attempt LLM analysis

#### Scenario: Fetch recent decklists for top archetypes
- **WHEN** the tool retrieves top N archetypes
- **THEN** it queries the decklists table for each archetype_group_id
- **AND** orders by tournament start_date descending
- **AND** limits to 5 most recent decklists per archetype
- **AND** joins with deck_cards table to get card details for each decklist
- **AND** filters for mainboard cards only

#### Scenario: Mainboard optimization considers deck synergy
- **WHEN** the LLM analyzes a decklist for mainboard optimization
- **THEN** it identifies cards that are not core to the deck's strategy or synergy
- **AND** preserves cards that are essential for consistency (mana base, key combo pieces, core threats)
- **AND** considers the user's archetype when determining what is "core" vs "flex"

#### Scenario: Filter legal cards before LLM analysis
- **WHEN** the tool prepares to send data to the LLM
- **THEN** it normalizes the format name to match database storage format (lowercase)
- **AND** queries the cards table for all cards where legalities->>'format' = 'legal'
- **AND** further filters to commonly-played cards only (cards appearing in recent tournament decklists)
- **AND** further filters by color identity matching the user's deck
- **AND** includes colorless cards in the filtered list
- **AND** treats phyrexian mana symbols as colorless ({W/P} = 0 mana, so {W/P}{U} requires only U)
- **AND** only sends this filtered card list to the LLM in the prompt
- **AND** the tool returns an error if card legality data is unavailable

#### Scenario: Color identity filtering considers deck colors
- **WHEN** filtering cards by color identity
- **THEN** the tool analyzes the user's deck to determine its color identity
- **AND** includes cards where color_identity is a subset of the deck's colors
- **AND** always includes colorless cards (empty color_identity array)
- **AND** treats phyrexian mana symbols as zero-cost when evaluating castability
- **AND** a card with {W/P}{U} is considered functionally {U} (castable with only U sources)
- **AND** excludes cards requiring colors not present in the deck after stripping phyrexian costs

### Requirement: Sideboard Optimization
The system SHALL provide an MCP tool that optimizes a user's sideboard to better answer the top N most frequent archetypes, considering post-sideboard games.

#### Scenario: Optimize sideboard against top 10 archetypes
- **WHEN** `optimize_sideboard` is called with card_details, archetype, format, and top_n=10
- **THEN** the tool calls `get_format_meta_rankings` to retrieve the top 10 archetypes by meta share
- **AND** for each of the top 10 archetypes, queries the database for up to 5 most recent decklists
- **AND** parses the decklist data to extract mainboard and sideboard card lists
- **AND** sends the user's current sideboard and sample decklists from top 10 archetypes to the LLM with sideboard optimization prompt
- **AND** if the LLM response does not result in exactly 15 sideboard cards, retries with explicit 15-card requirement (up to 2 retries)
- **AND** returns recommended sideboard additions, removals, or replacements
- **AND** recommendations consider what opponents will sideboard in games 2 and 3 based on actual sideboard cards observed
- **AND** recommendations explain how each card answers specific threats from top N archetypes

#### Scenario: Sideboard optimization considers post-board opponent plans
- **WHEN** the LLM analyzes sideboard optimization
- **THEN** it considers what common sideboard cards opponents bring in against the user's archetype based on observed decklists
- **AND** analyzes actual sideboard cards from the fetched decklists
- **AND** recommends cards that answer those opposing sideboard plans
- **AND** explains how the sideboard plan changes for games 2 and 3 against each top N archetype

### Requirement: Card Legality and Color Identity Filtering
The system SHALL provide a function that filters the card pool to only legal, color-appropriate cards before sending to the LLM for optimization recommendations.

#### Scenario: Query legal cards for format
- **WHEN** the optimization tool needs to filter cards
- **THEN** it normalizes the format parameter to match the database format (lowercase)
- **AND** queries cards table: `SELECT * FROM cards WHERE legalities->>'normalized_format' = 'legal'`
- **AND** the legalities field is a JSONB column containing format:status mappings
- **AND** further filters to commonly-played cards by joining with deck_cards table
- **AND** returns only cards that appear in recent decklists (last 180 days)

#### Scenario: Filter by deck color identity
- **WHEN** the tool has a list of legal cards and the user's deck
- **THEN** it determines the deck's color identity from the cards in the deck
- **AND** filters to cards where card.color_identity is a subset of deck_colors
- **AND** includes all colorless cards (color_identity = [])
- **AND** includes cards with phyrexian mana symbols that can be paid with life
- **AND** includes cards with generic mana costs only
- **AND** returns this filtered list for LLM context

#### Scenario: Include alternative cost cards
- **WHEN** filtering by color identity
- **THEN** the tool strips phyrexian mana symbols from the mana_cost before evaluating color requirements
- **AND** a card with mana_cost "{W/P}{U}" becomes functionally "{U}" for filtering purposes
- **AND** includes cards with only generic mana costs ({1}, {2}, {X}, etc.) regardless of deck colors
- **AND** includes these colorless-castable cards in the filtered list

### Requirement: Optimization Prompt Templates
The system SHALL provide prompt templates for mainboard optimization and sideboard optimization that guide the LLM to produce actionable recommendations.

#### Scenario: Load mainboard optimization prompt
- **WHEN** `optimize_mainboard` tool formats the LLM prompt
- **THEN** the prompt template includes placeholders for: deck_details, archetype, top_n_archetype_decklists, legal_card_pool, format
- **AND** the top_n_archetype_decklists includes actual card lists from recent decklists
- **AND** the legal_card_pool includes only commonly-played format-legal cards matching the deck's color identity
- **AND** the prompt instructs the LLM to identify non-essential cards (flex spots)
- **AND** the prompt requests structured JSON output with flex spots and recommended replacements
- **AND** the prompt requests justifications for each recommendation based on observed cards in meta decklists
- **AND** the prompt instructs the LLM to only suggest cards from the provided legal_card_pool

#### Scenario: Load sideboard optimization prompt
- **WHEN** `optimize_sideboard` tool formats the LLM prompt
- **THEN** the prompt template includes placeholders for: deck_details, archetype, top_n_archetype_decklists, current_sideboard, legal_card_pool, format
- **AND** the top_n_archetype_decklists includes actual mainboard and sideboard card lists from recent decklists
- **AND** the legal_card_pool includes only commonly-played format-legal cards matching the deck's color identity
- **AND** the prompt instructs the LLM to optimize against top N archetypes based on observed cards
- **AND** the prompt requests consideration of opponent sideboard plans based on actual sideboard cards
- **AND** the prompt requests structured JSON output with additions, removals, and justifications
- **AND** the prompt explicitly requests exactly 15 cards total in the final sideboard
- **AND** the prompt instructs the LLM to only suggest cards from the provided legal_card_pool


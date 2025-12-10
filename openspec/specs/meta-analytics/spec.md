# meta-analytics Specification

## Purpose
TBD - created by archiving change add-meta-analytics. Update Purpose after archive.
## Requirements
### Requirement: Multi-Format Support
The system SHALL support meta analytics for all constructed, non-Commander formats stored in the database without hardcoding format names.

#### Scenario: Query archetypes for any valid format
- **WHEN** GET /api/v1/meta/archetypes?format={format_name} is requested
- **AND** format_name exists in the tournaments table (e.g., Standard, Modern, Pioneer, Legacy, Vintage, Pauper)
- **THEN** return archetype rankings for that format
- **AND** system does not restrict to predefined format list

#### Scenario: Query matchups for any valid format
- **WHEN** GET /api/v1/meta/matchups?format={format_name} is requested
- **AND** format_name exists in the tournaments table
- **THEN** return matchup matrix for that format

### Requirement: Archetype Ranking Endpoint
The system SHALL provide a REST API endpoint that returns ranked archetype performance data for a selected format with configurable time windows for current and previous periods.

#### Scenario: Get archetype rankings with default time windows
- **WHEN** GET /api/v1/meta/archetypes?format={format_name} is requested
- **THEN** return JSON with archetypes ranked by meta share for chosen format
- **AND** each archetype includes main_title, meta_share_current, meta_share_previous, win_rate_current, win_rate_previous
- **AND** current period defaults to last 14 days
- **AND** previous period defaults to 14 days back from end of current period (no overlap)

#### Scenario: Get archetype rankings for different formats
- **WHEN** GET /api/v1/meta/archetypes?format=Modern is requested
- **THEN** return archetype rankings for Modern format only
- **WHEN** GET /api/v1/meta/archetypes?format=Pioneer is requested
- **THEN** return archetype rankings for Pioneer format only
- **AND** results are isolated by format

#### Scenario: Get archetype rankings with custom time windows
- **WHEN** GET /api/v1/meta/archetypes?format=Modern&current_days=7&previous_days=7 is requested
- **THEN** return archetypes with current period as last 7 days
- **AND** previous period as 7 days back from end of current period
- **AND** time windows do not overlap (automatic)

#### Scenario: Filter archetype rankings by color identity
- **WHEN** GET /api/v1/meta/archetypes?format=Pioneer&color_identity=dimir is requested
- **THEN** return only archetypes with color_identity matching "dimir"

#### Scenario: Filter archetype rankings by strategy
- **WHEN** GET /api/v1/meta/archetypes?format=Modern&strategy=aggro is requested
- **THEN** return only archetypes with strategy matching "aggro"

#### Scenario: Request archetypes for invalid format
- **WHEN** GET /api/v1/meta/archetypes?format=InvalidFormat is requested
- **THEN** return 400 Bad Request
- **AND** response includes error message indicating invalid format

#### Scenario: Time windows automatically non-overlapping
- **WHEN** any valid values for current_days and previous_days are provided
- **THEN** system automatically calculates non-overlapping windows
- **AND** previous period ends where current period begins
- **AND** no validation errors for time window overlap

### Requirement: Archetype Grouping by Color and Strategy
The system SHALL provide breakdown of archetype rankings grouped by color_identity and strategy within the selected format.

#### Scenario: Get archetype rankings grouped by color identity
- **WHEN** GET /api/v1/meta/archetypes?format=Modern&group_by=color_identity is requested
- **THEN** return archetypes (win rate and meta share) grouped by color_identity
- **AND** each group includes aggregated meta_share and win_rate

#### Scenario: Get archetype rankings grouped by strategy
- **WHEN** GET /api/v1/meta/archetypes?format=Modern&group_by=strategy is requested
- **THEN** return archetypes (win rate and meta share) grouped by strategy
- **AND** each group shows distribution of aggro, midrange, control, ramp, and combo

### Requirement: Meta Share Calculation
The system SHALL calculate meta share as the percentage of decklists with the archetype in the specified time window relative to total decklists in that time window for the format.

#### Scenario: Calculate meta share for archetype in current period
- **WHEN** calculating meta share for any format for last 14 days
- **THEN** count decklists with archetype_group_id matching each archetype (e.g. "neoform", "burn", etc)
- **AND** divide by total decklists in that format in last 14 days
- **AND** return as percentage (0-100)

#### Scenario: Compare meta share between current and previous periods
- **WHEN** calculating meta share change for any archetype
- **THEN** calculate meta_share_current for last 14 days
- **AND** calculate meta_share_previous for 14 days back from end of current period
- **AND** return both values for comparison

### Requirement: Win Rate Calculation
The system SHALL calculate win rate as the percentage of matches won by decklists with the archetype in the specified time window.

#### Scenario: Calculate win rate for archetype in current period
- **WHEN** calculating win rate for archetype for last 14 days (e.g. Legacy's "death_and_taxes")
- **THEN** count matches where winner_id has decklist with archetype "death_and_taxes"
- **AND** divide by total matches involving "death_and_taxes" decklists
- **AND** return as percentage (0-100)

#### Scenario: Calculate win rate with insufficient match data
- **WHEN** calculating win rate for archetype with less than 3 matches
- **THEN** return null for win_rate
- **AND** include match_count field indicating insufficient data

#### Scenario: Compare win rate between current and previous periods
- **WHEN** calculating win rate change for archetype (e.g. dimir_frogtide)
- **THEN** calculate win_rate_current for last 14 days
- **AND** calculate win_rate_previous for 14 days back from end of current period
- **AND** return both values for comparison

### Requirement: Matchup Matrix Endpoint
The system SHALL provide a REST API endpoint that returns a matchup matrix showing head-to-head win rates between archetypes for a selected format.

#### Scenario: Get matchup matrix for a single format
- **WHEN** GET /api/v1/meta/matchups?format={format_name} is requested
- **THEN** return JSON with matrix structure
- **AND** rows represent archetype as player (deck playing)
- **AND** columns represent archetype as opponent
- **AND** cells contain win rate percentage for row archetype vs column archetype

#### Scenario: Get matchup matrix with custom time window
- **WHEN** GET /api/v1/meta/matchups?format=Pioneer&days=30 is requested
- **THEN** return matchup matrix based on matches from last 30 days

#### Scenario: Get matchup matrix with insufficient match data
- **WHEN** GET /api/v1/meta/matchups?format={format_name} is requested
- **AND** a specific matchup has less than 3 matches
- **THEN** return null for win_rate in that matchup cell
- **AND** include match_count in each matchup cell

#### Scenario: Request matchup matrix for format with no data
- **WHEN** GET /api/v1/meta/matchups?format={format_name} is requested
- **AND** no matches exist for format in time window
- **THEN** return 404 Not Found
- **AND** response includes message indicating no data available

### Requirement: Time Window Filtering
The system SHALL filter tournament data by start_date timestamp to determine current and previous time windows for analysis.

#### Scenario: Filter tournaments by current time window
- **WHEN** current_days parameter is set to 14
- **THEN** include only tournaments where start_date is within last 14 days
- **AND** calculate from current UTC timestamp

#### Scenario: Filter tournaments by previous time window
- **WHEN** previous_days is 14 and current_days is 14
- **THEN** include only tournaments where start_date is between 14-28 days ago
- **AND** ensure no overlap with current time window (automatic)

#### Scenario: Non-overlapping time windows by design
- **WHEN** any valid current_days and previous_days are provided
- **THEN** previous period automatically ends where current period begins
- **AND** no overlap validation needed (guaranteed by calculation logic)

### Requirement: API Response Format
The system SHALL return JSON responses with consistent schema including data, metadata, and error information.

#### Scenario: Successful archetype ranking response
- **WHEN** GET /api/v1/meta/archetypes?format={format_name} succeeds
- **THEN** return 200 OK
- **AND** response body includes "data" array with archetype objects
- **AND** response includes "metadata" with query parameters and timestamp

#### Scenario: Successful matchup matrix response
- **WHEN** GET /api/v1/meta/matchups?format={format_name} succeeds
- **THEN** return 200 OK
- **AND** response body includes "matrix" object with archetype keys
- **AND** response includes "archetypes" array listing all archetypes in matrix

#### Scenario: Error response format
- **WHEN** API request fails validation or processing
- **THEN** return appropriate HTTP status code (400, 404, 500)
- **AND** response body includes "error" object with "message" and "details"

### Requirement: FastAPI Application Setup
The system SHALL provide a FastAPI application with automatic OpenAPI documentation and CORS support for frontend integration.

#### Scenario: Start FastAPI server
- **WHEN** uvicorn is started with main:app
- **THEN** API server runs on configured host and port
- **AND** OpenAPI documentation is available at /docs
- **AND** CORS middleware allows frontend origin

#### Scenario: API health check
- **WHEN** GET /health is requested
- **THEN** return 200 OK
- **AND** response indicates API is running

### Requirement: Data Aggregation with polars
The system SHALL use polars DataFrames for efficient grouping, aggregation, and pivot operations on tournament data.

#### Scenario: Calculate meta share using polars groupby
- **WHEN** aggregating decklists by archetype for meta share calculation
- **THEN** load relevant data into polars DataFrame
- **AND** use groupby on archetype_group_id
- **AND** calculate count and percentage efficiently

#### Scenario: Generate matchup matrix using polars pivot
- **WHEN** creating matchup matrix from match results
- **THEN** load match data with player archetypes into DataFrame
- **AND** use pivot_table to create row/column matrix
- **AND** aggregate win rates for each matchup cell

### Requirement: Format Archetypes Listing Tool
The system SHALL provide a `get_format_archetypes` MCP tool that returns all archetypes for a given format with metadata suitable for dropdown display.

#### Scenario: Get archetypes for valid format
- **WHEN** `get_format_archetypes` is called with format="Modern"
- **THEN** return JSON with format and archetypes array
- **AND** each archetype includes id (archetype_group_id), name (main_title), meta_share, color_identity
- **AND** archetypes are sorted by meta_share descending

#### Scenario: Get archetypes with no data
- **WHEN** `get_format_archetypes` is called with format that has no archetypes
- **THEN** return empty archetypes array
- **AND** include format in response for confirmation

#### Scenario: Archetypes derived from tournaments
- **WHEN** `get_format_archetypes` is called
- **THEN** archetypes are queried from archetype_groups table
- **AND** meta_share is calculated from decklists in last 30 days
- **AND** only archetypes with at least one decklist are returned


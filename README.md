# MTG Meta Mage

An AI-powered tool for analyzing Magic: The Gathering decklists against the competitive meta. Users can submit a decklist and format to get insights via LLM analysis.

## Features

- Card data collection from Scryfall API with format legality tracking
- Tournament data collection from TopDeck.gg API
- LLM-powered archetype classification for decklists
- PostgreSQL database for storing tournament, player, decklist, match, cards, and archetype data
- Initial bulk load and incremental update capabilities for all data types
- MCP server with tools for meta research, deck coaching, and deck optimization
- REST API for meta analytics (archetype rankings, matchup matrices)

## Roadmap

- **Streamlit UI**: Meta analytics dashboard and chat interface

## Prerequisites

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) package manager (install with `curl -LsSf https://astral.sh/uv/install.sh | sh`)
- PostgreSQL 18+ database set up
- TopDeck.gg API key ([Get API Key](https://topdeck.gg/docs/tournaments-v2))
- LLM API credentials (optional, for archetype classification):
  - Azure OpenAI: `AZURE_OPENAI_API_KEY`, `AZURE_OPENAI_LLM_ENDPOINT`, `AZURE_OPENAI_API_VERSION`
  - OpenAI: TBD
  - Anthropic: TBD
  - AWS Bedrock: TBD

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd mtg-meta-mage
```

2. Install dependencies using uv:
```bash
uv sync
```

This will create a virtual environment and install all dependencies automatically.

3. Set up environment variables:
Create a `.env` file in the project root with the following variables:

```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=mtg-meta-mage-db
TEST_DB_NAME=mtg-meta-mage-db-test
DB_USER=your_db_user
DB_PASSWORD=your_db_password

# TopDeck API
TOPDECK_API_KEY=your_topdeck_api_key

# LLM Configuration (for archetype classification)
LLM_MODEL=gpt-4o-mini  # Model name/deployment name

# Azure OpenAI (if using Azure)
AZURE_OPENAI_API_KEY=your_azure_api_key
AZURE_OPENAI_LLM_ENDPOINT=https://your-resource.openai.azure.com/openai/deployments/{}/chat/completions?api-version={}
AZURE_OPENAI_API_VERSION=2024-02-15-preview
```

## Data

### Database Setup

Create main and test DB and initialize the database schema:
```bash
uv run python src/etl/database/init_db.py
```
This will create all necessary tables, indexes, and constraints.

### Database Schema

The database includes the following tables:

#### Card Data Tables
- **cards**: Scryfall oracle card data (card_id, name, oracle_text, rulings, type_line, mana_cost, cmc, color_identity, legalities, etc.)
  - Stores canonical card information from Scryfall's oracle cards bulk data
  - Includes format legalities as JSONB for efficient filtering (e.g., `legalities->>'modern' = 'legal'`)
  - **Must be loaded before tournaments** (see Data Loading Dependencies below)

#### Tournament Data Tables
- **tournaments**: Tournament metadata (ID, name, format, dates, location). **Commander and Limited formats are filtered out**
- **players**: Player performance data per tournament (depends on `tournaments`)
- **decklists**: Decklist text storage linked to players (depends on `players`)
- **match_rounds**: Round information for tournaments (depends on `tournaments`)
- **matches**: Individual match results (1v1 only) (depends on `match_rounds` and `players`)

#### Card + Tournament Table
- **deck_cards**: Junction table linking tournament decklists to individual cards
  - Stores parsed card entries from decklists with quantities and sections (mainboard/sideboard)
  - Depends on both `decklists` and `cards` tables
  - Cards are parsed from `decklist_text` using `parse_decklist()` utility function

#### Archetype Classification Tables
- **archetype_groups**: Stores unique archetype definitions (format, main_title, color_identity, strategy)
  - Database-enforced uniqueness via UNIQUE constraint
  - Referenced by both `decklists` and `archetype_classifications`
- **archetype_classifications**: Tracks historical classification events with metadata
  - Stores confidence scores, LLM model used, prompt version, and timestamp
  - Enables tracking of reclassification and confidence changes over time

#### Metadata Table
- **load_metadata**: Tracks last successful load timestamp for incremental updates
  - Stores metadata for tournament, card, and archetype loads separately
  - Uses `data_type` field to distinguish between 'tournaments', 'cards', and 'archetypes'
  - Uses `load_type` field to distinguish between 'initial' and 'incremental' loads

### Loading Data

**IMPORTANT**: Tables must be loaded in a specific order due to foreign key constraints:

1. **Cards must be loaded before tournaments** - The `deck_cards` table has a foreign key to `cards`. If cards aren't loaded first, decklist parsing will still run but cards won't be found and won't be stored in `deck_cards`, resulting in incomplete decklist data.

2. **Tournament data internal order** (handled automatically by the pipeline):
   - `tournaments` → `players` → `decklists` → `deck_cards` (requires cards to exist)
   - `tournaments` → `match_rounds` → `matches` (requires players to exist)

3. **Archetypes must be loaded after both cards and tournaments** - Archetype classification requires both `decklists` (from tournaments) and enriched card data from `deck_cards` (joined with `cards`).

**First-time setup order:**
1. Initialize database schema
2. Load cards (initial load)
3. Load tournaments (initial load)
4. Load archetypes (initial load - requires LLM API credentials)

#### Loading Card Data from Scryfall

Before loading tournaments, you should load card data from Scryfall. This populates the `cards` table with oracle card information needed for decklist parsing.

```bash
# Initial load (full refresh)
uv run python src/etl/main.py --data-type cards --mode initial

# Incremental load skips existing cards
uv run python src/etl/main.py --data-type cards --mode incremental

# Load with custom batch size
uv run python src/etl/main.py --data-type cards --mode initial --batch-size 500
```

#### Loading Tournament Data

The incremental load automatically tracks the last loaded tournament timestamp and only fetches new data. When tournaments are loaded, decklists are automatically parsed and linked to cards in the `deck_cards` table.

The TopDeck API has a rate limit of 200 requests per minute. The client automatically enforces this limit with a 300ms delay between requests and includes retry logic for rate limit errors.

```bash
# Initial load overwrites any previous entry
uv run python src/etl/main.py --data-type tournaments --mode initial --days 180

# "Incremental" only loads data since last load
uv run python src/etl/main.py --data-type tournaments --mode incremental
```

#### Loading Archetype Classifications

Archetype classification uses LLMs to automatically categorize decklists by analyzing mainboard cards. This requires both card and tournament data to be loaded first, and requires LLM API credentials.

**Key Features:**
- Analyzes mainboard cards (name, oracle text, type, mana cost, CMC, color identity)
- Classifies into archetype groups (format, main_title, color_identity, strategy)
- Provides confidence scores (0.0-1.0) for classification quality
- Tracks historical classifications (model, prompt version, timestamp)

**Strategy Types:** aggro, midrange, control, ramp, combo

```bash
# Initial load - classify all unclassified decklists
uv run python src/etl/main.py --data-type archetypes --mode initial --model-provider azure_openai --prompt-id archetype_classification_v1

# Incremental load - classify decklists from tournaments since last archetype load
uv run python src/etl/main.py --data-type archetypes --mode incremental --model-provider azure_openai --prompt-id archetype_classification_v1
```

**Example Classification Output:**
```
main_title: "amulet_titan"
color_identity: "gruul"
strategy: "combo"
confidence: 0.95
```

**Reclassification Strategy:**
- Update `LLM_MODEL` environment variable to use a different model
- Modify prompt template in `src/etl/prompts/archetype_classification_v{?}.txt` (or create new version)
- Run `--mode initial` to reclassify existing decklists with new model/prompt
- Historical classifications are preserved in `archetype_classifications` table
- Each decklist's `archetype_group_id` is updated to the latest classification

## MCP Server & API

The application exposes capabilities through both an MCP server (for AI agents) and a REST API (for direct access). Both use the same underlying MCP tools, ensuring consistency.

### MCP Server

The MCP server runs alongside the FastAPI application and provides discoverable tools for meta research and deck coaching:

**Meta Research Tools:**
- `get_format_meta_rankings` - Archetype rankings with meta share and win rates
- `get_format_matchup_stats` - Head-to-head matchup matrix for all archetypes

**Deck Coaching Tools:**
- `parse_and_validate_decklist` - Parse decklist and enrich with card details
- `get_deck_matchup_stats` - Get matchup statistics for a specific archetype
- `generate_matchup_strategy` - AI-powered coaching for specific matchups

**Deck Optimization Tools:**
- `optimize_mainboard` - Identify flex spots and recommend replacements based on top meta archetypes
- `optimize_sideboard` - Suggest sideboard changes to answer the most frequent meta matchups

The MCP server is accessible at `http://localhost:8000/mcp` and can be used by any MCP client (Claude Desktop, custom agents, etc.).

### Meta Analytics API

The REST API provides endpoints for querying archetype performance and matchup data across all constructed formats.

### Starting the API Server

Start the FastAPI server using uvicorn:

```bash
# Development mode with auto-reload
uv run uvicorn src.app.api.main:app --reload --host 0.0.0.0 --port 8000

# Production mode
uv run uvicorn src.app.api.main:app --host 0.0.0.0 --port 8000
```

The API will be available at `http://localhost:8000`. OpenAPI documentation is available at:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

### API Endpoints

#### GET /health

Health check endpoint to verify API is running.

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2025-11-24T12:00:00Z"
}
```

#### GET /api/v1/meta/archetypes

Get archetype rankings with meta share and win rates for a format.

**Query Parameters:**
- `format` (required): Tournament format (e.g., "Modern", "Pioneer", "Standard")
- `current_days` (optional, default: 14): Number of days back from today for current period
- `previous_days` (optional, default: 14): Number of days back from end of current period for previous period
- `color_identity` (optional): Filter by color identity (e.g., "dimir", "jeskai")
- `strategy` (optional): Filter by strategy ("aggro", "midrange", "control", "ramp", "combo")
- `group_by` (optional): Group results by "color_identity" or "strategy"

**Example Request:**
```bash
curl "http://localhost:8000/api/v1/meta/archetypes?format=Modern"
```

**Example Response:**
```json
{
  "data": [
    {
      "main_title": "rakdos_midrange",
      "color_identity": "rakdos",
      "strategy": "midrange",
      "meta_share_current": 18.2,
      "meta_share_previous": 16.5,
      "win_rate_current": 54.7,
      "win_rate_previous": 52.9,
      "sample_size_current": 30,
      "sample_size_previous": 26,
      "match_count_current": 120,
      "match_count_previous": 95
    }
  ],
  "metadata": {
    "format": "Pioneer",
    "current_period": {
      "days": 14,
      "start_date": "2025-11-10T00:00:00Z",
      "end_date": "2025-11-24T00:00:00Z"
    },
    "previous_period": {
      "days": 14,
      "start_date": "2025-10-27T00:00:00Z",
      "end_date": "2025-11-10T00:00:00Z"
    },
    "timestamp": "2025-11-24T12:00:00Z"
  }
}
```

**Filtering Examples:**
```bash
# Filter by color identity
curl "http://localhost:8000/api/v1/meta/archetypes?format=Modern&color_identity=red"

# Filter by strategy
curl "http://localhost:8000/api/v1/meta/archetypes?format=Pioneer&strategy=aggro"

# Group by color identity
curl "http://localhost:8000/api/v1/meta/archetypes?format=Modern&group_by=color_identity"

# Custom time windows (last 7 days vs 14 days before that)
curl "http://localhost:8000/api/v1/meta/archetypes?format=Modern&current_days=7&previous_days=14"
```

#### GET /api/v1/meta/matchups

Get matchup matrix showing head-to-head win rates between archetypes.

**Query Parameters:**
- `format` (required): Tournament format (e.g., "Modern", "Pioneer", "Standard")
- `days` (optional, default: 14): Number of days to include in analysis

**Example Request:**
```bash
curl "http://localhost:8000/api/v1/meta/matchups?format=Modern"
```

**Example Response:**
```json
{
  "matrix": {
    "delver": {
      "mystic_forge": {
        "win_rate": 48.0,
        "match_count": 25
      },
      "reanimator": {
        "win_rate": 53.0,
        "match_count": 18
      }
    },
    "mystic_forge": {
      "delver": {
        "win_rate": 52.0,
        "match_count": 25
      },
      "reanimator": {
        "win_rate": 46.5,
        "match_count": 20
      }
    },
    "reanimator": {
      "delver": {
        "win_rate": 47.0,
        "match_count": 18
      },
      "mystic_forge": {
        "win_rate": 53.5,
        "match_count": 20
      }
    }
  },
  "archetypes": ["delver", "mystic_forge", "reanimator"],
  "metadata": {
    "format": "Legacy",
    "days": 14,
    "start_date": "2025-11-10T00:00:00Z",
    "timestamp": "2025-11-24T12:00:00Z"
  }
}
```

**Note:** Win rates may be `null` for matchups with fewer than 3 matches (insufficient data).

**Custom Time Window Example:**
```bash
# Last 30 days
curl "http://localhost:8000/api/v1/meta/matchups?format=Pioneer&days=30"
```

### Deck Optimization MCP Tools

The deck optimization tools are available through the MCP server and can be used by any MCP client (Claude Desktop, custom agents, etc.). These tools analyze your deck against the current meta and provide AI-powered recommendations.

**optimize_mainboard** - Identifies flex spots in your mainboard and recommends replacements based on top meta archetypes:
- Analyzes deck against top N meta archetypes (default: 5)
- Filters recommendations to format-legal cards matching your deck's color identity
- Uses actual meta decklists (up to 5 per archetype) to inform suggestions
- Provides detailed justifications for each recommendation

**optimize_sideboard** - Suggests sideboard changes to answer the most frequent meta matchups:
- Recommends additions/removals to improve matchup coverage
- Generates matchup-specific sideboard plans (what to bring in/out)
- Enforces 15-card sideboard constraint with retry logic
- Uses observed opponent sideboard strategies from meta decklists

**Usage via MCP Client:**
```python
# Example: Optimize mainboard
result = await client.call_tool(
    "meta_analytics",
    "optimize_mainboard",
    arguments={
        "card_details": parsed_deck["card_details"],
        "archetype": "Murktide",
        "format": "Modern",
        "top_n": 5
    }
)

# Example: Optimize sideboard
result = await client.call_tool(
    "meta_analytics",
    "optimize_sideboard",
    arguments={
        "card_details": parsed_deck["card_details"],
        "archetype": "Murktide",
        "format": "Modern",
        "top_n": 5
    }
)
```

For detailed documentation, examples, and workflow guides, see [tests/postman/mcp_server/README.md](tests/postman/mcp_server/README.md).

## API Attribution

This project uses data from the TopDeck.gg API. Per their usage requirements, any project using this data must include proper attribution:

```html
<p>Data provided by <a href="https://topdeck.gg" target="_blank">TopDeck.gg</a></p>
```

## Testing

Run the test suite using uv:

```bash
uv run pytest tests/
```

Or run individual test files:

```bash
uv run pytest tests/test_your_file.py
```

## Project Structure

```
mtg-meta-mage/
├── src/
│   ├── clients/              # External API clients (LLM, Scryfall, TopDeck)
│   ├── app/
│   │   ├── api/              # FastAPI routes (call MCP tools)
│   │   └── mcp/              # MCP server (business logic)
│   │       ├── tools/         # MCP tools (meta_research, deck_coaching)
│   │       └── prompts/       # LLM prompt templates
│   ├── etl/                  # ETL pipelines
│   │   ├── database/         # Database connection & schema
│   │   └── prompts/          # ETL prompt templates
│   └── core_utils.py         # Shared utilities (parse_decklist, etc.)
├── tests/
│   ├── unit/
│   ├── integration/
│   └── postman/              # Postman collections for all endpoints
├── pyproject.toml
├── AGENTS.md
└── README.md
```


## License

[Add your license here]

## Contributing

[Add contribution guidelines here]


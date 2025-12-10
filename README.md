# MTG Meta Mage

An AI-powered tool for analyzing Magic: The Gathering decks against the competitive meta. Users can submit a deck and format to get insights via LLM analysis.

## Features

- **Data Collection**: Card data from Scryfall API, tournament data from TopDeck.gg API
- **AI Classification**: LLM-powered archetype classification for decks
- **Database**: PostgreSQL for storing tournament, player, deck, match, cards, and archetype data
- **ETL Pipelines**: Initial bulk load and incremental update capabilities for all data types
- **Agent API**: LangGraph-powered conversational interface with intent routing, state management, and SSE streaming
- **MCP Server**: Discoverable tools for meta research, deck coaching, and deck optimization

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
LARGE_LANGUAGE_MODEL=gpt-4o-mini  # Model name/deployment name

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
- **decklists**: Deck list text storage linked to players (depends on `players`)
- **match_rounds**: Round information for tournaments (depends on `tournaments`)
- **matches**: Individual match results (1v1 only) (depends on `match_rounds` and `players`)

#### Card + Tournament Table
- **deck_cards**: Junction table linking tournament decks to individual cards
  - Stores parsed card entries from decks with quantities and sections (mainboard/sideboard)
  - Depends on both `decklists` and `cards` tables
  - Cards are parsed from `decklist_text` using `parse_deck()` utility function

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

1. **Cards must be loaded before tournaments** - The `deck_cards` table has a foreign key to `cards`. If cards aren't loaded first, deck parsing will still run but cards won't be found and won't be stored in `deck_cards`, resulting in incomplete deck data.

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

Before loading tournaments, you should load card data from Scryfall. This populates the `cards` table with oracle card information needed for deck parsing.

```bash
# Initial load (full refresh)
uv run python src/etl/main.py --data-type cards --mode initial

# Incremental load skips existing cards
uv run python src/etl/main.py --data-type cards --mode incremental

# Load with custom batch size
uv run python src/etl/main.py --data-type cards --mode initial --batch-size 500
```

#### Loading Tournament Data

The incremental load automatically tracks the last loaded tournament timestamp and only fetches new data. When tournaments are loaded, decks are automatically parsed and linked to cards in the `deck_cards` table.

The TopDeck API has a rate limit of 200 requests per minute. The client automatically enforces this limit with a 300ms delay between requests and includes retry logic for rate limit errors.

```bash
# Initial load overwrites any previous entry
uv run python src/etl/main.py --data-type tournaments --mode initial --days 180

# "Incremental" only loads data since last load
uv run python src/etl/main.py --data-type tournaments --mode incremental
```

#### Loading Archetype Classifications

Archetype classification uses LLMs to automatically categorize decks by analyzing mainboard cards. This requires both card and tournament data to be loaded first, and requires LLM API credentials.

**Key Features:**
- Analyzes mainboard cards (name, oracle text, type, mana cost, CMC, color identity)
- Classifies into archetype groups (format, main_title, color_identity, strategy)
- Provides confidence scores (0.0-1.0) for classification quality
- Tracks historical classifications (model, prompt version, timestamp)

**Strategy Types:** aggro, midrange, control, ramp, combo

```bash
# Initial load - classify all unclassified decks (uses DB_NAME env var or defaults to mtg-meta-mage-db)
uv run python src/etl/main.py --data-type archetypes --mode initial --model-provider azure_openai --prompt-id archetype_classification_v1

# Incremental load - classify decks from tournaments since last archetype load
uv run python src/etl/main.py --data-type archetypes --mode incremental --model-provider azure_openai --prompt-id archetype_classification_v1

# Load into test database
uv run python src/etl/main.py --data-type archetypes --mode initial --database mtg-meta-mage-db-test --model-provider azure_openai --prompt-id archetype_classification_v1
```

**Example Classification Output:**
```
main_title: "amulet_titan"
color_identity: "gruul"
strategy: "combo"
confidence: 0.95
```

**Reclassification Strategy:**
- Update `LARGE_LANGUAGE_MODEL` environment variable to use a different model
- Modify prompt template in `src/etl/prompts/archetype_classification_v{?}.txt` (or create new version)
- Run `--mode initial` to reclassify existing decks with new model/prompt
- Historical classifications are preserved in `archetype_classifications` table
- Each deck's `archetype_group_id` is updated to the latest classification

## Agent API & MCP Server

The application provides two access methods:
1. **Agent API** - Conversational chat interface with LangGraph orchestration
2. **MCP Server** - Discoverable tools for AI agents (Claude Desktop, etc.)

Both use the same underlying MCP tools, ensuring consistency.

### Agent API (Conversational Interface)

The Agent API provides a conversational chat interface powered by LangGraph that orchestrates MCP tools based on user intent. It maintains conversation state, enforces dependencies, and supports streaming responses.

**Key Features:**
- Intent-based routing between meta research and deck coaching workflows
- State management across conversation turns (format, archetype, deck data)
- Blocking dependency enforcement (e.g., deck required before optimization)
- SSE streaming with real-time updates (thinking, tool calls, content)
- Workflow interleaving (seamlessly switch between meta and deck workflows)

**Starting the Agent API:**

```bash
# Development mode with auto-reload
uv run uvicorn src.app.agent_api.main:app --reload --host 0.0.0.0 --port 8001

# Production mode
uv run uvicorn src.app.agent_api.main:app --host 0.0.0.0 --port 8001
```

The Agent API will be available at `http://localhost:8001`. OpenAPI documentation is available at:
- Swagger UI: `http://localhost:8001/docs`
- ReDoc: `http://localhost:8001/redoc`

**Agent API Endpoints:**

#### GET /welcome

Discovery endpoint that shows available workflows, formats, and tools before starting a conversation.

```bash
curl "http://localhost:8001/welcome"
```

**Response:**
```json
{
  "message": "Welcome to MTG Meta Mage! Here's what I can help you with:",
  "available_formats": ["Modern", "Pioneer", "Legacy", "Standard"],
  "workflows": [
    {
      "name": "meta_research",
      "description": "Format-wide analytics: meta rankings, matchup spreads, archetype lists",
      "example_queries": ["What are the top decks in Modern?", "Show me the Pioneer meta"],
      "tool_details": [
        {"name": "get_format_meta_rankings", "description": "..."},
        {"name": "get_format_matchup_stats", "description": "..."},
        {"name": "get_format_archetypes", "description": "..."}
      ]
    },
    {
      "name": "deck_coaching",
      "description": "Personalized coaching for your specific deck",
      "example_queries": ["How should I play against Tron?", "Optimize my sideboard"],
      "tool_details": [
        {"name": "get_enriched_deck", "description": "..."},
        {"name": "get_deck_matchup_stats", "description": "..."},
        {"name": "generate_deck_matchup_strategy", "description": "..."},
        {"name": "optimize_mainboard", "description": "..."},
        {"name": "optimize_sideboard", "description": "..."}
      ]
    }
  ],
  "tool_count": 8
}
```

#### POST /chat

Start or continue a conversation with streaming responses via Server-Sent Events (SSE).

**Request:**
```bash
curl -X POST "http://localhost:8001/chat" \
  -H "Content-Type: application/json" \
  -d '{
    "message": "What are the top decks in Modern?",
    "conversation_id": null,
    "context": {
      "format": "Modern",
      "days": 30
    }
  }'
```

**Response (SSE stream):**
```
event: metadata
data: {"conversation_id": "abc123", "format": "Modern"}

event: thinking
data: {"content": "Analyzing Modern meta rankings..."}

event: tool_call
data: {"tool": "get_format_meta_rankings", "status": "calling", "arguments": {"format": "Modern", "current_days": 30}}

event: tool_call
data: {"tool": "get_format_meta_rankings", "status": "complete", "summary": "Found 15 archetypes"}

event: content
data: {"text": "Based on the last 30 days of Modern data, here are the top archetypes:\n\n"}

event: content
data: {"text": "1. **Boros Energy** - 12.3% meta share, 54.2% win rate\n"}

event: state
data: {"has_deck": false, "format": "Modern", "archetype": null, "days": 30}

event: done
data: {}
```

**SSE Event Types:**
- `metadata` - Session info (conversation_id, format, archetype)
- `thinking` - Agent reasoning (optional)
- `tool_call` - Tool execution progress (calling/complete)
- `content` - Response text chunks
- `state` - State snapshot for UI synchronization
- `done` - Stream completion signal

**Conversation Context:**
- `format` (required) - Tournament format (Modern, Pioneer, etc.)
- `days` (optional) - Time window for meta analysis
- `archetype` (optional) - Archetype name after deck is provided
- `deck_text` (optional) - Raw deck list for coaching

**Workflow Routing:**
The agent automatically routes to the appropriate workflow based on intent:
- **Meta Research**: "top decks", "the meta", "matchup spread" (no deck provided)
- **Deck Coaching**: "my deck", "optimize", "how should I play" (deck provided)

**Blocking Dependencies:**
The agent enforces these dependencies before tool execution:
1. All tools require `format` - prompt user to select from dropdown
2. Deck coaching tools require `card_details` from prior `get_enriched_deck`
3. Deck coaching requires `archetype` - prompt after deck is provided
4. `generate_deck_matchup_strategy` requires `matchup_stats` from prior `get_deck_matchup_stats`
5. Meta tools prompt for `days` preference if not provided

#### GET /formats

Get available tournament formats for dropdown selection.

```bash
curl "http://localhost:8001/formats"
```

**Response:**
```json
{
  "formats": ["Modern", "Pioneer", "Legacy", "Standard", "Vintage", "Pauper"]
}
```

#### GET /archetypes

Get archetypes for a selected format with metadata for dropdown display.

```bash
curl "http://localhost:8001/archetypes?format=Modern"
```

**Response:**
```json
{
  "format": "Modern",
  "archetypes": [
    {
      "id": 1,
      "name": "Boros Energy",
      "meta_share": 12.3,
      "color_identity": "RW"
    },
    {
      "id": 2,
      "name": "Golgari Yawgmoth",
      "meta_share": 8.7,
      "color_identity": "BG"
    }
  ]
}
```

#### GET /conversations/{conversation_id}

Resume an existing conversation by retrieving its state and history.

```bash
curl "http://localhost:8001/conversations/abc123"
```

**Response:**
```json
{
  "conversation_id": "abc123",
  "state": {
    "format": "Modern",
    "archetype": "Burn",
    "days": 30,
    "has_deck": true
  },
  "messages": [
    {"role": "user", "content": "What's the Modern meta?"},
    {"role": "assistant", "content": "Based on the last 30 days..."}
  ]
}
```

### MCP Server

The MCP server runs alongside the FastAPI application and provides discoverable tools for meta research and deck coaching:

**Meta Research Tools:**
- `get_format_meta_rankings` - Archetype rankings with meta share and win rates
- `get_format_matchup_stats` - Head-to-head matchup matrix for all archetypes
- `get_format_archetypes` - List all archetypes in a format with metadata

**Deck Coaching Tools:**
- `get_enriched_deck` - Parse deck and enrich with card details from database
- `get_deck_matchup_stats` - Get matchup statistics for a specific archetype
- `generate_deck_matchup_strategy` - AI-powered coaching for specific matchups

**Deck Optimization Tools:**
- `optimize_mainboard` - Identify flex spots and recommend replacements based on top meta archetypes
- `optimize_sideboard` - Suggest sideboard changes to answer the most frequent meta matchups

The MCP server is accessible at `http://localhost:8000/mcp` and can be used by any MCP client (Claude Desktop, custom agents, etc.).

### Deck Optimization MCP Tools

The deck optimization tools are available through the MCP server and can be used by any MCP client (Claude Desktop, custom agents, etc.). These tools analyze your deck against the current meta and provide AI-powered recommendations.

**optimize_mainboard** - Identifies flex spots in your mainboard and recommends replacements based on top meta archetypes:
- Analyzes deck against top N meta archetypes (default: 5)
- Filters recommendations to format-legal cards matching your deck's color identity
- Uses actual meta decks (up to 5 per archetype) to inform suggestions
- Provides detailed justifications for each recommendation

**optimize_sideboard** - Suggests sideboard changes to answer the most frequent meta matchups:
- Recommends additions/removals to improve matchup coverage
- Generates matchup-specific sideboard plans (what to bring in/out)
- Enforces 15-card sideboard constraint with retry logic
- Uses observed opponent sideboard strategies from meta decks

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

### Database Configuration for Testing

The ETL pipelines support specifying a target database via the `--database` argument. This is useful for:
- Populating test databases with data
- Running pipelines against different environments
- Isolating test data from production

**Default Behavior:**
- If `--database` is not specified, the pipeline uses the `DB_NAME` environment variable
- `DB_NAME` should be set in your `.env` file (e.g., `mtg-meta-mage-db` for production)

**Examples:**

```bash
# Load archetypes into test database
uv run python src/etl/main.py \
  --data-type archetypes \
  --mode initial \
  --database mtg-meta-mage-db-test \
  --model-provider azure_openai \
  --prompt-id archetype_classification_v1

# Load cards into production database (uses DB_NAME env var or default)
uv run python src/etl/main.py \
  --data-type cards \
  --mode initial

# Load tournaments into a custom database
uv run python src/etl/main.py \
  --data-type tournaments \
  --mode initial \
  --days 90 \
  --database my-custom-db
```

**Note:** The test suite automatically manages the test database (`TEST_DB_NAME`) and loads required data via fixtures. You typically only need to manually run ETL pipelines for the test database if you want to pre-populate it before running tests.

## Project Structure

```
mtg-meta-mage/
├── src/
│   ├── clients/              # External API clients (LLM, Scryfall, TopDeck)
│   ├── app/
│   │   ├── agent_api/        # LangGraph agent with conversational interface
│   │   │   ├── graph.py       # LangGraph workflow (routing, subgraphs)
│   │   │   ├── routes.py      # FastAPI routes (/chat, /welcome, etc.)
│   │   │   ├── state.py       # Conversation state management
│   │   │   ├── store.py       # In-memory conversation storage
│   │   │   ├── streaming.py   # SSE event formatting
│   │   │   └── tool_catalog.py # MCP tool discovery
│   │   └── mcp/              # MCP server (business logic)
│   │       ├── tools/         # MCP tools (meta_research, deck_coaching)
│   │       └── prompts/       # LLM prompt templates
│   ├── etl/                  # ETL pipelines
│   │   ├── database/         # Database connection & schema
│   │   └── prompts/          # ETL prompt templates
│   └── core_utils.py         # Shared utilities (parse_deck, etc.)
├── tests/
│   ├── unit/
│   ├── integration/
│   └── postman/              # Postman collections for all endpoints
│       ├── agent/             # Agent API collection
│       └── mcp_server/        # MCP Server collection
├── openspec/                 # Specifications and change proposals
│   ├── specs/                # Current capabilities
│   └── changes/              # Proposed changes
├── pyproject.toml
├── AGENTS.md
└── README.md
```


## License

[Add your license here]

## Contributing

[Add contribution guidelines here]


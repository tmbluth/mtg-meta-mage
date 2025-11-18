# MTG Meta Mage

An AI-powered tool for analyzing Magic: The Gathering decklists against the competitive meta. Users can submit a decklist and format to get insights via LLM analysis.

## Features

- Card data collection from Scryfall API
- Tournament data collection from TopDeck.gg API
- PostgreSQL database for storing tournament, player, decklist, match, and cards data
- Initial bulk load and incremental update capabilities

## Prerequisites

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) package manager (install with `curl -LsSf https://astral.sh/uv/install.sh | sh`)
- PostgreSQL 18+ database set up
- TopDeck.gg API key ([Get API Key](https://topdeck.gg/docs/tournaments-v2))

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
```

## Data

### Database Setup

Create main and test DB and initialize the database schema:
```bash
uv run python src/database/init_db.py
```
This will create all necessary tables, indexes, and constraints.

### Database Schema

The database includes the following tables:

#### Card Data Tables
- **cards**: Scryfall oracle card data (card_id, name, oracle_text, rulings, type_line, mana_cost, cmc, color_identity, etc.)
  - Stores canonical card information from Scryfall's oracle cards bulk data
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

#### Metadata Table
- **load_metadata**: Tracks last successful load timestamp for incremental updates
  - Stores metadata for both tournament and card loads separately
  - Uses `data_type` field to distinguish between 'tournaments' and 'cards'
  - Uses `load_type` field to distinguish between 'initial' and 'incremental' loads

### Loading Data

**IMPORTANT**: Tables must be loaded in a specific order due to foreign key constraints:

1. **Cards must be loaded before tournaments** - The `deck_cards` table has a foreign key to `cards`. If cards aren't loaded first, decklist parsing will still run but cards won't be found and won't be stored in `deck_cards`, resulting in incomplete decklist data.

2. **Tournament data internal order** (handled automatically by the pipeline):
   - `tournaments` → `players` → `decklists` → `deck_cards` (requires cards to exist)
   - `tournaments` → `match_rounds` → `matches` (requires players to exist)

**First-time setup order:**
1. Initialize database schema
2. Load cards (initial load)
3. Load tournaments (initial load)

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

#### Scheduled Updates

**Daily**: Incremental tournament updates
```bash
# Run daily at off-peak hours
0 2 * * * cd /path/to/mtg-meta-mage && uv run python src/etl/main.py --data-type tournaments --mode incremental
```

**Check monthly for set releases**: Refresh card data
```bash
# Run monthly (e.g., first Sunday of the month at 3 AM)
0 3 1-7 * 0 cd /path/to/mtg-meta-mage && uv run python src/etl/main.py --data-type cards --mode incremental
```

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
│   ├── database/
│   ├── etl/
│   └──── api_clients/
├── tests/
│   ├── unit/
│   └── integration/
├── pyproject.toml
├── AGENTS.md
└── README.md
```


## License

[Add your license here]

## Contributing

[Add contribution guidelines here]


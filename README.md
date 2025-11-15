# MTG Meta Mage

An AI-powered tool for analyzing Magic: The Gathering decklists against the competitive meta. Users can submit a decklist and format to get insights via LLM analysis.

## Features

- Tournament data collection from TopDeck.gg API
- PostgreSQL database for storing tournament, player, decklist, and match data
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
DB_NAME=mtg_meta_mage
DB_USER=your_db_user
DB_PASSWORD=your_db_password

# TopDeck API
TOPDECK_API_KEY=your_topdeck_api_key
```

## Database Setup

1. Create a PostgreSQL database:
```bash
createdb mtg-meta-mage-db
```

2. Initialize the database schema:
```bash
uv run python src/database/init_db.py
```

This will create all necessary tables, indexes, and constraints.

## Database Schema

The database includes the following tables:

### Tournament Data Tables
- **tournaments**: Tournament metadata (ID, name, format, dates, location)
- **players**: Player performance data per tournament
- **decklists**: Decklist text storage linked to players
- **match_rounds**: Round information for tournaments
- **matches**: Individual match results (1v1 only)
- **load_metadata**: Tracks last successful load timestamp for incremental updates

### Card Data Tables
- **cards**: Scryfall oracle card data (card_id, name, oracle_text, rulings, type_line, mana_cost, cmc, color_identity, etc.)
  - Stores canonical card information from Scryfall's oracle cards bulk data
  - Each card represents a unique oracle card (not a specific printing)
  - Cards are loaded via `load_cards_from_bulk_data()` function
  - Indexed by name for efficient decklist matching

- **deck_cards**: Junction table linking decklists to individual cards
  - Stores parsed card entries from decklists with quantities and sections (mainboard/sideboard)
  - Cards are parsed from `decklist_text` using `parse_decklist()` function
  - Automatically populated when tournaments are loaded via `parse_and_store_decklist_cards()`
  - Cards not found in the cards table are logged but not stored

## Usage

### Loading Card Data from Scryfall

Before loading tournaments, you should load card data from Scryfall. This populates the `cards` table with oracle card information needed for decklist parsing.

**Using the CLI script (recommended):**

```bash
# Load only Scryfall card data
uv run python src/etl/main.py --data-type cards

# Load with custom batch size
uv run python src/etl/main.py --data-type cards --batch-size 500
```

**Using Python directly:**

```python
from src.etl.etl_pipeline import load_cards_from_bulk_data
from src.database.connection import DatabaseConnection

DatabaseConnection.initialize_pool()
result = load_cards_from_bulk_data(batch_size=1000)
print(f"Loaded {result['cards_loaded']} cards")
DatabaseConnection.close_pool()
```

**Note**: Scryfall bulk data is large (hundreds of MB). The first load may take several minutes. Subsequent loads use upsert logic to update existing cards.

**Recommendation**: Load Scryfall card data weekly or when new sets are released. See "Scryfall Bulk Data Updates" section below.

### Loading Tournament Data

#### Initial Load

Load tournament data from the past 90 days (default) or specify a custom number of days:

```bash
# Load only tournament data
uv run python src/etl/main.py --data-type tournaments --mode initial --days 90

# Or use default (tournaments, incremental)
uv run python src/etl/main.py --mode initial --days 90
```

#### Incremental Load

Load new tournaments since the last successful load:

```bash
uv run python src/etl/main.py --data-type tournaments --mode incremental

# Or use default (tournaments, incremental)
uv run python src/etl/main.py
```

The incremental load automatically tracks the last loaded tournament timestamp and only fetches new data. When tournaments are loaded, decklists are automatically parsed and linked to cards in the `deck_cards` table.

### Loading Both Card Data and Tournament Data

You can load both card data and tournament data in a single command:

```bash
# Load both cards and tournaments (incremental tournament load)
uv run python src/etl/main.py --data-type both

# Load both with initial tournament load
uv run python src/etl/main.py --data-type both --mode initial --days 90
```

This is useful for initial setup or when you want to refresh both datasets at once.

## Data Loading Scenarios

### Scenario 1: First-Time Setup

When setting up the database for the first time, you need to load card data before loading tournaments (so decklists can be properly parsed).

**Step 1: Initialize Database Schema**
```bash
uv run python src/database/init_db.py
```

**Step 2: Load Scryfall Card Data**
```bash
# This will take 5-10 minutes for the first load
uv run python src/etl/main.py --data-type cards
```

**Step 3: Load Tournament Data**
```bash
# Load tournaments from the past 90 days
uv run python src/etl/main.py --data-type tournaments --mode initial --days 90
```

**Alternative: Load Both at Once**
```bash
# Load cards first, then tournaments automatically
uv run python src/etl/main.py --data-type both --mode initial --days 90
```

### Scenario 2: Regular Incremental Updates

For ongoing operation, run incremental updates to fetch new tournaments since the last load.

**Daily/Weekly Incremental Update**
```bash
# Load only new tournaments (fast, typically < 1 minute)
uv run python src/etl/main.py --data-type tournaments --mode incremental

# Or use the default (same as above)
uv run python src/etl/main.py
```

**Note**: Card data doesn't need frequent updates. Only refresh cards when new sets are released or weekly/monthly.

### Scenario 3: Refreshing Card Data After New Set Release

When a new Magic set is released, refresh card data to include new cards.

**Refresh Card Data Only**
```bash
# Updates existing cards and adds new ones (uses upsert logic)
uv run python src/etl/main.py --data-type cards
```

**Refresh Both Cards and Load New Tournaments**
```bash
# Refresh cards, then load any new tournaments
uv run python src/etl/main.py --data-type both --mode incremental
```

**Expected Time**: 3-5 minutes (faster than initial load since it's updating existing records)

### Scenario 4: Loading Historical Tournament Data

To load tournaments from a specific time period (e.g., for analysis of a particular format or time period):

**Load Specific Date Range**
```bash
# Load tournaments from the past 180 days
uv run python src/etl/main.py --data-type tournaments --mode initial --days 180

# Load tournaments from the past 30 days
uv run python src/etl/main.py --data-type tournaments --mode initial --days 30
```

**Note**: The `--days` parameter only applies to initial loads. For incremental loads, it uses the last load timestamp automatically.

### Scenario 5: Production Deployment Workflow

For production systems, recommended workflow:

**Initial Deployment**
```bash
# 1. Initialize database
uv run python src/database/init_db.py

# 2. Load card data (one-time, takes 5-10 minutes)
uv run python src/etl/main.py --data-type cards

# 3. Load initial tournament data
uv run python src/etl/main.py --data-type tournaments --mode initial --days 90
```

**Scheduled Updates** (via cron or scheduler)

**Daily**: Incremental tournament updates
```bash
# Run daily at off-peak hours
0 2 * * * cd /path/to/mtg-meta-mage && uv run python src/etl/main.py --data-type tournaments --mode incremental
```

**Weekly**: Refresh card data
```bash
# Run weekly (e.g., Sunday at 3 AM)
0 3 * * 0 cd /path/to/mtg-meta-mage && uv run python src/etl/main.py --data-type cards
```

**Monthly**: Full refresh of both datasets
```bash
# Run monthly for comprehensive update
0 4 1 * * cd /path/to/mtg-meta-mage && uv run python src/etl/main.py --data-type both --mode incremental
```

## Data Filtering

The system automatically filters:

- **Commander formats**: EDH, Pauper EDH, Duel Commander, Tiny Leaders, EDH Draft, Oathbreaker
- **Multiplayer matches**: Only 1v1 matches and byes are stored (excludes pods with 3+ players)
- **Game type**: Only Magic: The Gathering tournaments are included

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

To run scripts with the virtual environment:

```bash
uv run python src/etl/main.py --data-type tournaments --mode initial
```

## Project Structure

```
mtg-meta-mage/
├── src/
│   ├── database/
│   ├── services/
│   └── data/
├── tests/
│   ├── unit/
│   └── integration/
├── pyproject.toml
└── README.md
```

## Rate Limiting

The TopDeck API has a rate limit of 200 requests per minute. The client automatically enforces this limit with a 300ms delay between requests and includes retry logic for rate limit errors.

## Error Handling

The ETL pipeline includes comprehensive error handling:

- Database connection errors are logged and retried
- API rate limit errors trigger exponential backoff
- Invalid tournament data is skipped with logging
- Transaction rollback on errors ensures data consistency
- Cards not found in the cards table during decklist parsing are logged but processing continues
- Scryfall bulk data download failures are logged with detailed error messages

## Scryfall Bulk Data Updates

Scryfall updates their bulk data files regularly. The `load_cards_from_bulk_data()` function automatically fetches the latest URLs from Scryfall's API.

**Update Frequency Recommendations:**
- **Weekly**: For active development and testing
- **Monthly**: For production systems (sufficient for most use cases)
- **On-demand**: When new sets are released or when you notice missing cards in decklist parsing

The function uses upsert logic (`ON CONFLICT DO UPDATE`), so re-running it is safe and will update existing cards with any changes from Scryfall.

**Performance Notes:**
- First load: ~5-10 minutes (downloads ~200MB+ of data)
- Subsequent loads: ~3-5 minutes (updates existing cards)
- Network speed and database performance affect load times

## License

[Add your license here]

## Contributing

[Add contribution guidelines here]


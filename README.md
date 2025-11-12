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
createdb mtg_meta_mage
```

2. Initialize the database schema:
```bash
uv run python src/database/init_db.py
```

This will create all necessary tables, indexes, and constraints.

## Database Schema

The database includes the following tables:

- **tournaments**: Tournament metadata (ID, name, format, dates, location)
- **players**: Player performance data per tournament
- **decklists**: Decklist text storage linked to players
- **match_rounds**: Round information for tournaments
- **matches**: Individual match results (1v1 only)
- **load_metadata**: Tracks last successful load timestamp for incremental updates

## Usage

### Initial Load

Load tournament data from the past 90 days (default) or specify a custom number of days:

```bash
uv run python src/data/load_tournaments.py --mode initial --days 90
```

### Incremental Load

Load new tournaments since the last successful load:

```bash
uv run python src/data/load_tournaments.py --mode incremental
```

The incremental load automatically tracks the last loaded tournament timestamp and only fetches new data.

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
uv run python src/data/load_tournaments.py --mode initial
```

## Project Structure

```
mtg-meta-mage/
├── src/
│   ├── database/
│   ├── services/
│   └── data/
├── tests/
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

## License

[Add your license here]

## Contributing

[Add contribution guidelines here]


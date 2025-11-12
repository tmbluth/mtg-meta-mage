# Project Context

## Purpose
MTG Meta Mage is an AI-powered tool for analyzing Magic: The Gathering decklists against the competitive meta. The system collects tournament data from TopDeck.gg API, stores it in PostgreSQL, and provides insights via LLM analysis. 

Users can:
- Submit decklists to get competitive meta analysis
- Archetype classification
- Meta tracking for weekly trends
- Receive sideboard optimizations for the meta
- Get matchup specific coaching
- Have their deck analyzed for synergies and anti-synergies (non-bos)

## Tech Stack
- **Language**: Python 3.11+
- **Package Manager**: [uv](https://github.com/astral-sh/uv)
- **Database**: PostgreSQL 18+
- **Core Dependencies**:
  - `psycopg2-binary` - PostgreSQL database adapter
  - `requests` - HTTP client for API calls
  - `python-dotenv` - Environment variable management
  - `pydantic` - Data validation and settings management
- **Testing**: `pytest` with custom markers for unit/integration tests

## Project Conventions

### Code Style
- Follow PEP-8 style guide
- Use type hints for all function signatures
- Prefer functional, declarative programming; avoid classes where possible
- Use lowercase with underscores for directories and files (e.g., `routers/user_routes.py`)
- Follow the Receive an Object, Return an Object (RORO) pattern
- Use early returns for error conditions to avoid deeply nested if statements
- Place the happy path last in functions for improved readability
- Avoid unnecessary else statements; use the if-return pattern
- Use guard clauses to handle preconditions and invalid states early
- Implement proper error logging with the `logging` module

### Architecture Patterns
- **Service Layer Pattern**: External API clients (`TopDeckClient`, `ScryfallClient`) abstract API interactions
- **Connection Pooling**: Database connections managed via `ThreadedConnectionPool` for efficient resource usage
- **ETL Pipeline**: Extract-Transform-Load pattern for tournament data ingestion
- **Context Managers**: Database transactions and cursors use context managers for automatic cleanup
- **Filter Functions**: Pure functions for data validation and filtering (e.g., `filter_tournaments`, `is_valid_match`)
- **Dependency Injection**: Services accept optional parameters (e.g., API keys) with fallback to environment variables

### Testing Strategy
- **Unit Tests**: Located in `tests/unit/`, use mocking to avoid external dependencies
  - Mark with `@pytest.mark.unit` marker
  - Test individual functions and classes in isolation
- **Integration Tests**: Located in `tests/integration/`, make real API/DB calls
  - Mark with `@pytest.mark.integration` marker
  - Require proper environment configuration (API keys, test database)
  - Use fixtures for shared test setup (`conftest.py`)
- **Test Organization**: Mirror source structure (`tests/unit/` and `tests/integration/` parallel `src/`)
- **Fixtures**: Use pytest fixtures for database config, API clients, and shared test data
- **Error Handling**: Tests should verify both success and error paths

### Git Workflow
- Feature branches: `feature/feature-name`
- Use descriptive commit messages
- Ensure tests pass before committing

## Domain Context
- **Magic: The Gathering Formats**: The system focuses on constructed formats (Standard, Modern, Legacy, Vintage, Pioneer, Pauper)
- **Tournament Structure**: Tournaments have rounds, matches, players, and decklists
- **Match Types**: Only 1v1 matches are stored (excludes multiplayer pods)
- **Excluded Formats**:
  - Commander formats: EDH, Pauper EDH, Duel Commander, Tiny Leaders, EDH Draft, Oathbreaker
  - Limited formats: Draft, Sealed, Limited, Booster Draft, Sealed Deck, Cube Draft, Team Draft, Team Sealed
- **Data Sources**: 
  - TopDeck.gg API provides tournament data, player standings, decklists, and match results
  - Scryfall API provides card data, prices, and rulings

## Important Constraints
- **Rate Limiting**: TopDeck API enforces 200 requests per minute (300ms delay between requests)
- **Data Filtering**: Database stores constructed-only formats (excludes Commander and Limited)
- **Match Filtering**: Only 1v1 matches are stored (excludes pods with 3+ players)
- **Game Type**: Only "Magic: The Gathering" tournaments are included
- **Database**: Requires PostgreSQL 18+ with proper schema initialization
- **API Attribution**: Must include TopDeck.gg attribution when displaying their data
- **Error Handling**: Comprehensive error handling with retry logic, exponential backoff, and transaction rollback

## External Dependencies
- **TopDeck.gg API**: 
  - Endpoint: `https://topdeck.gg/api`
  - Authentication: API key via `TOPDECK_API_KEY` environment variable
  - Rate Limit: 200 requests/minute
  - Provides: Tournament data, player standings, decklists, match rounds
- **Scryfall API**:
  - Endpoint: `https://api.scryfall.com`
  - No authentication required
  - Rate Limit: 50-100 requests/second
  - Provides: Card data, prices, rulings, bulk data downloads
- **PostgreSQL Database**:
  - Tables: `tournaments`, `players`, `decklists`, `match_rounds`, `matches`, `load_metadata`
  - Connection pooling: 1-10 connections (configurable)
  - Environment variables: `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`

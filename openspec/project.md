# Project Context

## Purpose
MTG Meta Mage is an AI-powered tool for analyzing Magic: The Gathering decklists against the competitive meta. The system collects tournament data from TopDeck.gg API, stores it in PostgreSQL, and provides insights via LLM analysis. 

**Current Features Implemented:**
- Card data collection from Scryfall API
- Tournament data collection from TopDeck.gg API
- LLM-powered archetype classification for decklists
- Initial and incremental ETL pipelines for cards, tournaments, and archetypes
- Meta analytics REST API with archetype rankings and matchup matrix

**Future Features:**
- Get decklist analysis 
  - Submit deck and select meta time window
  - Strongest/weakest cards against top tier decks
  - Deck piloting guide for selected meta
- Update decklist
  - Submit deck and select meta time window
  - Update maindeck flex spots for selected meta
  - Update sideboard for the selected meta
- User Interface
  - Meta analytics displayed 
  - Chat 

## Tech Stack
- **Language**: Python 3.11+
- **Package Manager**: [uv](https://github.com/astral-sh/uv)
- **Database**: PostgreSQL 18+
- **Core Dependencies**:
  - `psycopg2-binary` - PostgreSQL database adapter
  - `requests` - HTTP client for API calls
  - `python-dotenv` - Environment variable management
  - `pydantic` - Data validation and settings management
  - `langchain-core`, `langchain-openai`, `langchain-anthropic`, `langchain-aws` - LLM integration
- **Testing**: `pytest` with custom markers for unit/integration tests
- **Envitonment Variables**: these can be found in .env

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
- **Service Layer Pattern**: External API clients (`TopDeckClient`, `ScryfallClient`, `LLMClient`) abstract API interactions
- **Connection Pooling**: Database connections managed via `ThreadedConnectionPool` for efficient resource usage
- **ETL Pipeline**: Extract-Transform-Load pattern for tournament, card, and archetype data ingestion
  - `BasePipeline` abstract class with `load_initial()` and `load_incremental()` methods
  - `CardsPipeline`, `TournamentsPipeline`, `ArchetypeClassificationPipeline` extend `BasePipeline`
- **Context Managers**: Database transactions and cursors use context managers for automatic cleanup
- **Filter Functions**: Pure functions for data validation and filtering (e.g., `filter_tournaments`, `is_valid_match`)
- **Dependency Injection**: Services accept optional parameters (e.g., API keys, model names) with fallback to environment variables
- **Normalized Schema Design**: Two-table design for archetype classification (`archetype_groups` + `archetype_classifications`)
- MCP server to be decoupled from backend agent API

### Testing Strategy
- **Unit Tests**: Located in `tests/unit/`, use mocking to avoid external dependencies
  - Mark with `@pytest.mark.unit` marker
  - Test individual functions and classes in isolation
- **Integration Tests**: Located in `tests/integration/`, make real API/DB calls
  - Mark with `@pytest.mark.integration` marker
  - Require proper environment configuration (API keys, test database)
  - Use fixtures for shared test setup (`conftest.py`)
- **Postman Tests**: located in `tests/postman` to test API endpoints
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
  - LLM APIs (Azure OpenAI, OpenAI, Anthropic, AWS Bedrock) provide archetype classification

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
  - Provides: Card data and rulings via bulk data downloads
- **LLM APIs**:
  - Azure OpenAI: Requires `AZURE_OPENAI_API_KEY`, `AZURE_OPENAI_LLM_ENDPOINT`, `AZURE_OPENAI_API_VERSION`
  - OpenAI: Requires `OPENAI_API_KEY`
  - Anthropic: Requires `ANTHROPIC_API_KEY`
  - AWS Bedrock: Requires AWS credentials and `AWS_REGION`
  - Model selection: `LLM_MODEL` environment variable (e.g., "gpt-4o-mini", "claude-3-5-sonnet-20241022")
  - Provides: Archetype classification for decklists
- **PostgreSQL Database**:
  - Tables: `tournaments`, `players`, `decklists`, `match_rounds`, `matches`, `load_metadata`, `cards`, `deck_cards`, `archetype_groups`, `archetype_classifications`
  - Connection pooling: 1-10 connections (configurable)
  - Environment variables: `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` in .env.
  - Use `TEST_DB_NAME` in .env for testing database operations (make sure to clean up after)

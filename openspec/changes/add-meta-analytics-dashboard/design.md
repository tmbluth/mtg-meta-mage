# Design: Meta Analytics API

## Context
MTG Meta Mage collects tournament data from TopDeck.gg and classifies decklists using LLM-based archetype classification. This data is stored in PostgreSQL with a normalized schema. Users need to analyze meta trends to understand which archetypes are performing well and how different archetypes match up against each other.

**Stakeholders**: Competitive MTG players, deck builders, tournament organizers

**Constraints**:
- Database schema is already defined (tournaments, players, decklists, matches, archetype_groups, archetype_classifications)
- Backend-only feature (frontend development excluded from this change)
- Must support ALL constructed, non-Commander formats in the database dynamically (no hardcoded format lists)
- Formats include: Standard, Modern, Pioneer, Legacy, Vintage, Pauper, and any future formats added to tournaments table
- Time window comparisons should not overlap

## Goals / Non-Goals

**Goals**:
- Provide REST API for archetype performance analytics
- Calculate meta share and win rates for archetypes
- Support time window comparison for trend analysis
- Generate matchup matrix for head-to-head archetype performance
- Enable filtering by format, color_identity, and strategy
- Use polars for efficient data manipulation

**Non-Goals**:
- Frontend UI implementation (future work)
- Authentication/authorization (future consideration)
- Caching layer (can be added later if needed)
- Real-time updates (batch analytics sufficient for v1)
- Player-specific analytics (focus on archetype-level only)

## Decisions

### Decision 1: Use FastAPI for REST API
**Why**: FastAPI provides automatic OpenAPI documentation, built-in validation with Pydantic, async support, and excellent performance. It's a natural fit with the existing Pydantic usage in the codebase.

**Alternatives considered**:
- Flask: Less modern, no automatic validation, no async support
- Django REST Framework: Too heavyweight for analytics API needs

### Decision 2: Use polars for data aggregation
**Why**: polars excels at group-by operations, aggregations, and pivot tables needed for analytics. It's the industry standard for data analysis in Python and will significantly simplify the calculation logic.

**Alternatives considered**:
- Raw SQL with complex queries: Harder to maintain, less flexible for ad-hoc calculations
- Python dictionaries: Too manual, error-prone for complex aggregations

### Decision 3: Two main endpoints pattern
**Why**: Keep API simple and focused. Archetype rankings and matchup matrix are distinct use cases with different query patterns.

**Endpoints**:
1. `GET /api/v1/meta/archetypes?format={format}` - Ranked archetype list with filters (works for any format)
2. `GET /api/v1/meta/matchups?format={format}` - Matchup matrix for format (works for any format)

**Format Handling**:
- Format is a required query parameter
- No hardcoded format list - queries database for tournaments matching the format value
- If format has no data, returns empty results or 404
- This design automatically supports new formats as they appear in the database

### Decision 4: Separate services layer
**Why**: Keep route handlers thin, business logic testable. Services handle database queries and polars calculations, routes handle HTTP concerns.

**Structure**:
```
src/app/api/
├── main.py                 # FastAPI app initialization
├── models.py               # Pydantic request/response models
├── routes/
│   └── meta_routes.py      # Route handlers
└── services/
    └── meta_service.py     # Analytics calculation logic
```

### Decision 5: Default time windows
**Why**: Provide sensible defaults while allowing customization. "Last 2 weeks" is a common competitive meta snapshot period, "2-8 weeks ago" provides 6-week comparison window.

**Default values**:
- Current period: Last 2 weeks (14 days)
- Previous period: 2-8 weeks ago (6 weeks, no overlap with current)
- Parameterized via query string for flexibility

### Decision 6: Win rate calculation from matches table
**Why**: The matches table contains winner_id which can be joined with decklists to determine archetype win rates. This provides the most accurate head-to-head data.

**Calculation approach**:
1. Join matches → decklists → archetype_groups for both players
2. Count wins per archetype in time window
3. Calculate win rate = wins / (wins + losses)
4. Calculate meta share = deck_count / total_decks

## Risks / Trade-offs

**Risk**: Large datasets may cause slow queries
- **Mitigation**: Use existing database indexes, add query timeouts, monitor performance
- **Future**: Add caching layer if needed (Redis with TTL)

**Risk**: Matchup matrix may be sparse for less popular formats
- **Mitigation**: Document minimum match count requirements, return null for insufficient data
- **Future**: Consider Bayesian smoothing for small sample sizes

**Risk**: Time zone handling for tournament dates
- **Mitigation**: Store timestamps in UTC, perform time window calculations in UTC
- **Note**: Tournament start_date is already TIMESTAMP in schema

**Trade-off**: Using polars means loading data into memory
- **Pro**: Much simpler code, fast aggregations
- **Con**: Not suitable for streaming large datasets
- **Acceptable for v1**: Current data volume is manageable, can optimize later

## Migration Plan

**Phase 1: Implementation**
1. Add dependencies to pyproject.toml
2. Create FastAPI application structure
3. Implement archetype ranking endpoint with tests
4. Implement matchup matrix endpoint with tests
5. Add integration tests with test database

**Phase 2: Deployment**
1. Document API endpoints in README
2. Provide uvicorn startup command
3. No database migrations needed (using existing schema)

**Rollback**:
- Remove src/app/api directory
- Remove dependencies from pyproject.toml
- No database changes to revert

## Open Questions
None - requirements are clear and technical approach is straightforward.


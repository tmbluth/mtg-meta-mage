# Design: Card Data Tables for Deck Synergy Analysis

## Context
The MTG Meta Mage system currently stores decklists as raw text in the `decklists` table. To enable deck synergy analysis, optimization recommendations, and card combination queries, we need structured card data and a normalized relationship between decklists and cards.

Scryfall provides bulk data exports via their API:
- Oracle Cards: One card object per Oracle ID (159 MB, updated daily)
- Rulings: All rulings linked by oracle_id (23.2 MB, updated daily)

TopDeck.gg provides decklists as text in a standard MTG format (quantity + card name, with mainboard/sideboard sections).

## Goals / Non-Goals

### Goals
- Store Scryfall card data (oracle_text, rulings, type_line, mana_cost, etc.) in a queryable database table
- Parse decklist text to extract individual cards and link them to decklists via a junction table
- Support efficient queries for card combinations, synergies, and deck analysis
- Handle Scryfall bulk data updates (cards change infrequently, but rulings may be added)

### Non-Goals
- Real-time card price updates (prices are stale after 24 hours in bulk data)
- Storing all card printings (only Oracle cards, one per unique card)
- Parsing non-standard decklist formats
- Handling card name variations/disambiguation automatically (initial version)

## Decisions

### Decision: Use Scryfall UUID (id) as Primary Key
**Rationale**: Scryfall's `id` field is a UUID that uniquely identifies each card printing. However, for Oracle cards bulk data, we get one card per Oracle ID. We'll use the `id` field from the Oracle cards bulk data as our primary key, which ensures uniqueness and allows direct linking to Scryfall.

**Alternatives Considered**:
- Use `oracle_id` as primary key: Rejected because Oracle cards bulk data uses `id` (not `oracle_id`) and we need the specific card object's ID
- Composite key (name + set): Rejected because Oracle cards don't have a specific set (they're the "best" version)

### Decision: Store Rulings as Comma-Concatenated Text
**Rationale**: Rulings are text comments that don't need to be queried individually. Concatenating them with commas simplifies the schema and matches the requirement specification.

**Alternatives Considered**:
- Separate `rulings` table: Rejected for initial version - adds complexity without immediate query needs
- JSON array: Rejected - PostgreSQL text is simpler and sufficient for initial use case

### Decision: Store color_identity as Text Array
**Rationale**: PostgreSQL supports array types natively, and color_identity is naturally an array of color symbols (e.g., ["U", "R"]). This enables efficient array queries.

**Alternatives Considered**:
- Comma-separated string: Rejected - loses type safety and makes queries harder
- Separate junction table: Rejected - overkill for a small, fixed set of colors

### Decision: Parse Decklists During ETL, Not On-Demand
**Rationale**: Parsing decklists once during data ingestion allows us to populate `deck_cards` table immediately and enables efficient queries without parsing overhead.

**Alternatives Considered**:
- Parse on-demand: Rejected - would require parsing every query, inefficient for analysis workloads
- Separate parsing service: Rejected - adds unnecessary complexity for initial version

### Decision: Handle Missing Cards Gracefully
**Rationale**: Some decklist cards may not exist in Scryfall bulk data (very old cards, custom cards, typos). We'll log these cases but continue processing.

**Alternatives Considered**:
- Fail on missing cards: Rejected - too brittle, would break ETL pipeline
- Create placeholder entries: Rejected - pollutes card data with invalid entries

## Risks / Trade-offs

### Risk: Scryfall Bulk Data Size
**Mitigation**: Oracle cards is 159 MB uncompressed. We'll download and process in batches, using streaming JSON parsing if needed. Rulings are only 23.2 MB, manageable.

### Risk: Decklist Parsing Accuracy
**Mitigation**: Start with standard MTG decklist format (quantity + name). Add comprehensive unit tests with real examples. Log parsing failures for manual review.

### Risk: Card Name Matching
**Mitigation**: Initial version uses exact name matching. Future enhancement could use fuzzy matching or Scryfall's card name search API for disambiguation.

### Risk: Bulk Data Update Frequency
**Mitigation**: Scryfall updates bulk data every 12 hours. We'll document that card data should be refreshed weekly or after set releases. Add a mechanism to check for updates.

### Trade-off: Normalization vs Performance
**Decision**: Normalized schema (cards + deck_cards) enables efficient queries and reduces data duplication. Acceptable trade-off for query performance benefits.

## Migration Plan

1. **Schema Migration**: Add new tables without modifying existing `decklists` table (non-breaking)
2. **Data Loading**: 
   - Download Scryfall bulk data (oracle_cards + rulings)
   - Load into `cards` table (one-time initial load)
3. **Backfill Deck Cards**: 
   - Parse existing `decklist_text` entries
   - Populate `deck_cards` table for historical data
   - Handle parsing errors gracefully (log and continue)
4. **ETL Integration**: 
   - Modify ETL pipeline to parse decklists during ingestion
   - Populate `deck_cards` for new decklists automatically

**Rollback**: Since we're adding new tables, rollback is straightforward:
- Drop `deck_cards` table
- Drop `cards` table
- No impact on existing `decklists` table

## Open Questions

1. **Card Name Variations**: How should we handle card name variations (e.g., "Lightning Bolt" vs "Lightning Bolt (Borderless)")? 
   - **Answer**: Initial version uses exact matching. Future: Use Scryfall's fuzzy search or normalize names.

2. **Bulk Data Refresh Strategy**: Should we automatically refresh Scryfall bulk data, or require manual refresh?
   - **Answer**: Start with manual refresh. Add automatic weekly refresh in future iteration.

3. **Decklist Format Variations**: What decklist formats does TopDeck.gg actually provide?
   - **Answer**: Need to inspect real TopDeck.gg API responses to confirm format. Assume standard MTG format initially.


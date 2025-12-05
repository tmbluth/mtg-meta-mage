## 0. Schema and Data Migration

- [x] 0.1 Add legalities column to cards table in `src/etl/database/schema.sql`
  - [x] 0.1.1 Add `legalities JSONB` column to cards table
  - [x] 0.1.2 Add GIN index on legalities field for efficient JSON queries
  - [x] 0.1.3 Create migration script to add column to existing database
- [x] 0.2 Update ScryfallClient to capture legalities in `src/clients/scryfall_client.py`
  - [x] 0.2.1 Update `transform_card_to_db_row` to extract legalities from Scryfall card object
  - [x] 0.2.2 Convert legalities dict to JSONB-compatible format
  - [x] 0.2.3 Handle missing legalities field (default to empty JSON object)
- [x] 0.3 Update CardsPipeline to store legalities in `src/etl/cards_pipeline.py`
  - [x] 0.3.1 Add legalities to INSERT statement
  - [x] 0.3.2 Add legalities to ON CONFLICT UPDATE clause
  - [x] 0.3.3 Update batch_data tuple to include legalities
- [x] 0.4 Run cards ETL pipeline to populate legalities data
  - [x] 0.4.1 Execute migration script on database
  - [x] 0.4.2 Run `python -m src.etl.main --load-cards --initial` to reload all cards with legalities
  - [x] 0.4.3 Verify legalities data is populated with spot checks
- [x] 0.5 Add unit tests for legalities in `tests/unit/test_cards_pipeline.py`
  - [x] 0.5.1 Test transform_card_to_db_row includes legalities
  - [x] 0.5.2 Test INSERT with legalities field
  - [x] 0.5.3 Test query filtering by legalities->>'format'

## 1. Shared Functionality Implementation (TDD)

- [ ] 1.1 Create prompt templates in `src/app/mcp/prompts/`
  - [ ] 1.1.1 Create `mainboard_optimization_prompt.py` with template for flex spot identification
  - [ ] 1.1.2 Create `sideboard_optimization_prompt.py` with template for sideboard tweaks
  - [ ] 1.1.3 Both templates include format parameter and legality instructions
- [ ] 1.2 Create shared helper function `_get_legal_cards_for_format` in `deck_coaching_tools.py`
  - [ ] 1.2.1 Accept parameters: format (str)
  - [ ] 1.2.2 Normalize format parameter to lowercase before querying
  - [ ] 1.2.3 Query cards table: `WHERE legalities->>'normalized_format' = 'legal'`
  - [ ] 1.2.4 Join with deck_cards table to filter to commonly-played cards (appearing in decklists from last 180 days)
  - [ ] 1.2.5 Return DataFrame with card_id, name, type_line, mana_cost, cmc, color_identity
  - [ ] 1.2.6 Handle missing legality data with appropriate errors
- [ ] 1.3 Create shared helper function `_determine_deck_color_identity` in `deck_coaching_tools.py`
  - [ ] 1.3.1 Accept card_details list from user's deck
  - [ ] 1.3.2 Aggregate all color_identity values from cards
  - [ ] 1.3.3 Return set of colors present in deck (e.g., {'W', 'U', 'B'})
- [ ] 1.4 Create shared helper function `_filter_cards_by_color_identity` in `deck_coaching_tools.py`
  - [ ] 1.4.1 Accept parameters: legal_cards (DataFrame), deck_colors (set)
  - [ ] 1.4.2 Include cards where color_identity is subset of deck_colors
  - [ ] 1.4.3 Always include colorless cards (empty color_identity)
  - [ ] 1.4.4 Strip phyrexian mana symbols from mana_cost before evaluating color requirements
  - [ ] 1.4.5 Treat phyrexian mana as zero cost (e.g., {W/P}{U} functionally becomes {U})
  - [ ] 1.4.6 Include cards with only generic mana costs ({1}, {2}, {X})
  - [ ] 1.4.7 Return filtered DataFrame of color-appropriate cards
- [ ] 1.5 Create shared helper function `_fetch_archetype_decklists` in `deck_coaching_tools.py`
  - [ ] 1.5.1 Accept parameters: archetype_group_ids (list), format (str), limit_per_archetype (int, default: 5)
  - [ ] 1.5.2 Query decklists table joined with tournaments and deck_cards for each archetype_group_id
  - [ ] 1.5.3 Order by tournament start_date descending
  - [ ] 1.5.4 Limit to N most recent decklists per archetype
  - [ ] 1.5.5 Return structured dict mapping archetype_id to list of decklists with card details
- [ ] 1.6 Create shared helper function `_format_archetype_decklists_for_prompt` in `deck_coaching_tools.py`
  - [ ] 1.6.1 Accept archetype decklists dict and archetype metadata
  - [ ] 1.6.2 Format decklists into readable text for LLM prompt
  - [ ] 1.6.3 Include archetype name, meta share, and sample card lists
  - [ ] 1.6.4 For mainboard optimization: include mainboard cards only
  - [ ] 1.6.5 For sideboard optimization: include both mainboard and sideboard cards
- [ ] 1.7 Create shared helper function `_format_legal_cards_for_prompt` in `deck_coaching_tools.py`
  - [ ] 1.7.1 Accept filtered legal cards DataFrame
  - [ ] 1.7.2 Format into concise text for LLM context (name, type, mana cost)
  - [ ] 1.7.3 Limit output size (e.g., group by card type or CMC ranges)
  - [ ] 1.7.4 Return formatted string for prompt inclusion

## 2. Mainboard Optimization (TDD)

- [ ] 2.1 Write unit tests for `optimize_mainboard` in `tests/unit/test_deck_coaching_tools.py`
  - [ ] 2.1.1 Test with valid deck and top 5 archetypes (happy path)
  - [ ] 2.1.2 Test with empty meta data (error handling)
  - [ ] 2.1.3 Test format legality verification
  - [ ] 2.1.4 Test with unavailable card legality data
  - [ ] 2.1.5 Mock `get_format_meta_rankings` and LLM client calls
- [ ] 2.2 Implement `optimize_mainboard` MCP tool in `deck_coaching_tools.py`
  - [ ] 2.2.1 Add @mcp.tool() decorator and function signature
  - [ ] 2.2.2 Accept parameters: card_details, archetype, format, top_n (default: 5)
  - [ ] 2.2.3 Normalize format parameter to lowercase
  - [ ] 2.2.4 Call `get_format_meta_rankings` to get top N archetypes
  - [ ] 2.2.5 Handle empty meta data with error response
  - [ ] 2.2.6 Query legal cards: `_get_legal_cards_for_format(format)`
  - [ ] 2.2.7 Determine deck color identity: `_determine_deck_color_identity(card_details)`
  - [ ] 2.2.8 Filter legal cards by color identity: `_filter_cards_by_color_identity(legal_cards, deck_colors)`
  - [ ] 2.2.9 Extract archetype_group_ids from top N archetypes
  - [ ] 2.2.10 Call `_fetch_archetype_decklists` to get recent decklists for each archetype (max 5 per archetype)
  - [ ] 2.2.11 Format deck summary, archetype decklists, and filtered legal cards for prompt
  - [ ] 2.2.12 Send to LLM with mainboard optimization prompt requesting JSON output
  - [ ] 2.2.13 Parse JSON response for recommended cards
  - [ ] 2.2.14 Return structured response with flex spots and recommendations
- [ ] 2.3 Run unit tests and iterate until passing: `pytest tests/unit/test_deck_coaching_tools.py::test_optimize_mainboard* -v`

## 3. Sideboard Optimization (TDD)

- [ ] 3.1 Write unit tests for `optimize_sideboard` in `tests/unit/test_deck_coaching_tools.py`
  - [ ] 3.1.1 Test with valid deck and top 5 archetypes (happy path)
  - [ ] 3.1.2 Test with empty meta data (error handling)
  - [ ] 3.1.3 Test format legality verification
  - [ ] 3.1.4 Test 15-card sideboard constraint validation
  - [ ] 3.1.5 Test with unavailable card legality data
  - [ ] 3.1.6 Mock `get_format_meta_rankings` and LLM client calls
- [ ] 3.2 Implement `optimize_sideboard` MCP tool in `deck_coaching_tools.py`
  - [ ] 3.2.1 Add @mcp.tool() decorator and function signature
  - [ ] 3.2.2 Accept parameters: card_details, archetype, format, top_n (default: 5)
  - [ ] 3.2.3 Normalize format parameter to lowercase
  - [ ] 3.2.4 Call `get_format_meta_rankings` to get top N archetypes
  - [ ] 3.2.5 Handle empty meta data with error response
  - [ ] 3.2.6 Query legal cards: `_get_legal_cards_for_format(format)`
  - [ ] 3.2.7 Determine deck color identity: `_determine_deck_color_identity(card_details)`
  - [ ] 3.2.8 Filter legal cards by color identity: `_filter_cards_by_color_identity(legal_cards, deck_colors)`
  - [ ] 3.2.9 Extract archetype_group_ids from top N archetypes
  - [ ] 3.2.10 Call `_fetch_archetype_decklists` to get recent decklists for each archetype (max 5 per archetype)
  - [ ] 3.2.11 Format current sideboard, archetype decklists (with sideboards), and filtered legal cards for prompt
  - [ ] 3.2.12 Send to LLM with sideboard optimization prompt requesting JSON output with exactly 15 cards
  - [ ] 3.2.13 Parse JSON response for sideboard changes
  - [ ] 3.2.14 Validate resulting sideboard has exactly 15 cards
  - [ ] 3.2.15 If validation fails, retry with explicit 15-card requirement (up to 2 retries)
  - [ ] 3.2.16 Return structured response with additions/removals and justifications
- [ ] 3.3 Run unit tests and iterate until passing: `pytest tests/unit/test_deck_coaching_tools.py::test_optimize_sideboard* -v`

## 4. Integration Tests and Documentation

- [ ] 4.1 Write integration tests in `tests/integration/test_mcp_workflows.py`
  - [ ] 4.1.1 Test full mainboard optimization workflow with real deck and meta data
  - [ ] 4.1.2 Test full sideboard optimization workflow with real deck and meta data
  - [ ] 4.1.3 Test both tools together for complete deck optimization
  - [ ] 4.1.4 Verify format legality constraints are enforced end-to-end
- [ ] 4.2 Run integration tests: `pytest tests/integration/test_mcp_workflows.py -v -m integration`
- [ ] 4.3 Update MCP server documentation in `tests/postman/mcp_server/README.md`
  - [ ] 4.3.1 Document `optimize_mainboard` tool with parameters and examples
  - [ ] 4.3.2 Document `optimize_sideboard` tool with parameters and examples
  - [ ] 4.3.3 Add example workflows for deck optimization

## 5. Validation

- [ ] 5.1 Run `openspec validate add-deck-optimization-tools --strict`
- [ ] 2.2 Manual testing: Test both tools with sample decklists against current meta


## 1. Database Schema
- [x] 1.1 Write unit tests for cards table schema (card_id PK, set, collector_num, name, oracle_text, rulings, type_line, mana_cost, cmc, color_identity, scryfall_uri)
- [x] 1.2 Write unit tests for deck_cards table schema (decklist_id FK, card_id FK, section, quantity)
- [x] 1.3 Add CREATE TABLE statements for `cards` table to schema.sql
- [x] 1.4 Add CREATE TABLE statements for `deck_cards` table to schema.sql
- [x] 1.5 Add foreign key constraints and indexes for performance
- [x] 1.6 Update init_db.py to execute new schema changes

## 2. Scryfall Client Enhancements
- [x] 2.1 Review unit tests for parsing Scryfall oracle_cards bulk data structure
- [x] 2.2 Review unit tests for parsing Scryfall rulings bulk data structure
- [x] 2.3 Review unit tests for joining cards with rulings by oracle_id
- [x] 2.4 Enhance `download_bulk_data` to return structured data ready for database insertion
- [x] 2.5 Add method to transform Scryfall card objects to database row format
- [x] 2.6 Add method to concatenate rulings by comma for a card

## 3. Decklist Parser
- [x] 3.1 Review unit tests for parsing standard MTG decklist format (quantity + card name)
- [x] 3.2 Review unit tests for identifying mainboard vs sideboard sections
- [x] 3.3 Review unit tests for handling edge cases (empty decklists, malformed entries, special characters)
- [x] 3.4 Add `parse_decklist` function to `src/etl/utils.py`
- [x] 3.5 Implement mainboard/sideboard detection logic

## 4. Card Data Loader
- [x] 4.1 Review unit tests for loading cards from Scryfall bulk data into database
- [x] 4.2 Review unit tests for handling duplicate cards (upsert logic)
- [x] 4.3 Review unit tests for joining and storing rulings
- [x] 4.4 Add `insert_cards` method to `CardsPipeline` class in `src/etl/cards_pipeline.py`
- [x] 4.5 Implement batch insertion with error handling

## 5. Deck Cards ETL
- [x] 5.1 Write unit tests for extracting cards from decklist_text and matching to cards table
- [x] 5.2 Write unit tests for handling cards not found in cards table (logging/mapping)
- [x] 5.3 Write unit tests for populating deck_cards table with quantities and sections
- [x] 5.4 Add `insert_deck_cards` method to `TournamentsPipeline` class in `src/etl/tournaments_pipeline.py`
- [x] 5.5 Integrate decklist parsing into existing ETL pipeline

## 6. Validation and Error Handling
- [x] 6.1 Write unit tests for error handling when Scryfall bulk data is unavailable
- [x] 6.2 Write unit tests for error handling when decklist parsing fails
- [x] 6.3 Write unit tests for transaction rollback on card loading errors
- [x] 6.4 Add comprehensive logging for card data operations

## 7. Integration Tests
- [x] 7.1 Write integration tests for schema creation (in `test_e2e_cards.py` and `test_e2e_tournaments.py`)
- [x] 7.2 Write integration tests for Scryfall bulk data download, parsing and loading in `test_e2e_cards.py`
- [x] 7.3 Write integration tests for end-to-end decklist processing for both Topdeck and Scryfall data in `test_e2e_tournaments.py`

## 8. Documentation
- [x] 8.1 Write good logging in all the new app functionality developed in this task list
- [x] 8.2 Scan codebase and align documentation with code
- [x] 8.3 Update README.md with card data tables documentation
- [x] 8.4 Update schema.sql comments with table descriptions
- [x] 8.5 Document Scryfall bulk data update frequency recommendations



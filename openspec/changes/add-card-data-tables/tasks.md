## 1. Database Schema
- [ ] 1.1 Write unit tests for cards table schema (card_id PK, set, collector_num, name, oracle_text, rulings, type_line, mana_cost, cmc, color_identity, scryfall_uri)
- [ ] 1.2 Write unit tests for deck_cards table schema (decklist_id FK, card_id FK, section, quantity)
- [ ] 1.3 Add CREATE TABLE statements for `cards` table to schema.sql
- [ ] 1.4 Add CREATE TABLE statements for `deck_cards` table to schema.sql
- [ ] 1.5 Add foreign key constraints and indexes for performance
- [ ] 1.6 Update init_db.py to execute new schema changes
- [ ] 1.7 Write integration tests for schema creation

## 2. Scryfall Client Enhancements
- [ ] 2.1 Write unit tests for parsing Scryfall oracle_cards bulk data structure
- [ ] 2.2 Write unit tests for parsing Scryfall rulings bulk data structure
- [ ] 2.3 Write unit tests for joining cards with rulings by oracle_id
- [ ] 2.4 Enhance `download_bulk_data` to return structured data ready for database insertion
- [ ] 2.5 Add method to transform Scryfall card objects to database row format
- [ ] 2.6 Add method to concatenate rulings by comma for a card
- [ ] 2.7 Write integration tests for Scryfall bulk data download and parsing

## 3. Decklist Parser
- [ ] 3.1 Write unit tests for parsing standard MTG decklist format (quantity + card name)
- [ ] 3.2 Write unit tests for identifying mainboard vs sideboard sections
- [ ] 3.3 Write unit tests for handling edge cases (empty decklists, malformed entries, special characters)
- [ ] 3.4 Create `src/data/decklist_parser.py` with `parse_decklist` function
- [ ] 3.5 Implement mainboard/sideboard detection logic
- [ ] 3.6 Write integration tests with real decklist examples

## 4. Card Data Loader
- [ ] 4.1 Write unit tests for loading cards from Scryfall bulk data into database
- [ ] 4.2 Write unit tests for handling duplicate cards (upsert logic)
- [ ] 4.3 Write unit tests for joining and storing rulings
- [ ] 4.4 Create `src/data/card_loader.py` with `load_cards_from_bulk_data` function
- [ ] 4.5 Implement batch insertion with error handling
- [ ] 4.6 Write integration tests for full card data loading pipeline

## 5. Deck Cards ETL
- [ ] 5.1 Write unit tests for extracting cards from decklist_text and matching to cards table
- [ ] 5.2 Write unit tests for handling cards not found in cards table (logging/mapping)
- [ ] 5.3 Write unit tests for populating deck_cards table with quantities and sections
- [ ] 5.4 Add `parse_and_store_decklist_cards` method to ETLPipeline
- [ ] 5.5 Integrate decklist parsing into existing ETL pipeline
- [ ] 5.6 Write integration tests for end-to-end decklist processing

## 6. Validation and Error Handling
- [ ] 6.1 Write unit tests for error handling when Scryfall bulk data is unavailable
- [ ] 6.2 Write unit tests for error handling when decklist parsing fails
- [ ] 6.3 Write unit tests for transaction rollback on card loading errors
- [ ] 6.4 Add comprehensive logging for card data operations
- [ ] 6.5 Write integration tests for error recovery scenarios

## 7. Documentation
- [ ] 7.1 Update README.md with card data tables documentation
- [ ] 7.2 Update schema.sql comments with table descriptions
- [ ] 7.3 Document decklist format expectations
- [ ] 7.4 Document Scryfall bulk data update frequency recommendations


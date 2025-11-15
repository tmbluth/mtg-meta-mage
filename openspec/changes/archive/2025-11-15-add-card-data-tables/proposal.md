# Change: Add Card Data Tables for Deck Synergy Analysis

## Why
To enable deck synergy and optimization analysis, we need structured card data from Scryfall and a way to link individual cards to decklists. Currently, decklists are stored as raw text, making it impossible to query card combinations, analyze synergies, or provide optimization recommendations.

## What Changes
- Add `cards` table to store Scryfall oracle card data (oracle_text, rulings, type_line, mana_cost, etc.)
- Add `deck_cards` table to link decklists to individual cards with quantities and sections (mainboard/sideboard)
- Add ETL process to load Scryfall bulk data (oracle_cards and rulings) into the `cards` table
- Add decklist parsing functionality to extract cards from `decklist_text` and populate `deck_cards`
- Enhance `ScryfallClient` to transform card data for database insertion (bulk data download already exists)

## Impact
- Affected specs: New capability `card-data-management`
- Affected code: 
  - `src/database/schema.sql` - Add new tables and indexes 
  - `src/database/init_db.py` - Schema initialization (CLI script)
  - `src/etl/api_clients/scryfall_client.py` - Add card transformation methods (bulk download already exists)
  - `src/etl/etl_pipeline.py` - Consolidated ETL pipeline including:
    - Card data loading (`load_cards_from_bulk_data` function)
    - Decklist parsing (`parse_decklist` function)
    - Tournament filtering functions (`filter_tournaments`, `filter_rounds_data`, etc.)
    - Tournament ETL pipeline (`ETLPipeline` class)
  - `src/etl/main.py` - CLI entry point for loading tournaments


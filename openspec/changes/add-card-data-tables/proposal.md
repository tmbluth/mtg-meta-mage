# Change: Add Card Data Tables for Deck Synergy Analysis

## Why
To enable deck synergy and optimization analysis, we need structured card data from Scryfall and a way to link individual cards to decklists. Currently, decklists are stored as raw text, making it impossible to query card combinations, analyze synergies, or provide optimization recommendations.

## What Changes
- Add `cards` table to store Scryfall oracle card data (oracle_text, rulings, type_line, mana_cost, etc.)
- Add `deck_cards` table to link decklists to individual cards with quantities and sections (mainboard/sideboard)
- Add ETL process to load Scryfall bulk data (oracle_cards and rulings) into the `cards` table
- Add decklist parsing functionality to extract cards from `decklist_text` and populate `deck_cards`
- Enhance `ScryfallClient` to support structured bulk data processing for database insertion

## Impact
- Affected specs: New capability `card-data-management`
- Affected code: 
  - `src/database/schema.sql` - Add new tables and indexes
  - `src/database/init_db.py` - Schema initialization
  - `src/services/scryfall_client.py` - Enhance bulk data processing
  - `src/data/etl_pipeline.py` - Add card data loading and decklist parsing
  - New module: `src/data/decklist_parser.py` - Parse decklist text format
  - New module: `src/data/card_loader.py` - Load Scryfall bulk data into database


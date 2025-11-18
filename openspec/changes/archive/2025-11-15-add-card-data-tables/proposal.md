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
  - `src/etl/base_pipeline.py` - Base abstract class for ETL pipelines
  - `src/etl/cards_pipeline.py` - Cards ETL pipeline (`CardsPipeline` class) including:
    - Card data loading (`insert_cards` method)
  - `src/etl/tournaments_pipeline.py` - Tournament ETL pipeline (`TournamentsPipeline` class) including:
    - Tournament filtering functions (`filter_tournaments`, `filter_rounds_data`, etc.)
    - Decklist processing and deck_cards insertion (`insert_deck_cards` method)
  - `src/etl/utils.py` - Utility functions including:
    - Decklist parsing (`parse_decklist` function)
    - Card name normalization (`normalize_card_name` function)
    - Load metadata management (`get_last_load_timestamp`, `update_load_metadata`)
  - `src/etl/main.py` - CLI entry point for loading tournaments and cards


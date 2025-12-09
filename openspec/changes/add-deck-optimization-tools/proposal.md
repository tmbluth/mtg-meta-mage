# Change: Add Deck Optimization Tools

## Why
Users need tools to optimize their decks against the current meta. The existing deck coaching tools provide matchup-specific advice but don't help users identify which cards to change in their maindeck flex spots or how to tune their sideboard to answer the most frequent archetypes in the format.

## What Changes
- Add `optimize_mainboard` MCP tool that analyzes a user's maindeck, identifies non-critical cards (flex spots), and recommends replacements in response to on actual meta decks from the top N most frequent archetypes
- Add `optimize_sideboard` MCP tool that suggests sideboard tweaks to better answer the top N most frequent archetypes, considering opponent sideboard plans in games 2 and 3 based on observed sideboard cards
- Both tools leverage meta rankings data (via `get_format_meta_rankings`) to identify top N archetypes
- Tools fetch up to 5 recent decks per archetype from the database to provide concrete card data to the LLM
- Tools provide justifications for recommendations based on actual cards observed in meta decks (why these cards, why not alternatives)

## Impact
- Affected specs: `matchup-coaching`, `card-data-management`
- Affected code: 
  - `src/app/mcp/tools/deck_coaching_tools.py` (new tools and filtering functions)
  - `src/app/mcp/prompts/` (new prompt templates)
  - `src/etl/database/schema.sql` (add legalities JSONB column to cards table)
  - `src/etl/cards_pipeline.py` (capture legalities from Scryfall)
  - `src/clients/scryfall_client.py` (transform legalities to database format)
- Data migration: Re-run cards ETL pipeline to populate legalities field
- New dependencies: None (leverages existing LLM client and meta research tools)


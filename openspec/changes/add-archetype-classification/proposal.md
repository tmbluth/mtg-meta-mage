# Change: Add LLM-Based Archetype Classification

## Why
MTG Meta Mage needs to automatically classify decklists into archetypes (e.g., "Amulet Titan", "Elves", "Dimir Control") to enable meta analysis, trend tracking, and matchup coaching. Rather than using clustering or rule-based systems, this change leverages LLMs to provide flexible, context-aware archetype identification that understands card synergies and deck strategies.

## What Changes
- Add `archetypes` table to store deck classifications with metadata (format, main_title, color_identity, strategy, confidence, LLM model, prompt_id, timestamp)
- Create independent archetype ETL pipeline that runs after tournament/card data loads
- Implement LLM-based classification logic that analyzes mainboard cards and their attributes (name, oracle_text, type_line, mana_cost, cmc, color_identity)
- Add CLI command for archetype classification with modes (initial/incremental) and time window parameters
- Link archetypes to decklists with support for multiple classifications per deck (decklists use latest archetype_id)
- **BREAKING**: Modify `decklists` table to add `archetype_id` foreign key field

## Impact
- Affected specs: New capability `archetype-classification`
- Affected code:
  - `src/database/schema.sql` - Add `archetypes` table, modify `decklists` table
  - `src/etl/` - New archetype ETL pipeline module and `prompts` folder
  - `src/etl/main.py` - Add archetype classification CLI command
  - `tests/unit/` - New unit tests for archetype classification logic
  - `tests/integration/` - New integration tests for archetype ETL pipeline
- New dependencies: `strands-agents` LLM abstraction library (OpenAI, Azure OpenAI, Anthropic, Bedrock)


# Change: Add LLM-Based Archetype Classification

## Why
MTG Meta Mage needs to automatically classify decklists into archetypes (e.g., "Amulet Titan", "Elves", "Dimir Control") to enable meta analysis, trend tracking, and matchup coaching. Rather than using clustering or rule-based systems, this change leverages LLMs to provide flexible, context-aware archetype identification that understands card synergies and deck strategies.

## What Changes
- Add `archetype_groups` table to store unique archetype definitions (format, main_title, color_identity, strategy) with database-enforced uniqueness
- Add `archetype_classifications` table to store classification events with metadata (confidence, LLM model, prompt_id, timestamp)
- Create `ArchetypeClassificationPipeline` class extending `BasePipeline` for independent ETL that runs after tournament/card data loads
- Implement LLM-based classification logic that analyzes mainboard cards and their attributes (name, oracle_text, type_line, mana_cost, cmc, color_identity)
- Add CLI command for archetype classification with modes (initial/incremental) following existing ETL patterns
- Link decklists to archetype groups with support for historical classification tracking (decklists use `archetype_group_id`)
- Use existing `load_metadata` table with `data_type='archetypes'` for tracking classification loads
- **BREAKING**: Modify `decklists` table to add `archetype_group_id` foreign key field

## Impact
- Affected specs: New capability `archetype-classification`
- Affected code:
  - `src/database/schema.sql` - Add `archetype_groups` and `archetype_classifications` tables, modify `decklists` table
  - `src/etl/archetype_pipeline.py` - New archetype ETL pipeline module
  - `src/etl/api_clients/llm_client.py` - LLM client abstraction using Langchain
  - `src/etl/prompts/archetype_classification_v1.txt` - Prompt template for classification
  - `src/etl/main.py` - Add archetype classification CLI command
  - `tests/unit/` - New unit tests for archetype classification logic
  - `tests/integration/` - New integration tests for archetype ETL pipeline
- New dependencies: `langchain-core`, `langchain-openai`, `langchain-anthropic`, `langchain-aws` for LLM integration


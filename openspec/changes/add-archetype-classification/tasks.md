# Implementation Tasks

## 1. Database Schema
- [ ] 1.1 Add `archetypes` table to `schema.sql` with all fields (archetype_id, decklist_id, format, main_title, color_identity, strategy, archetype_confidence, llm_model, prompt_id, classified_at)
- [ ] 1.2 Add `archetype_id` foreign key column to `decklists` table
- [ ] 1.3 Create indexes on `archetypes.decklist_id`, `archetypes.format`, and `decklists.archetype_id`
- [ ] 1.4 Create migration script to apply schema changes to existing databases
- [ ] 1.5 Write unit tests for schema validation (table exists, columns correct, constraints enforced)

## 2. LLM Client Integration
- [ ] 2.1 Write unit tests for prompt generation with mock card data
- [ ] 2.2 Write unit tests for response parsing with various LLM outputs
- [ ] 2.3 Add `strands-agents` dependency to `pyproject.toml`
- [ ] 2.4 Create `src/etl/llm_client.py` with LLM client abstraction
- [ ] 2.5 Implement prompt formatting function (mainboard cards → JSON prompt)
- [ ] 2.6 Implement response parsing function (LLM JSON → Pydantic model)
- [ ] 2.7 Add error handling for LLM API failures (rate limits, timeouts, invalid responses)

## 3. Archetype Classification Pipeline
- [ ] 3.1 Write unit tests for classification logic with mocked LLM responses
- [ ] 3.2 Write unit tests for database query methods with test fixtures
- [ ] 3.3 Create `src/etl/archetype_pipeline.py` with `ArchetypeClassificationPipeline` class
- [ ] 3.4 Implement `get_unclassified_decklists()` method (query decklists without archetype_id)
- [ ] 3.5 Implement `get_decklist_mainboard_cards()` method (join deck_cards + cards for mainboard)
- [ ] 3.6 Implement `classify_decklist()` method (call LLM, parse response, store archetype)
- [ ] 3.7 Implement `update_decklist_archetype()` method (update decklists.archetype_id to latest)
- [ ] 3.8 Implement `classify_initial()` method (classify all unclassified or classified decks)
- [ ] 3.9 Implement `classify_incremental()` method (classify decks from tournaments since last classification)
- [ ] 3.10 Add batch processing with configurable batch size
- [ ] 3.11 Add progress logging (X/Y decks classified, success/failure counts)

## 4. CLI Integration
- [ ] 4.1 Add `archetypes` option to `--data-type` argument in `src/etl/main.py`
- [ ] 4.2 Add `load_archetypes()` function to handle archetype classification CLI command
- [ ] 4.3 Add `--batch-size` argument for archetype classification (default: 50)
- [ ] 4.4 Update CLI help text and usage examples

## 5. Prompt Engineering
- [ ] 5.1 Use `src/etl/prompts/archetype_classification.txt` for prompt templates
- [ ] 5.2 Define prompt version ID (e.g., "archetype_classification_v1")
- [ ] 5.3 Create structured JSON prompt template with instructions
- [ ] 5.4 Define Pydantic response model for validation (ArchetypeClassificationResponse)
- [ ] 5.5 Add examples/documentation for prompt structure
- [ ] 5.6 Write unit tests for prompt versioning and template rendering

## 6. Load Metadata Tracking
- [ ] 6.1 Add `load_subtype` field to `load_metadata` table (distinguish tournament/card/archetype loads)
- [ ] 6.2 Use `load_subtype` field where needed in repo
- [ ] 6.3 Update archetype pipeline to record classification timestamps in `load_metadata`
- [ ] 6.4 Implement `get_last_archetype_load_timestamp()` method
- [ ] 6.5 Write unit tests for load metadata tracking

## 7. Error Handling & Edge Cases
- [ ] 7.1 Handle decks with >10% missing card data (log error, set confidence=0)
- [ ] 7.2 Handle LLM rate limit errors (exponential backoff, retry logic)
- [ ] 7.3 Handle invalid LLM responses (log error, retry with clarification prompt)
- [ ] 7.4 Handle database transaction failures (rollback, log error)
- [ ] 7.5 Provide logging throughout code

## 8. Testing
- [ ] 8.1 Create `tests/integration/test_archetype_classification.py`
- [ ] 8.2 Test end-to-end archetype classification with real database and mock LLM
- [ ] 8.3 Test initial classification mode
- [ ] 8.4 Test incremental classification mode (classify new decks only)
- [ ] 8.5 Test handling of missing card data (cards not in `cards` table)
- [ ] 8.6 Test confidence scoring and low-confidence filtering
- [ ] 8.7 Test archetype_id updates on decklists table
- [ ] 8.8 Validate archetype_id updates on decklists table
- [ ] 8.9 Review classification accuracy for various archetypes (Aggro, Control, Combo, Ramp, Midrange)
- [ ] 8.10 Run unit tests: `pytest tests/unit/ -m unit -v`
- [ ] 8.11 Run integration tests: `pytest tests/integration/ -m integration -v`
- [ ] 8.12 Manually test initial classification on sample dataset (10 decks)
- [ ] 8.13 Manually test incremental classification on new decks
- [ ] 8.14 Monitor LLM API costs during testing
- [ ] 8.15 Fix any linter errors: `pylint src/etl/archetype_pipeline.py src/etl/llm_client.py`

## 9. Documentation
- [ ] 9.1 Update `README.md` with archetype classification CLI usage
- [ ] 9.2 Add archetype classification examples to documentation
- [ ] 9.3 Document LLM model selection and environment variables
- [ ] 9.4 Document prompt versioning and reclassification strategy
- [ ] 9.5 Update `openspec/project.md` with archetype classification conventions





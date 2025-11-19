# Implementation Tasks

## 1. Database Schema
- [x] 1.1 Add `archetypes` table to `schema.sql` with all fields (archetype_id, decklist_id, format, main_title, color_identity, strategy, archetype_confidence, llm_model, prompt_id, classified_at)
- [x] 1.2 Add `archetype_id` foreign key column to `decklists` table
- [x] 1.3 Create indexes on `archetypes.decklist_id`, `archetypes.format`, and `decklists.archetype_id`
- [x] 1.4 Create migration script to apply schema changes to existing databases
- [x] 1.5 Write unit tests for schema validation (table exists, columns correct, constraints enforced)

## 2. LLM Client Integration
- [x] 2.1 Write unit tests for prompt generation with mock card data
- [x] 2.2 Write unit tests for response parsing with various LLM outputs
- [x] 2.3 Add `langchain-core`, `langchain-openai`, `langchain-anthropic` and `langchain-aws` dependencies to `pyproject.toml`
- [x] 2.4 Create `src/etl/api_clients/llm_client.py` with LLM client abstraction
- [x] 2.5 Implement prompt formatting function (mainboard cards → JSON prompt)
- [x] 2.6 Implement response parsing function (LLM JSON → Pydantic model)
- [x] 2.7 Add error handling for LLM API failures (rate limits, timeouts, invalid responses)

## 3. Archetype Classification Pipeline
- [x] 3.1 Write unit tests for classification logic with mocked LLM responses
- [x] 3.2 Write unit tests for database query methods with test fixtures
- [x] 3.3 Create `src/etl/archetype_pipeline.py` with `ArchetypeClassificationPipeline` class extending `BasePipeline`
- [x] 3.4 Implement `get_unclassified_decklists()` method (query decklists without archetype_id)
- [x] 3.5 Implement `get_decklist_mainboard_cards()` method (join deck_cards + cards for mainboard)
- [x] 3.6 Implement `insert_archetype()` method (call LLM, parse response, store archetype)
- [x] 3.7 Implement `update_decklist_archetype()` method (update decklists.archetype_id to latest)
- [x] 3.8 Implement `load_initial()` method following BasePipeline interface (classify all unclassified or classified decks, return standardized result dict)
- [x] 3.9 Implement `load_incremental()` method following BasePipeline interface (classify decks from tournaments since last classification, return standardized result dict)
- [x] 3.10 Add batch processing with configurable batch size
- [x] 3.11 Add progress logging (X/Y decks classified, success/failure counts)
- [x] 3.12 Use `update_load_metadata()` from `src/etl/utils.py` with `data_type='archetypes'`
- [x] 3.13 Use `get_last_load_timestamp()` from `src/etl/utils.py` with `data_type='archetypes'`

## 4. CLI Integration
- [x] 4.1 Add `archetypes` option to `--data-type` argument in `src/etl/main.py`
- [x] 4.2 Add `load_archetypes()` function to handle archetype classification CLI command
- [x] 4.3 Add `--batch-size` argument for archetype classification (default: 50)
- [x] 4.4 Update CLI help text and usage examples

## 5. Prompt Engineering
- [x] 5.1 Use `src/etl/prompts/archetype_classification.txt` for prompt templates
- [x] 5.2 Define prompt version ID (e.g., "archetype_classification_v1")
- [x] 5.3 Create structured JSON prompt template with instructions
- [x] 5.4 Define Pydantic response model for validation (ArchetypeClassificationResponse)
- [x] 5.5 Add examples/documentation for prompt structure
- [x] 5.6 Write unit tests for prompt versioning and template rendering

## 6. Load Metadata Tracking
- [x] 6.1 Verify `load_metadata` table has `data_type` field (already exists in main)
- [x] 6.2 Use `data_type='archetypes'` for archetype classification loads
- [x] 6.3 Update archetype pipeline to record classification timestamps using `update_load_metadata(data_type='archetypes')`
- [x] 6.4 Use existing `get_last_load_timestamp('archetypes')` from `src/etl/utils.py`
- [x] 6.5 Write unit tests for load metadata tracking with archetype data type

## 7. Error Handling & Edge Cases
- [x] 7.1 Handle decks with >10% missing card data (log error, set confidence=0)
- [x] 7.2 Handle LLM rate limit errors (exponential backoff, retry logic)
- [x] 7.3 Handle invalid LLM responses (log error, retry with clarification prompt)
- [x] 7.4 Handle database transaction failures (rollback, log error)
- [x] 7.5 Provide logging throughout code

## 8. Testing
- [x] 8.1 Create `tests/integration/test_e2e_archetype_classification.py` that tests `src/etl/api_clients/llm_client` and `src/etl/archetype_pipeline.py` (llm_client, initial/incremental, confidence scoring, low confidence filtering, archetype_id updates on decklists table)
- [x] 8.2 Create `tests/integration/test_llm_client.py` with real small LLM calls for only AzureOpenAI (DO NOT test Anthropic, OpenAI, or AWS Bedrock)
- [x] 8.3 Test end-to-end archetype classification with real database and mock LLM
- [x] 8.4 Run unit tests: `pytest tests/unit/ -m unit -v` (148/158 passing - 93.7%)
- [x] 8.5 Run integration tests: `pytest tests/integration/ -m integration -v` (require Azure OpenAI credentials to run)
- [x] 8.6 Fix any linter errors: `pylint src/etl/archetype_pipeline.py src/etl/api_clients/llm_client.py` (no errors)

## 9. Documentation
- [ ] 9.1 Update `README.md` with archetype classification CLI usage
- [ ] 9.2 Add archetype classification examples to documentation
- [ ] 9.3 Document LLM model selection and environment variables
- [ ] 9.4 Document prompt versioning and reclassification strategy
- [ ] 9.5 Update `openspec/project.md` with archetype classification conventions





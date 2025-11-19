# Design: LLM-Based Archetype Classification

## Context
The system currently ingests tournament data (tournaments, players, decklists, matches) and card data (cards, deck_cards) but lacks archetype classification. Users need automatic categorization of decks to understand the meta landscape, track archetype trends, and receive matchup-specific coaching.

**Constraints:**
- Must run independently after tournament and card data loads (dependency on both datasets)
- Must support initial load (classify historical decks) and incremental load (classify new decks)
- Must handle multiple classifications per deck over time (e.g., reclassifying with better prompts/models)
- Must preserve LLM model and prompt version for reproducibility
- Must provide confidence scores for classification quality assessment

**Stakeholders:**
- End users: Need accurate archetype labels for meta analysis
- Data pipeline: Requires independent, idempotent ETL process
- Future LLM agent: Will use archetype data for coaching and recommendations

## Goals / Non-Goals

**Goals:**
- Provide flexible, context-aware archetype classification using LLMs
- Support versioning of classifications (multiple archetypes per deck with timestamps)
- Enable CLI-driven classification with initial/incremental modes
- Extract mainboard card details from `deck_cards` and `cards` tables for LLM analysis
- Store structured archetype metadata (format, main_title, color_identity, strategy, confidence)
- Link archetypes to decklists with latest archetype_id reference

**Non-Goals:**
- Real-time classification during tournament data ingestion (runs independently)
- Clustering or rule-based classification (LLM-only approach)
- Sideboard analysis (mainboard only for archetype identification)
- Multi-format archetype normalization (e.g., "Tron" across Modern/Legacy)
- User-facing API endpoints (CLI only for now)

## Decisions

### Decision 1: LLM Provider Abstraction
**What:** Use a unified LLM client interface that supports multiple providers (OpenAI, AzureOpenAI, Anthropic, Bedrock).

**Why:**
- Flexibility to switch providers based on cost, performance, or availability
- Future-proof against provider changes or deprecations
- Enables A/B testing of different models

**Alternatives Considered:**
- Single LLM API integration - rejected due to vendor lock-in
- Local open-source models - rejected due to complexity and infrastructure requirements
- Rule-based classification - rejected per user requirement (LLM-only approach)

**Implementation:**
- Use `strands-agents` library for unified interface across providers
- Store `llm_model` (e.g., "gpt-4o", "claude-3-5-sonnet") and `prompt_id` in `archetypes` table
- Allow model selection via environment variable or CLI parameter

### Decision 2: Archetype Schema Design
**What:** Create two-table normalized design with `archetype_groups` for unique archetype definitions and `archetype_classifications` for historical classification events.

**Why:**
- **Normalized design:** Stores unique archetype definitions (format, main_title, strategy, color_identity) once in `archetype_groups`, eliminating data duplication
- **Database-enforced uniqueness:** UNIQUE constraint ensures same archetype gets same group_id
- **Historical tracking:** `archetype_classifications` preserves all classification attempts with confidence, model, and prompt metadata
- **Simple queries:** Decklists reference `archetype_group_id` directly for current archetype without complex joins
- **Clean separation:** "What is this archetype?" (groups) vs "When/how was this classified?" (classifications)

**Schema:**
```sql
-- Unique archetype definitions
CREATE TABLE archetype_groups (
    archetype_group_id SERIAL PRIMARY KEY,
    format TEXT NOT NULL,
    main_title TEXT NOT NULL,
    color_identity TEXT,
    strategy TEXT NOT NULL CHECK (strategy IN ('aggro', 'midrange', 'control', 'ramp', 'combo')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (format, main_title, strategy, color_identity)
);

-- Historical classification events
CREATE TABLE archetype_classifications (
    classification_id SERIAL PRIMARY KEY,
    decklist_id INTEGER NOT NULL,
    archetype_group_id INTEGER NOT NULL,
    archetype_confidence FLOAT NOT NULL CHECK (archetype_confidence >= 0 AND archetype_confidence <= 1),
    llm_model TEXT NOT NULL,
    prompt_id TEXT NOT NULL,
    classified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (decklist_id) REFERENCES decklists(decklist_id) ON DELETE CASCADE,
    FOREIGN KEY (archetype_group_id) REFERENCES archetype_groups(archetype_group_id) ON DELETE CASCADE
);

-- Link decklists to current archetype
ALTER TABLE decklists ADD COLUMN archetype_group_id INTEGER 
    REFERENCES archetype_groups(archetype_group_id) ON DELETE SET NULL;
```

**Alternatives Considered:**
- Single table with group_id column - rejected due to data duplication (storing format, main_title, strategy, color_identity in every classification row)
- Versioning via JSONB column - rejected for schema clarity and queryability
- No historical tracking - rejected as we want to track confidence changes and reclassification patterns

### Decision 3: ETL Pipeline Independence
**What:** Archetype classification runs as a separate ETL step after tournament and card data loads, following the `BasePipeline` pattern.

**Why:**
- Clean separation of concerns (data ingestion vs. enrichment)
- Allows tournament and card loads to complete without LLM dependencies
- Enables reclassification without re-ingesting source data
- Supports batch processing with configurable time windows
- Follows existing ETL patterns (`TournamentsPipeline`, `CardsPipeline`)

**Implementation:**
- New `ArchetypeClassificationPipeline` class extending `BasePipeline`
- Implements `load_initial()` and `load_incremental()` methods with standardized return format
- New CLI command: `python -m src.etl.main --data-type archetypes --mode [initial|incremental]`
- Initial mode: Classify all decklists (including previously classified ones, creating new archetype rows)
- Incremental mode: Classify decks from tournaments since last archetype load
- Uses `load_metadata` table with `data_type='archetypes'` to track classification timestamps
- Leverages shared utility functions from `src/etl/utils.py` (`update_load_metadata`, `get_last_load_timestamp`)

**Alternatives Considered:**
- Inline classification during tournament load - rejected due to complexity and failure isolation
- Event-driven classification (triggers on decklist insert) - rejected as over-engineering for current scale

### Decision 4: Mainboard Card Enrichment
**What:** Join `deck_cards` (mainboard only) with `cards` table to provide LLM with card details from player deck lists.

**Why:**
- LLM needs context beyond card names (oracle text, types, costs) for accurate classification
- Mainboard cards are primary archetype indicators (sideboard varies by meta)

**Data Flow:**
```
decklists.decklist_id → deck_cards (section='mainboard') → cards → LLM prompt
```

**Enriched Fields:**
- `name`: Card name
- `oracle_text`: Rules text (key for synergy detection)
- `type_line`: Creature types, card types (e.g., "Creature — Human Wizard")
- `mana_cost`: Cost string (e.g., "{1}{R}")
- `cmc`: Converted mana cost (numeric)
- `color_identity`: Color identity array (for deck color classification)

**Alternatives Considered:**
- Include sideboard cards - rejected as sideboard varies by matchup, not archetype
- Pre-compute card embeddings - rejected as premature optimization
- Using "decklist_text" from `decklists` - deck_cards already has parsed cards attached to user decklists

### Decision 5: LLM Prompt Structure
**What:** Structured JSON prompt with mainboard cards and metadata, requesting structured JSON response.

**Why:**
- JSON input/output simplifies parsing and validation
- Structured format reduces LLM hallucination
- Enables prompt versioning via `prompt_id`

**Prompt Template:**
```json
{
  "task": "Classify MTG decklist archetype",
  "format": "Modern",
  "mainboard_cards": [
    {
      "name": "Amulet of Vigor",
      "quantity": 4,
      "type_line": "Artifact",
      "mana_cost": "{1}",
      "cmc": 1,
      "color_identity": [],
      "oracle_text": "Whenever a permanent enters the battlefield tapped and under your control, untap it."
    },
    // ... more cards
  ],
  "instructions": "" // config-driven text from archetype_classification.txt
}
```

**Expected Response:**
```json
{
  "main_title": "amulet_titan",
  "color_identity": "gruul",
  "strategy": "combo",
  "confidence": 0.95,
  "reasoning": "Deck focuses on Amulet of Vigor + bounce lands combo to ramp into Primeval Titan."
}
```

**Alternatives Considered:**
- Free-text prompt - rejected due to inconsistent LLM responses
- Pre-defined archetype list - rejected due to rigidity (meta evolves)
- Few-shot examples in prompt - deferred to future optimization if needed

## Risks / Trade-offs

### Risk 1: LLM API Costs
- **Mitigation:** Batch processing, prompt optimization, caching of classifications
- **Trade-off:** Cost vs. accuracy (cheaper models may misclassify)

### Risk 2: LLM Hallucination
- **Mitigation:** Confidence scores, manual review of low-confidence classifications, prompt engineering
- **Trade-off:** Confidence threshold filtering may exclude valid archetypes

### Risk 3: Prompt Version Drift
- **Mitigation:** Store `prompt_id` with each classification, enable reclassification with new prompts
- **Trade-off:** Multiple classifications per deck increase storage

### Risk 4: Rate Limiting
- **Mitigation:** Exponential backoff, configurable batch sizes, respect provider rate limits
- **Trade-off:** Slower classification for large initial loads

### Risk 5: Decklist Ambiguity
- **Mitigation:** Confidence scoring, human review for edge cases
- **Trade-off:** Some decks may be inherently ambiguous (e.g., hybrid strategies)

## Migration Plan

### Phase 1: Schema Migration
1. Create `archetypes` table with all fields
2. Add `archetype_id` column to `decklists` table (nullable, foreign key)
3. Create indexes on `archetypes.decklist_id` and `decklists.archetype_id`
4. Test schema changes on test database

### Phase 2: ETL Implementation
1. Implement LLM client abstraction using strands-agents classes
2. Create `ArchetypeClassificationPipeline` extending `BasePipeline` (query decks, enrich cards, classify)
3. Implement `load_initial()` and `load_incremental()` with standardized return format
4. Add CLI command for archetype classification in `main.py`
5. Use shared utility functions from `src/etl/utils.py` for metadata tracking

### Phase 3: Testing
1. Unit tests for prompt generation, response parsing, confidence scoring
2. Integration tests for end-to-end classification pipeline
3. Manual validation of sample classifications across formats

### Phase 4: Initial Load
1. Run initial classification on existing decklists (batched)
2. Monitor API costs and classification accuracy
3. Adjust prompts and confidence thresholds as needed

### Rollback Plan
- Drop `archetype_id` column from `decklists` table
- Drop `archetypes` table
- Remove archetype ETL code (no impact on existing data loads)

## Open Questions
1. **Q:** Which LLM model should be default (gpt-4o, claude-3-5-sonnet)?
   - **A:** Start with `gpt-4o-mini` for cost efficiency, allow override via `LLM_MODEL` env var

2. **Q:** Should we support reclassification of existing decks (force flag)?
   - **A:** Yes, use `--mode initial` flag to run classification on unclassified and classified decks

3. **Q:** How to handle decks with missing card data (cards not in `cards` table)?
   - **A:** Log missing cards, skip classification if >10% of mainboard cards are missing, set confidence=0

4. **Q:** Should color_identity be inferred from cards or from LLM?
   - **A:** LLM provides color_identity (e.g., "dimir", "jeskai") as human-readable label, not derived from cards

5. **Q:** Should we validate LLM responses against expected schema?
   - **A:** Yes, use Pydantic models for response validation, retry with clarification prompt if invalid


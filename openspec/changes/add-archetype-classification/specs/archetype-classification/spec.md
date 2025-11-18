# archetype-classification Specification Delta

## ADDED Requirements

### Requirement: Archetypes Table Storage
The system SHALL store archetype classifications in an `archetypes` table with the following fields:
- `archetype_id` (SERIAL, PRIMARY KEY): Unique identifier for each classification
- `decklist_id` (INTEGER, FOREIGN KEY): References `decklists.decklist_id`
- `format` (TEXT): Tournament format (e.g., "Modern", "Standard")
- `main_title` (TEXT): Archetype name based on key cards/themes (e.g., "amulet_titan", "elves", "blink")
- `color_identity` (TEXT): Human-readable color description (e.g., "dimir", "jeskai", "colorless")
- `strategy` (TEXT): One of "aggro", "midrange", "control", "ramp", or "combo" (enforced by CHECK constraint)
- `archetype_confidence` (FLOAT): Confidence score from 0 to 1 (enforced by CHECK constraint)
- `llm_model` (TEXT): LLM model used for classification (e.g., "gpt-4o", "claude-3-5-sonnet")
- `prompt_id` (TEXT): Version identifier for prompt used (e.g., "archetype_classification_v1")
- `classified_at` (TIMESTAMP): Timestamp when classification was performed

#### Scenario: Store New Archetype Classification
- **WHEN** a decklist is classified by the LLM
- **AND** the LLM returns archetype metadata (main_title, color_identity, strategy, confidence)
- **THEN** a new row is inserted into the `archetypes` table with all fields populated
- **AND** the `archetype_id` is returned for updating the decklist

#### Scenario: Support Multiple Classifications Per Deck
- **WHEN** a decklist is reclassified (e.g., with a new prompt or model)
- **THEN** a new row is inserted into the `archetypes` table with a new `archetype_id`
- **AND** the previous classification remains in the table for historical tracking
- **AND** the `decklists.archetype_id` is updated to reference the latest classification

#### Scenario: Enforce Strategy Constraint
- **WHEN** an archetype classification is stored with an invalid strategy value
- **THEN** the database rejects the insert with a CHECK constraint violation
- **AND** only "aggro", "midrange", "control", "ramp", or "combo" are accepted

#### Scenario: Enforce Confidence Range
- **WHEN** an archetype classification is stored with a confidence score < 0 or > 1
- **THEN** the database rejects the insert with a CHECK constraint violation
- **AND** only values between 0 and 1 (inclusive) are accepted

### Requirement: Decklist Archetype Linking
The system SHALL link decklists to their latest archetype classification via a foreign key.

#### Scenario: Handle Archetype Deletion
- **WHEN** an archetype row is deleted from the `archetypes` table
- **THEN** the corresponding `decklists.archetype_id` is set to NULL (ON DELETE SET NULL)
- **AND** the decklist row is not deleted

### Requirement: Mainboard Card Enrichment
The system SHALL extract and enrich mainboard card data from decklists for LLM analysis.

#### Scenario: Query Mainboard Cards for Decklist
- **WHEN** a decklist is selected for archetype classification
- **THEN** the system queries `deck_cards` table for cards with `section='mainboard'`
- **AND** joins with the `cards` table to retrieve card details (name, oracle_text, type_line, mana_cost, cmc, color_identity)
- **AND** returns a structured list of mainboard cards with quantities

#### Scenario: Handle Missing Card Data
- **WHEN** a mainboard card in `deck_cards` does not have a matching entry in the `cards` table
- **THEN** the missing card is logged with card_id and decklist_id
- **AND** the card is excluded from the enriched card list

#### Scenario: Filter Sideboard Cards
- **WHEN** querying cards for archetype classification
- **THEN** only cards with `section='mainboard'` are included
- **AND** cards with `section='sideboard'` are excluded from LLM analysis

### Requirement: LLM-Based Classification
The system SHALL use LLM APIs to classify decklists into archetypes based on mainboard card data.

#### Scenario: Generate Classification Prompt
- **WHEN** a decklist's mainboard cards are retrieved
- **THEN** the system uses a developer-supplied structured JSON prompt containing:
  - Tournament format
  - List of mainboard cards with: name, quantity, type_line, mana_cost, cmc, color_identity, oracle_text
  - Instructions for LLM to identify main_title, color_identity, strategy, and confidence
- **AND** the prompt includes the current `prompt_id` version identifier

#### Scenario: Parse LLM Classification Response
- **WHEN** the LLM returns a classification response
- **THEN** the system validates the response against a Pydantic model (ArchetypeClassificationResponse)
- **AND** extracts main_title, color_identity, strategy, and confidence fields
- **AND** if the response is invalid, the system retries once with a clarification prompt 
- **AND** if retry fails, the classification is logged as failed and skipped

#### Scenario: Handle LLM API Errors
- **WHEN** the LLM API returns an error (rate limit, timeout, invalid request)
- **THEN** the system logs the error with decklist_id and error details
- **AND** implements exponential backoff for rate limit errors (up to 3 retries)
- **AND** skips the decklist if all retries fail

#### Scenario: Store Classification Metadata
- **WHEN** a classification is successfully parsed from LLM response
- **THEN** the system stores the `llm_model` used (e.g., "gpt-4o-mini")
- **AND** stores the `prompt_id` version identifier
- **AND** records the `classified_at` timestamp

### Requirement: Archetype Classification Pipeline
The system SHALL provide an independent ETL pipeline for archetype classification that runs after tournament and card data loads, following the `BasePipeline` abstract class pattern.

#### Scenario: Incremental Classification Mode
- **WHEN** the archetype classification pipeline runs in incremental mode
- **THEN** the system queries the last archetype classification timestamp from `load_metadata` using `get_last_load_timestamp('archetypes')`
- **AND** retrieves all decklists from tournaments with `start_date >= last_timestamp`
- **AND** classifies each new or updated decklist in batches
- **AND** updates the load timestamp in `load_metadata` table using `update_load_metadata(data_type='archetypes')`
- **AND** returns a standardized result dictionary with keys: `success` (bool), `objects_loaded` (int), `objects_processed` (int), `errors` (int)

#### Scenario: Run Initial Archetype Classification
- **WHEN** the archetype classification pipeline runs with `--mode initial` flag
- **THEN** the system queries all decklists (regardless of existing `archetype_id`)
- **AND** creates new archetype classifications for each decklist
- **AND** updates `decklists.archetype_id` to reference the new classifications
- **AND** preserves previous archetype rows for historical tracking
- **AND** logs progress and completion stats (X/Y decks classified, success/failure counts)
- **AND** records the load timestamp in `load_metadata` table
- **AND** returns a standardized result dictionary with keys: `success` (bool), `objects_loaded` (int), `objects_processed` (int), `errors` (int)

#### Scenario: Batch Processing
- **WHEN** the archetype classification pipeline processes decklists
- **THEN** the system processes decks in configurable batches (default: 50)
- **AND** commits each batch to the database upon successful classification
- **AND** continues processing remaining batches if one batch fails

### Requirement: CLI Command for Archetype Classification
The system SHALL provide CLI commands for archetype classification with mode and parameter options.

#### Scenario: Run Incremental Archetype Classification
- **WHEN** the user runs `python -m src.etl.main --data-type archetypes --mode incremental`
- **THEN** the system classifies decklists from tournaments since last archetype load
- **AND** logs progress and completion statistics
- **AND** returns exit code 0 on success

#### Scenario: Force Reclassification
- **WHEN** the user runs `python -m src.etl.main --data-type archetypes --force-reclassify`
- **THEN** the system reclassifies all decklists (creates new archetype rows)
- **AND** updates decklists to reference the new archetype classifications
- **AND** logs progress and completion statistics

#### Scenario: Configure Batch Size
- **WHEN** the user runs `python -m src.etl.main --data-type archetypes --batch-size 100`
- **THEN** the system processes decklists in batches of 100
- **AND** commits each batch to the database upon completion

### Requirement: Load Metadata Tracking for Archetypes
The system SHALL track archetype classification loads in the `load_metadata` table to enable incremental processing using the existing `data_type` field.

#### Scenario: Record Archetype Load Timestamp
- **WHEN** an archetype classification pipeline run completes successfully
- **THEN** the system inserts a new row into `load_metadata` with:
  - `last_load_timestamp`: Unix timestamp of the latest tournament processed
  - `load_type`: "incremental" or "initial"
  - `data_type`: "archetypes" (distinguishes from tournament/card loads)
  - `objects_loaded`: Count of decklists classified in this batch
- **AND** the load timestamp is used for future incremental runs
- **AND** uses `update_load_metadata()` utility function from `src/etl/utils.py`

#### Scenario: Query Last Archetype Load Timestamp
- **WHEN** the archetype classification pipeline runs in incremental mode
- **THEN** the system queries `load_metadata` for the most recent row with `data_type='archetypes'`
- **AND** retrieves the `last_load_timestamp` value using `get_last_load_timestamp('archetypes')` from `src/etl/utils.py`
- **AND** uses this timestamp to filter tournaments for incremental classification

### Requirement: Confidence-Based Filtering
The system SHALL support filtering and validation of archetype classifications based on confidence scores.

#### Scenario: Log Low-Confidence Classifications
- **WHEN** a classification has confidence < 0.5
- **THEN** the system logs a warning with decklist_id, archetype, and confidence score
- **AND** stores the classification in the database with the `main_title` as "other"

#### Scenario: Skip Classification on Missing Data
- **WHEN** >10% of a decklist's mainboard cards are missing from the `cards` table
- **THEN** the system skips LLM classification for that decklist
- **AND** logs an error with decklist_id and missing card count
- **AND** does not create an archetype row for that decklist


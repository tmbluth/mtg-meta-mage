# Card Data Management Specification

## ADDED Requirements

### Requirement: Cards Table Storage
The system SHALL store Scryfall oracle card data in a `cards` table with the following fields:
- `card_id` (TEXT, PRIMARY KEY): Scryfall UUID from the `id` field
- `set` (TEXT): Set code (e.g., "MH2")
- `collector_num` (TEXT): Collector number within the set
- `name` (TEXT): Card name
- `oracle_text` (TEXT): Full rules text
- `rulings` (TEXT): Comma-concatenated rulings from Scryfall rulings bulk data
- `type_line` (TEXT): Supertype(s), type(s), and subtype(s)
- `mana_cost` (TEXT): Cost as parsed by Scryfall
- `cmc` (FLOAT): Converted mana cost
- `color_identity` (TEXT[]): Color identity as array (e.g., ["U", "R"])
- `scryfall_uri` (TEXT): Link to Scryfall card page

#### Scenario: Load Oracle Cards from Scryfall Bulk Data
- **WHEN** Scryfall oracle cards bulk data is downloaded
- **AND** the data is processed and loaded into the database
- **THEN** each card object is inserted into the `cards` table with all required fields populated
- **AND** duplicate cards (same `card_id`) are handled via upsert logic

#### Scenario: Join Cards with Rulings
- **WHEN** Scryfall rulings bulk data is downloaded
- **AND** rulings are joined with cards by `oracle_id`
- **THEN** rulings for each card are concatenated with commas and stored in the `rulings` field
- **AND** cards without rulings have an empty `rulings` field

### Requirement: Deck Cards Junction Table
The system SHALL store individual card entries from decklists in a `deck_cards` table with the following fields:
- `decklist_id` (INTEGER, FOREIGN KEY): References `decklists.decklist_id`
- `card_id` (TEXT, FOREIGN KEY): References `cards.card_id`
- `section` (TEXT): Either "mainboard" or "sideboard"
- `quantity` (INTEGER): Number of copies of this card in the deck

#### Scenario: Parse Decklist and Store Cards
- **WHEN** a decklist is processed from `decklist_text`
- **AND** the decklist is parsed to extract card names and quantities
- **THEN** each card is matched to the `cards` table by name
- **AND** entries are created in `deck_cards` with correct `decklist_id`, `card_id`, `section`, and `quantity`
- **AND** mainboard and sideboard sections are correctly identified

#### Scenario: Handle Missing Cards
- **WHEN** a card name in a decklist does not exist in the `cards` table
- **THEN** the missing card is logged for review
- **AND** processing continues for remaining cards
- **AND** no entry is created in `deck_cards` for the missing card

### Requirement: Decklist Parsing
The system SHALL parse standard MTG decklist text format to extract:
- Card quantities (numeric prefix)
- Card names (text after quantity)
- Section identification (mainboard vs sideboard)

#### Scenario: Parse Standard Decklist Format
- **WHEN** a decklist text contains lines in format "4 Lightning Bolt" or "2 Mountain"
- **THEN** the parser extracts quantity (4, 2) and card name ("Lightning Bolt", "Mountain")
- **AND** cards before a sideboard separator are marked as "mainboard"
- **AND** cards after a sideboard separator are marked as "sideboard"

#### Scenario: Handle Edge Cases in Decklist Parsing
- **WHEN** a decklist contains empty lines, comments, or malformed entries
- **THEN** the parser skips invalid lines and logs warnings
- **AND** valid cards are still extracted and processed

### Requirement: Scryfall Bulk Data Integration
The system SHALL download and process Scryfall bulk data files:
- Oracle Cards bulk data (type: "oracle_cards")
- Rulings bulk data (type: "rulings")

#### Scenario: Download Scryfall Bulk Data
- **WHEN** the system requests Scryfall bulk data
- **THEN** the current download URL is fetched from `/bulk-data` endpoint
- **AND** the bulk data file is downloaded and parsed as JSON
- **AND** the data is returned in a structured format ready for database insertion

#### Scenario: Process Bulk Data for Database
- **WHEN** Scryfall bulk data is downloaded
- **THEN** card objects are transformed to match the `cards` table schema
- **AND** rulings are joined with cards by `oracle_id`
- **AND** the data is ready for batch insertion into the database


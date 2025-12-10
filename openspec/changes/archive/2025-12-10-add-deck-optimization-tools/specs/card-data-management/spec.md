## ADDED Requirements

### Requirement: Card Legality Data Storage
The system SHALL store format legality information for each card in the cards table to enable accurate card filtering for deck optimization.

#### Scenario: Store legalities as JSONB
- **WHEN** cards are loaded from Scryfall bulk data
- **THEN** the cards table includes a legalities JSONB column
- **AND** the legalities field stores format:status mappings (e.g., {"modern": "legal", "standard": "not_legal"})
- **AND** the field supports efficient JSON queries with -> and ->> operators
- **AND** an index is created on legalities for query performance

#### Scenario: ETL pipeline captures legalities
- **WHEN** the CardsPipeline transforms Scryfall card data
- **THEN** it extracts the legalities field from the Scryfall card object
- **AND** stores it as JSONB in the database
- **AND** handles missing legalities field gracefully (default to empty JSON object)

#### Scenario: Query cards by format legality
- **WHEN** optimization tools need to find legal cards
- **THEN** they normalize the format parameter to match database format (lowercase)
- **AND** query: `WHERE legalities->>'normalized_format' = 'legal'`
- **AND** the query returns only cards legal in that format
- **AND** cards with banned status are excluded
- **AND** cards with restricted status are included with appropriate warnings


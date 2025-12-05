## ADDED Requirements

### Requirement: Mainboard Optimization
The system SHALL provide an MCP tool that optimizes a user's maindeck by identifying flex spots (non-critical cards) and recommending replacement cards that improve performance against the top N most frequent archetypes.

#### Scenario: Optimize mainboard against top 5 archetypes
- **WHEN** `optimize_mainboard` is called with card_details, archetype, format, and top_n=5
- **THEN** the tool calls `get_format_meta_rankings` to retrieve the top 5 archetypes by meta share
- **AND** for each of the top 5 archetypes, queries the database for up to 5 most recent decklists
- **AND** parses the decklist data to extract mainboard card lists
- **AND** sends the user's maindeck card list and sample decklists from top 5 archetypes to the LLM with mainboard optimization prompt
- **AND** returns identified flex spots (cards not crucial for deck consistency/synergy)
- **AND** returns recommended replacement cards with justifications
- **AND** justifications explain why those cards improve matchups against top N archetypes based on actual cards played
- **AND** justifications explain why other common alternatives were not recommended

#### Scenario: Handle empty or insufficient meta data
- **WHEN** `optimize_mainboard` is called but `get_format_meta_rankings` returns no archetypes
- **THEN** the tool returns an error message indicating insufficient meta data
- **AND** does not attempt LLM analysis

#### Scenario: Fetch recent decklists for top archetypes
- **WHEN** the tool retrieves top N archetypes
- **THEN** it queries the decklists table for each archetype_group_id
- **AND** orders by tournament start_date descending
- **AND** limits to 5 most recent decklists per archetype
- **AND** joins with deck_cards table to get card details for each decklist
- **AND** filters for mainboard cards only

#### Scenario: Mainboard optimization considers deck synergy
- **WHEN** the LLM analyzes a decklist for mainboard optimization
- **THEN** it identifies cards that are not core to the deck's strategy or synergy
- **AND** preserves cards that are essential for consistency (mana base, key combo pieces, core threats)
- **AND** considers the user's archetype when determining what is "core" vs "flex"

#### Scenario: Filter legal cards before LLM analysis
- **WHEN** the tool prepares to send data to the LLM
- **THEN** it queries the cards table for all cards where legalities->>'format' = 'legal'
- **AND** further filters by color identity matching the user's deck
- **AND** includes colorless cards in the filtered list
- **AND** includes cards with alternative mana costs (phyrexian mana, generic costs)
- **AND** only sends this filtered card list to the LLM in the prompt
- **AND** the tool returns an error if card legality data is unavailable

#### Scenario: Color identity filtering considers deck colors
- **WHEN** filtering cards by color identity
- **THEN** the tool analyzes the user's deck to determine its color identity
- **AND** includes cards where color_identity is a subset of the deck's colors
- **AND** always includes colorless cards (empty color_identity array)
- **AND** includes cards with phyrexian mana or hybrid costs that could be cast with available colors
- **AND** excludes cards requiring colors not present in the deck

### Requirement: Sideboard Optimization
The system SHALL provide an MCP tool that optimizes a user's sideboard to better answer the top N most frequent archetypes, considering post-sideboard games.

#### Scenario: Optimize sideboard against top 10 archetypes
- **WHEN** `optimize_sideboard` is called with card_details, archetype, format, and top_n=10
- **THEN** the tool calls `get_format_meta_rankings` to retrieve the top 10 archetypes by meta share
- **AND** for each of the top 10 archetypes, queries the database for up to 5 most recent decklists
- **AND** parses the decklist data to extract mainboard and sideboard card lists
- **AND** sends the user's current sideboard and sample decklists from top 10 archetypes to the LLM with sideboard optimization prompt
- **AND** returns recommended sideboard additions, removals, or replacements
- **AND** recommendations consider what opponents will sideboard in games 2 and 3 based on actual sideboard cards observed
- **AND** recommendations explain how each card answers specific threats from top N archetypes


#### Scenario: Sideboard optimization considers post-board opponent plans
- **WHEN** the LLM analyzes sideboard optimization
- **THEN** it considers what common sideboard cards opponents bring in against the user's archetype based on observed decklists
- **AND** analyzes actual sideboard cards from the fetched decklists
- **AND** recommends cards that answer those opposing sideboard plans
- **AND** explains how the sideboard plan changes for games 2 and 3 against each top N archetype

### Requirement: Card Legality and Color Identity Filtering
The system SHALL provide a function that filters the card pool to only legal, color-appropriate cards before sending to the LLM for optimization recommendations.

#### Scenario: Query legal cards for format
- **WHEN** the optimization tool needs to filter cards
- **THEN** it queries cards table: `SELECT * FROM cards WHERE legalities->>'format' = 'legal'`
- **AND** the legalities field is a JSONB column containing format:status mappings
- **AND** returns all cards legal in the specified format

#### Scenario: Filter by deck color identity
- **WHEN** the tool has a list of legal cards and the user's deck
- **THEN** it determines the deck's color identity from the cards in the deck
- **AND** filters to cards where card.color_identity is a subset of deck_colors
- **AND** includes all colorless cards (color_identity = [])
- **AND** includes cards with phyrexian mana symbols that can be paid with life
- **AND** includes cards with generic mana costs only
- **AND** returns this filtered list for LLM context

#### Scenario: Include alternative cost cards
- **WHEN** filtering by color identity
- **THEN** the tool checks mana_cost field for phyrexian mana symbols ({W/P}, {U/P}, etc.)
- **AND** includes these cards even if their color isn't in the deck's identity
- **AND** checks for cards with only generic mana costs ({1}, {2}, {X}, etc.)
- **AND** includes these colorless-castable cards in the filtered list

### Requirement: Optimization Prompt Templates
The system SHALL provide prompt templates for mainboard optimization and sideboard optimization that guide the LLM to produce actionable recommendations.

#### Scenario: Load mainboard optimization prompt
- **WHEN** `optimize_mainboard` tool formats the LLM prompt
- **THEN** the prompt template includes placeholders for: deck_summary, archetype, top_n_archetype_decklists, legal_card_pool, format
- **AND** the top_n_archetype_decklists includes actual card lists from recent decklists
- **AND** the legal_card_pool includes only format-legal cards matching the deck's color identity
- **AND** the prompt instructs the LLM to identify non-essential cards (flex spots)
- **AND** the prompt requests structured output with flex spots and recommended replacements
- **AND** the prompt requests justifications for each recommendation based on observed cards in meta decklists
- **AND** the prompt instructs the LLM to only suggest cards from the provided legal_card_pool

#### Scenario: Load sideboard optimization prompt
- **WHEN** `optimize_sideboard` tool formats the LLM prompt
- **THEN** the prompt template includes placeholders for: deck_summary, archetype, top_n_archetype_decklists, current_sideboard, legal_card_pool, format
- **AND** the top_n_archetype_decklists includes actual mainboard and sideboard card lists from recent decklists
- **AND** the legal_card_pool includes only format-legal cards matching the deck's color identity
- **AND** the prompt instructs the LLM to optimize against top N archetypes based on observed cards
- **AND** the prompt requests consideration of opponent sideboard plans based on actual sideboard cards
- **AND** the prompt requests structured output with additions, removals, and justifications
- **AND** the prompt instructs the LLM to only suggest cards from the provided legal_card_pool


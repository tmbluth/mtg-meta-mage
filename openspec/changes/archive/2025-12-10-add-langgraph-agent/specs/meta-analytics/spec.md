## ADDED Requirements

### Requirement: Format Archetypes Listing Tool
The system SHALL provide a `get_format_archetypes` MCP tool that returns all archetypes for a given format with metadata suitable for dropdown display.

#### Scenario: Get archetypes for valid format
- **WHEN** `get_format_archetypes` is called with format="Modern"
- **THEN** return JSON with format and archetypes array
- **AND** each archetype includes id (archetype_group_id), name (main_title), meta_share, color_identity
- **AND** archetypes are sorted by meta_share descending

#### Scenario: Get archetypes with no data
- **WHEN** `get_format_archetypes` is called with format that has no archetypes
- **THEN** return empty archetypes array
- **AND** include format in response for confirmation

#### Scenario: Archetypes derived from tournaments
- **WHEN** `get_format_archetypes` is called
- **THEN** archetypes are queried from archetype_groups table
- **AND** meta_share is calculated from decklists in last 30 days
- **AND** only archetypes with at least one decklist are returned


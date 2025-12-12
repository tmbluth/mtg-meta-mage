## ADDED Requirements

### Requirement: Streamlit Frontend Application
The system SHALL provide a Streamlit-based frontend UI at `src/app/ui/app.py` that communicates with the Agent API.

#### Scenario: Page load calls welcome endpoint
- **WHEN** user opens the Streamlit app
- **THEN** call GET /welcome to create a session
- **AND** display the LLM-generated welcome message in chat
- **AND** store conversation_id in session state
- **AND** show format dropdown populated from available_formats

#### Scenario: Chat hidden until format selected
- **WHEN** format dropdown has no selection
- **THEN** chat input is hidden
- **WHEN** user selects a format
- **THEN** chat input is revealed
- **AND** meta research is enabled

#### Scenario: Meta research controls displayed after format
- **WHEN** user selects a format
- **THEN** show current_days slider (7-90, default 14)
- **AND** show previous_days slider (7-90, default 14)
- **AND** user can immediately ask meta research questions

#### Scenario: Deck coaching panel revealed on intent
- **WHEN** user expresses deck coaching/optimization intent via chat
- **THEN** agent response explains requirements (archetype + deck + current_days)
- **AND** UI reveals deck coaching panel with:
  - Archetype dropdown (populated from GET /archetypes?format=X)
  - Decklist textarea (prepopulated with "MAINBOARD\n\nSIDEBOARD\n")
  - Meta days slider (7-90, default 14)
  - Submit Deck button

#### Scenario: Deck coaching disabled until requirements met
- **WHEN** deck coaching panel is visible
- **AND** archetype OR decklist OR current_days is missing
- **THEN** deck coaching chat requests are blocked
- **WHEN** all requirements are met (archetype + decklist + current_days)
- **THEN** deck coaching is enabled

#### Scenario: SSE streaming handled in chat
- **WHEN** POST /chat returns SSE stream
- **THEN** thinking events display as status indicator
- **AND** tool_call events display as expandable details
- **AND** content events stream into chat response
- **AND** state events update session state
- **AND** done event completes the response

#### Scenario: Session state synchronized from SSE
- **WHEN** state SSE event is received
- **THEN** update session state with has_deck, format, archetype, current_days
- **AND** UI reflects updated state


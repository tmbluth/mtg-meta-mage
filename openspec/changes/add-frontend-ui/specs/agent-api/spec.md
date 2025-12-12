## MODIFIED Requirements

### Requirement: Conversation State Persistence
The system SHALL persist conversation state within a session, allowing state to be modified and preserved across turns.

#### Scenario: Persist welcome info from session initialization
- **WHEN** GET /welcome creates a new conversation
- **THEN** tool_catalog is stored in conversation state
- **AND** available_formats is stored in conversation state
- **AND** workflows is stored in conversation state
- **AND** this data persists for all subsequent /chat calls

#### Scenario: Persist format selection
- **WHEN** user selects format via context or message
- **THEN** format is stored in conversation state
- **AND** format persists across all subsequent turns
- **AND** user can change format by selecting new value

#### Scenario: Persist current_days and previous_days preference
- **WHEN** user specifies current_days or previous_days via context
- **THEN** current_days and previous_days are stored in conversation state
- **AND** current_days is used by meta research tools and deck matchup tools
- **AND** previous_days is used by get_format_meta_rankings for period comparison
- **AND** user can adjust these values in subsequent turns

#### Scenario: Persist archetype selection
- **WHEN** user selects archetype after providing deck
- **THEN** archetype is stored in conversation state
- **AND** archetype is used by get_deck_matchup_stats, generate_deck_matchup_strategy, optimize_mainboard, optimize_sideboard
- **AND** archetype can be changed by user selection

#### Scenario: Persist card_details from enriched deck
- **WHEN** get_enriched_deck is called successfully
- **THEN** card_details is stored in conversation state
- **AND** card_details persists until new deck is provided
- **AND** card_details is used by generate_deck_matchup_strategy, optimize_mainboard, optimize_sideboard

#### Scenario: Persist matchup_stats
- **WHEN** get_deck_matchup_stats is called successfully
- **THEN** matchup_stats is stored in conversation state
- **AND** matchup_stats can be refreshed by calling get_deck_matchup_stats again
- **AND** matchup_stats is used by generate_deck_matchup_strategy

### Requirement: Blocking Dependency Enforcement
The system SHALL enforce blocking dependencies by prompting users for missing required inputs before executing MCP tools.

#### Scenario: Block all tools without format
- **WHEN** user attempts any operation without format set
- **THEN** agent prompts user to select format from dropdown
- **AND** does NOT call any MCP tools
- **AND** first interaction should establish format selection

#### Scenario: Block deck coaching without card_details
- **WHEN** LLM determines optimize_mainboard, optimize_sideboard, or generate_deck_matchup_strategy are needed
- **AND** card_details is not set (get_enriched_deck not called)
- **THEN** agent prompts user to provide deck text first
- **AND** does NOT call the requested tool

#### Scenario: Block deck coaching without archetype
- **WHEN** user requests deck coaching operations
- **AND** archetype is not set
- **AND** deck has been provided (card_details exists)
- **THEN** agent prompts user to select archetype from dropdown
- **AND** does NOT proceed until archetype is set

#### Scenario: Block deck coaching without current_days
- **WHEN** user requests deck coaching operations
- **AND** current_days is not set
- **THEN** agent prompts user to set the meta days slider
- **AND** does NOT proceed until current_days is provided

#### Scenario: Block matchup strategy without matchup_stats
- **WHEN** user requests generate_deck_matchup_strategy
- **AND** matchup_stats is not set
- **THEN** agent calls get_deck_matchup_stats first
- **OR** prompts user to get matchup stats if archetype missing

#### Scenario: Block meta tools without current_days preference
- **WHEN** user requests get_format_meta_rankings or get_format_matchup_stats
- **AND** current_days parameter is not provided by user
- **THEN** agent prompts user to specify time window preference
- **AND** does NOT use default value without user input

### Requirement: Chat Endpoint
The system SHALL provide a POST /chat endpoint that accepts user messages with optional context and returns SSE streamed responses with LLM-interpreted natural language content.

#### Scenario: Valid chat request
- **WHEN** POST /chat is called with message "What's the Modern meta?"
- **AND** context includes format: "Modern", current_days: 30, previous_days: 30
- **THEN** return SSE stream with metadata, tool_call, content, state, done events
- **AND** return Content-Type: text/event-stream
- **AND** content events contain LLM-generated natural language responses

#### Scenario: Chat uses welcome info from session
- **WHEN** POST /chat is called with a conversation_id from /welcome
- **THEN** the agent has access to tool_catalog, available_formats, workflows from session state
- **AND** uses this context when generating responses

#### Scenario: Chat request with deck text
- **WHEN** POST /chat is called with context.deck_text containing deck list
- **THEN** agent calls get_enriched_deck with provided deck
- **AND** stores card_details in conversation state
- **AND** LLM interprets enriched deck data and responds naturally
- **AND** prompts for archetype selection if not provided

#### Scenario: Chat response is natural language
- **WHEN** MCP tools return structured data during chat
- **THEN** the LLM interprets the data
- **AND** generates conversational response explaining findings
- **AND** user does not see raw JSON or tool output

#### Scenario: Invalid message (empty)
- **WHEN** POST /chat is called with empty message
- **THEN** return 400 Bad Request
- **AND** include error message in response

### Requirement: SSE Streaming Response
The system SHALL stream responses via Server-Sent Events with distinct event types for metadata, thinking, tool calls, content, state updates, and completion.

#### Scenario: Stream metadata event
- **WHEN** POST /chat processing begins
- **THEN** emit SSE event with type "metadata"
- **AND** include conversation_id, format, archetype in data payload

#### Scenario: Stream thinking event
- **WHEN** agent is reasoning about user request
- **THEN** emit SSE event with type "thinking"
- **AND** include reasoning content in data payload

#### Scenario: Stream tool call events
- **WHEN** agent calls an MCP tool
- **THEN** emit SSE event with type "tool_call" and status "calling"
- **AND** include tool name and arguments
- **WHEN** tool execution completes
- **THEN** emit SSE event with type "tool_call" and status "complete"
- **AND** include summary of results

#### Scenario: Stream content events
- **WHEN** agent generates response text
- **THEN** emit SSE events with type "content"
- **AND** include text chunks incrementally
- **AND** chunks are delivered as they become available

#### Scenario: Stream state event
- **WHEN** response is nearly complete
- **THEN** emit SSE event with type "state"
- **AND** include has_deck, format, archetype, current_days for UI state sync

#### Scenario: Stream done event
- **WHEN** response is complete
- **THEN** emit SSE event with type "done"
- **AND** close SSE connection

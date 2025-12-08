## ADDED Requirements

### Requirement: LangGraph Workflow Orchestration
The system SHALL provide a LangGraph-based workflow that orchestrates MCP tools for meta research and deck coaching, with state management and intent-based routing.

#### Scenario: Initialize new conversation
- **WHEN** POST /chat is called with `conversation_id: null`
- **THEN** create a new conversation with unique ID
- **AND** initialize empty state for format, days, archetype, card_details, matchup_stats
- **AND** return conversation_id in metadata SSE event

#### Scenario: Resume existing conversation
- **WHEN** POST /chat is called with valid `conversation_id`
- **THEN** load existing conversation state
- **AND** preserve format, archetype, days, card_details, matchup_stats from prior turns
- **AND** append new message to conversation history

#### Scenario: Route to meta research workflow
- **WHEN** user message mentions "the meta", "top decks", "format overview"
- **AND** user does NOT mention "my deck" or provide deck text
- **THEN** route to meta research workflow
- **AND** set current_workflow to "meta_research"

#### Scenario: Route to deck coaching workflow
- **WHEN** user message mentions "my deck", "my sideboard"
- **OR** user provides deck list text
- **OR** user asks for optimization or coaching advice
- **THEN** route to deck coaching workflow
- **AND** set current_workflow to "deck_coaching"

#### Scenario: Interleave workflows
- **WHEN** user switches from meta research to deck coaching (or vice versa)
- **THEN** preserve all state from prior workflow
- **AND** use meta results to inform archetype/opponent suggestions
- **AND** allow seamless transition without losing context

### Requirement: MCP Client via langchain-mcp-adapters
The system SHALL call MCP tools using `langchain-mcp-adapters` `MultiServerMCPClient` with `streamable_http` transport, configured for the meta/deck MCP server.

#### Scenario: Initialize MCP client
- **WHEN** the agent prepares to call any MCP tool
- **THEN** it constructs a `MultiServerMCPClient` from `langchain-mcp-adapters`
- **AND** uses `transport: "streamable_http"` pointing to the MCP server URL

#### Scenario: Invoke MCP tools via adapter
- **WHEN** get_format_meta_rankings, get_format_matchup_stats, get_format_archetypes, get_enriched_deck, get_deck_matchup_stats, generate_deck_matchup_strategy, optimize_mainboard, or optimize_sideboard are needed
- **THEN** the agent calls them through the MCP adapter client
- **AND** handles tool_call events reflecting the adapter invocation status

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
- **AND** include has_deck, format, archetype, days for UI state sync

#### Scenario: Stream done event
- **WHEN** response is complete
- **THEN** emit SSE event with type "done"
- **AND** close SSE connection

### Requirement: Blocking Dependency Enforcement
The system SHALL enforce blocking dependencies by prompting users for missing required inputs before executing MCP tools.

#### Scenario: Block all tools without format
- **WHEN** user attempts any operation without format set
- **THEN** agent prompts user to select format from dropdown
- **AND** does NOT call any MCP tools
- **AND** first interaction should establish format selection

#### Scenario: Block deck coaching without card_details
- **WHEN** user requests optimize_mainboard, optimize_sideboard, or generate_deck_matchup_strategy
- **AND** card_details is not set (get_enriched_deck not called)
- **THEN** agent prompts user to provide deck text first
- **AND** does NOT call the requested tool

#### Scenario: Block deck coaching without archetype
- **WHEN** user requests deck coaching operations
- **AND** archetype is not set
- **AND** deck has been provided (card_details exists)
- **THEN** agent prompts user to select archetype from dropdown
- **AND** does NOT proceed until archetype is set

#### Scenario: Block matchup strategy without matchup_stats
- **WHEN** user requests generate_deck_matchup_strategy
- **AND** matchup_stats is not set
- **THEN** agent calls get_deck_matchup_stats first
- **OR** prompts user to get matchup stats if archetype missing

#### Scenario: Block meta tools without days preference
- **WHEN** user requests get_format_meta_rankings or get_format_matchup_stats
- **AND** days parameter is not provided by user
- **THEN** agent prompts user to specify time window preference
- **AND** does NOT use default value without user input

### Requirement: Chat Endpoint
The system SHALL provide a POST /chat endpoint that accepts user messages with optional context and returns SSE streamed responses.

#### Scenario: Valid chat request
- **WHEN** POST /chat is called with message "What's the Modern meta?"
- **AND** context includes format: "Modern", days: 30
- **THEN** return SSE stream with metadata, tool_call, content, state, done events
- **AND** return Content-Type: text/event-stream

#### Scenario: Chat request with deck text
- **WHEN** POST /chat is called with context.deck_text containing deck list
- **THEN** agent calls get_enriched_deck with provided deck
- **AND** stores card_details in conversation state
- **AND** prompts for archetype selection if not provided

#### Scenario: Invalid message (empty)
- **WHEN** POST /chat is called with empty message
- **THEN** return 400 Bad Request
- **AND** include error message in response

### Requirement: Welcome Discovery Endpoint
The system SHALL provide a GET /welcome endpoint that dynamically retrieves available tools from the MCP server and presents them with workflows, formats, and example queries before a conversation is started.

#### Scenario: Get welcome information
- **WHEN** GET /welcome is called
- **THEN** return JSON with message, available_formats, workflows, and tool_count
- **AND** dynamically fetch tool catalog from MCP server using get_tool_catalog_safe
- **AND** enrich workflows with tool details including name and description

#### Scenario: Welcome response includes workflows
- **WHEN** GET /welcome succeeds
- **THEN** response includes workflows array with meta_research and deck_coaching
- **AND** each workflow includes name, description, example_queries, and tool_details
- **AND** tool_details are dynamically populated from MCP server catalog with name and description for each tool

#### Scenario: Welcome response includes formats
- **WHEN** GET /welcome succeeds
- **THEN** response includes available_formats array from tournaments table
- **AND** formats are sorted alphabetically
- **AND** only include constructed, non-Commander formats

#### Scenario: Welcome response handles MCP server unavailable
- **WHEN** GET /welcome is called and MCP server is unavailable
- **THEN** tool_details arrays are empty but workflows are still returned
- **AND** tool_count is 0
- **AND** available_formats and workflow structure are still present

### Requirement: Formats Dropdown Endpoint
The system SHALL provide a GET /formats endpoint that returns available tournament formats for dropdown selection.

#### Scenario: Get available formats
- **WHEN** GET /formats is called
- **THEN** return JSON with formats array
- **AND** formats are derived from tournaments table (not hardcoded)
- **AND** only include constructed, non-Commander formats

#### Scenario: Formats response schema
- **WHEN** GET /formats succeeds
- **THEN** response includes `{"formats": ["Modern", "Pioneer", ...]}`
- **AND** formats are sorted alphabetically

### Requirement: Archetypes Dropdown Endpoint
The system SHALL provide a GET /archetypes endpoint that returns archetypes for a selected format with metadata for dropdown display.

#### Scenario: Get archetypes for format
- **WHEN** GET /archetypes?format=Modern is called
- **THEN** return JSON with format and archetypes array
- **AND** each archetype includes id, name, meta_share, color_identity
- **AND** archetypes are sorted by meta_share descending

#### Scenario: Missing format parameter
- **WHEN** GET /archetypes is called without format query parameter
- **THEN** return 400 Bad Request
- **AND** include error message indicating format is required

#### Scenario: Invalid format parameter
- **WHEN** GET /archetypes?format=InvalidFormat is called
- **THEN** return 404 Not Found
- **AND** include error message indicating format not found

### Requirement: Conversation Resume Endpoint
The system SHALL provide a GET /conversations/{conversation_id} endpoint that returns conversation state and history for session resumption.

#### Scenario: Resume valid conversation
- **WHEN** GET /conversations/{valid_id} is called
- **THEN** return JSON with conversation_id, state, and messages
- **AND** state includes format, archetype, days, has_deck
- **AND** messages include role (user/assistant) and content

#### Scenario: Resume invalid conversation
- **WHEN** GET /conversations/{invalid_id} is called
- **THEN** return 404 Not Found
- **AND** include error message indicating conversation not found

### Requirement: Conversation State Persistence
The system SHALL persist conversation state within a session, allowing state to be modified and preserved across turns.

#### Scenario: Persist format selection
- **WHEN** user selects format via context or message
- **THEN** format is stored in conversation state
- **AND** format persists across all subsequent turns
- **AND** user can change format by selecting new value

#### Scenario: Persist days preference
- **WHEN** user specifies days preference
- **THEN** days is stored in conversation state
- **AND** days persists across meta and deck matchup tools
- **AND** user can adjust days in subsequent turns

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

### Requirement: LLM Configuration Consistency
The system SHALL use the same LLM configuration for agent reasoning and tool-internal LLM calls.

#### Scenario: Agent uses environment LLM configuration
- **WHEN** agent is initialized
- **THEN** LLM model is configured from LARGE_LANGUAGE_MODEL environment variable
- **AND** LLM provider is configured from LLM_PROVIDER environment variable

#### Scenario: Tool LLM calls use same configuration
- **WHEN** generate_deck_matchup_strategy, optimize_mainboard, or optimize_sideboard makes internal LLM calls
- **THEN** those calls use the same LARGE_LANGUAGE_MODEL and LLM_PROVIDER as the agent
- **AND** configuration is sourced from environment variables

### Requirement: Meta-Informed Deck Coaching
The system SHALL use meta research results to inform deck coaching suggestions.

#### Scenario: Meta results inform archetype dropdown
- **WHEN** user views meta results then provides deck
- **THEN** archetype dropdown options are informed by meta results
- **AND** top archetypes from meta appear prominently in dropdown

#### Scenario: Meta results inform optimization targets
- **WHEN** user views meta rankings then runs optimize_sideboard
- **THEN** meta rankings inform which archetypes appear in top_n
- **AND** sideboard is optimized against most relevant meta decks

#### Scenario: Coaching deep-dive across matchups
- **WHEN** user gets coaching for one matchup
- **AND** then asks about another matchup
- **THEN** reuse card_details and refresh matchup_stats for new opponent
- **AND** switch opponent_archetype while preserving deck context


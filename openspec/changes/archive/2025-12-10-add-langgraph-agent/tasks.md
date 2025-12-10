> Follow TDD: add or update tests first, then implement until green.

## 1. Dependencies and Setup
- [x] 1.1 Add `langgraph>=0.2.0` to pyproject.toml dependencies
- [x] 1.2 Add `sse-starlette>=2.0.0` to pyproject.toml dependencies
- [x] 1.3 Run `uv sync` to install new dependencies
- [x] 1.4 Confirm `langchain-mcp-adapters>=0.1.0` is pinned and used for MCP client (upgrade if needed)

## 2. Meta Research Tool Enhancement
- [x] 2.1 Write unit tests for `get_format_archetypes` (valid format, no data, sorting, schema)
- [x] 2.2 Implement `get_format_archetypes` MCP tool in `meta_research_tools.py`
- [x] 2.3 Verify tool returns id, name, meta_share, color_identity per archetype

## 3. Conversation State Management
- [x] 3.1 Write unit tests for conversation store CRUD + TTL behavior (in-memory)
- [x] 3.2 Create `src/app/agent_api/state.py` with ConversationState TypedDict
- [x] 3.3 Create `src/app/agent_api/store.py` with in-memory conversation store
- [x] 3.4 Implement conversation CRUD operations (create, get, update) to satisfy tests

## 4. LangGraph Workflow Implementation
- [x] 4.1 Write unit tests/specs for routing (meta vs deck), blocking rules, interleaving
- [x] 4.2 Create `src/app/agent_api/graph.py` with StateGraph definition
- [x] 4.3 Implement router node for intent classification (meta_research vs deck_coaching)
- [x] 4.4 Implement meta_research subgraph with tool calling nodes
- [x] 4.5 Implement deck_coaching subgraph with tool calling nodes
- [x] 4.6 Implement blocking dependency checks in each subgraph
- [x] 4.7 Configure graph with checkpointing for state persistence
- [x] 4.8 Ensure all routing/blocking tests are green

## 5. FastAPI Endpoint Implementation
- [x] 5.1 Write endpoint tests (contracts) for /welcome, /formats, /archetypes, /conversations/{id}, /chat (SSE shape)
- [x] 5.2 Create `src/app/agent_api/routes.py` with router
- [x] 5.3 Implement GET /welcome endpoint with dynamic tool catalog retrieval
- [x] 5.4 Implement GET /formats endpoint with database query
- [x] 5.5 Implement GET /archetypes endpoint calling get_format_archetypes
- [x] 5.6 Implement GET /conversations/{id} endpoint
- [x] 5.7 Implement POST /chat endpoint with SSE streaming
- [x] 5.8 Add request validation for all endpoints
- [x] 5.9 Make endpoint tests pass

## 6. SSE Streaming Implementation
- [x] 6.1 Write SSE formatting tests for event types (metadata, thinking, tool_call, content, state, done)
- [x] 6.2 Create `src/app/agent_api/streaming.py` with SSE event generators
- [x] 6.3 Implement metadata event emission
- [x] 6.4 Implement thinking event emission
- [x] 6.5 Implement tool_call event emission (calling/complete status)
- [x] 6.6 Implement content event emission for text chunks
- [x] 6.7 Implement state event emission
- [x] 6.8 Implement done event emission
- [x] 6.9 Make SSE formatting tests pass

## 7. LLM Interpretation Layer
- [x] 7.1 Write unit tests for LLM interpretation of MCP tool responses
- [x] 7.2 Create response interpretation prompts in `src/app/agent_api/prompts.py`
- [x] 7.3 Implement LLM interpretation node in graph.py that processes tool results
- [x] 7.4 Update /chat to pass tool results through LLM for natural language response
- [x] 7.5 Write unit tests for LLM-generated welcome message
- [x] 7.6 Update /welcome to generate LLM-interpreted welcome message
- [x] 7.7 Ensure LLM has access to tool_catalog when generating responses

## 8. Session Initialization via /welcome
- [x] 8.1 Write unit tests for /welcome session creation
- [x] 8.2 Update /welcome to create new conversation and return conversation_id
- [x] 8.3 Store tool_catalog, available_formats, workflows in conversation state
- [x] 8.4 Write unit tests for /chat accessing welcome info from session
- [x] 8.5 Update /chat to use session-stored tool_catalog for LLM context
- [x] 8.6 Update state.py with available_formats and workflows fields

## 9. Integration and Testing
- [x] 9.1 Create `src/app/agent_api/main.py` to mount agent routes
- [x] 9.2 Review new code generated for the changes above for inconsistencies
- [x] 9.3 Update root FastAPI app to include agent router
- [x] 9.4 Write integration tests for full conversation flow with LLM interpretation
- [x] 9.5 Write integration tests for /welcome session initialization
- [x] 9.6 Write integration tests for /chat using welcome info from session
- [x] 9.7 Write integration tests for workflow interleaving scenarios
- [x] 9.8 Write integration tests for blocking dependency enforcement
- [x] 9.9 Update Postman collection with /welcome → /chat flow
- [x] 9.10 Run agent integration tests and root cause any issues
- [x] 9.11 Run Postman agent collection and root cause any issues

## 10. Documentation
- [x] 10.1 Update README with agent API documentation
- [x] 10.2 Add API endpoint examples to README showing /welcome → /chat flow
- [x] 10.3 Document environment variables for LLM configuration (already in README)
- [x] 10.4 Verify alignment between specs, code, and tests


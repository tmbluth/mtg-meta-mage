# Change: Add LangGraph FastAPI Agent

## Why
The MCP tools exist for meta research and deck coaching, but there's no conversational agent layer that can orchestrate multi-turn interactions, manage state across conversations, and intelligently route between meta research and deck coaching workflows. Users need a chat interface that understands context, collects required inputs progressively, and maintains session state. Additionally, raw MCP tool data needs to be interpreted by an LLM to provide natural language responses to users.

## What Changes
- Add LangGraph workflow with two sub-workflows: Meta Research and Deck Coaching
- Add FastAPI endpoints: GET /welcome, POST /chat, GET /formats, GET /archetypes, GET /conversations/{id}
- Add GET /welcome endpoint that initializes a new conversation session and returns LLM-generated natural language welcome message
- Invoke /welcome automatically when starting a new session to populate initial context
- Store welcome info (tool catalog, available formats, workflows) in conversation state for use throughout the session
- Add LLM interpretation layer in agent API: MCP tool responses are processed by an LLM to generate natural language responses
- Both /welcome and /chat return LLM-interpreted natural language responses (not raw JSON data)
- Add `get_format_archetypes` MCP tool (currently missing)
- Add conversation state management with persistence
- Add SSE streaming response support for real-time feedback
- Add intent-based routing between meta research and deck coaching
- Add dependency enforcement for required inputs before tool execution
- Use `langchain-mcp-adapters` MultiServerMCPClient for MCP tool calls (streamable_http)
- Add langgraph and sse-starlette dependencies

## Impact
- Affected specs: `agent-api` (new), `meta-analytics` (modified)
- Affected code: 
  - `src/app/agent_api/` - new agent implementation
  - `src/app/mcp/tools/meta_research_tools.py` - add `get_format_archetypes`
  - `pyproject.toml` - new dependencies


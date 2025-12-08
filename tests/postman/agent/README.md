# MTG Meta Mage Agent API - Postman Collection

This Postman collection provides examples and tests for the MTG Meta Mage Agent API endpoints.

## Setup

1. Import the collection into Postman
2. Set the `base_url` collection variable to your server URL (default: `http://localhost:8000`)
3. Set the `format` variable to your preferred format (default: `Modern`)

## Collection Structure

### Discovery Endpoints
- **GET /welcome** - Get welcome information including available formats, workflows, and tools
- **GET /formats** - Get list of available formats
- **GET /archetypes** - Get archetypes for a specific format

### Conversation Endpoints
- **POST /chat** - Start or continue a conversation (returns SSE stream)
- **GET /conversations/{id}** - Retrieve conversation state and history

### Example Workflows
- **Meta Research Workflow** - Complete workflow for meta research
- **Deck Coaching Workflow** - Complete workflow for deck coaching

## Usage

### Starting a New Conversation

1. Call **GET /welcome** to see available capabilities
2. Call **GET /formats** to see available formats
3. Call **POST /chat** with your message and context

The `/chat` endpoint returns a Server-Sent Events (SSE) stream. In Postman, you'll see the raw SSE format. For a better experience, use a client that supports SSE parsing.

### Continuing a Conversation

1. Extract the `conversation_id` from the first `/chat` response (it's in the SSE metadata event)
2. The collection automatically sets the `conversation_id` variable from responses
3. Call **POST /chat** again with the same `conversation_id` to continue

### SSE Event Types

The `/chat` endpoint streams these event types:

- `metadata` - Conversation metadata (conversation_id, format, archetype)
- `thinking` - Agent reasoning (optional)
- `tool_call` - Tool execution progress
- `content` - Response text chunks
- `state` - State snapshot for UI
- `done` - Stream complete

## Testing

The collection includes test scripts that:
- Verify status codes
- Check response structure
- Extract conversation_id for use in subsequent requests

Run the collection in Postman's test runner to verify all endpoints work correctly.

## Environment Variables

Set these environment variables before starting the server:

- `LARGE_LANGUAGE_MODEL` - LLM model name (e.g., "gpt-4", "claude-3-opus")
- `LLM_PROVIDER` - LLM provider (e.g., "openai", "anthropic")
- `MCP_SERVER_NAME` - MCP server name (default: "mtg-meta-mage-mcp")
- `MCP_SERVER_URL` - MCP server URL (default: "http://localhost:8000/mcp")
- `DB_NAME` - Database name
- `DB_USER` - Database user
- `DB_PASSWORD` - Database password
- `DB_HOST` - Database host
- `DB_PORT` - Database port

## Notes

- Conversations expire after 3 hours (configurable via TTL)
- Message history is capped at 10 messages (configurable)
- The agent API requires format and days for meta research workflows
- The agent API requires format, deck_text, and archetype for deck coaching workflows


# Postman Collection for MTG Meta Mage MCP Server

This directory contains a Postman collection for testing the MTG Meta Mage MCP server.

## Collection Overview

The collection includes requests for:

### Meta Research Tools
- **Get Format Meta Rankings** - Retrieve format-wide archetype popularity and win rates
- **Get Format Meta Rankings (Filtered)** - Filter rankings by color identity
- **Get Format Meta Rankings (Grouped)** - Group rankings by strategy type
- **Get Format Matchup Stats** - Get complete matchup matrix for the format

### Deck Coaching Tools
- **Parse and Validate Decklist** - Parse a decklist and enrich with card details
- **Get Deck Matchup Stats** - Get matchup stats for a specific archetype
- **Generate Matchup Strategy** - Generate AI-powered strategy advice for a matchup

### FastAPI Endpoints
- **Get Archetype Rankings** - FastAPI endpoint calling MCP tool
- **Get Matchup Matrix** - FastAPI endpoint calling MCP tool

## Setup

### Import the Collection

1. Open Postman
2. Click **Import** in the top left
3. Select `MTG_Meta_Mage_MCP_Server.postman_collection.json`
4. The collection will appear in your workspace

### Configure Variables

The collection uses environment variables:

- `mcp_base_url` - Base URL for the MCP server (default: `http://localhost:8000`)
- `format` - Tournament format to query (default: `Modern`)

You can modify these in the collection variables or create a Postman environment.

## Running the MCP Server

Before using the collection, start the MCP server:

```bash
# Start the FastMCP server
fastmcp run src/app/mcp/server.py --port 8000

# Or if using the FastAPI app (which includes MCP tools):
uvicorn src.app.api.main:app --reload --port 8080
```

## Architecture

The system uses a two-server architecture:

1. **FastMCP Server** (port 8000): Exposes MCP tools via JSON-RPC protocol with `streamable_http` transport
2. **FastAPI Server** (port 8080): REST API endpoints that call MCP tools internally using `MultiServerMCPClient`

### FastAPI Endpoints (Recommended for Testing)

The FastAPI server provides REST endpoints that internally call MCP tools:

- **GET** `/api/v1/meta/archetypes` - Get archetype rankings (calls `get_format_meta_rankings` MCP tool)
- **GET** `/api/v1/meta/matchups` - Get matchup matrix (calls `get_format_matchup_stats` MCP tool)

These endpoints are the recommended way to test the system as they work out of the box with standard HTTP clients.

### MCP Server (For External MCP Clients)

The MCP server exposes tools through the standard MCP JSON-RPC protocol:

- **Endpoint**: `http://localhost:8000/mcp`
- **Protocol**: JSON-RPC 2.0 with `streamable_http` transport
- **Tool Discovery**: `tools/list` method
- **Tool Invocation**: `tools/call` method

**Note**: The MCP server uses JSON-RPC protocol and requires an MCP client library (like `langchain-mcp-adapters`) to interact with. Direct HTTP POST requests won't work without proper JSON-RPC formatting and session management.

## Using with External MCP Clients

These same tools can be used by any MCP client:

### Claude Desktop

Add to your Claude Desktop MCP configuration:

```json
{
  "mcpServers": {
    "mtg-meta-mage": {
      "url": "http://localhost:8000/mcp",
      "transport": "streamable_http"
    }
  }
}
```

### Custom Agents

Use the `langchain-mcp-adapters` library:

```python
from langchain_mcp_adapters.client import MultiServerMCPClient

client = MultiServerMCPClient({
    "meta_analytics": {
        "url": "http://localhost:8000/mcp",
        "transport": "streamable_http"
    }
})

# Get available tools
tools = await client.get_tools()

# Call a tool
result = await client.call_tool(
    "meta_analytics",
    "get_format_meta_rankings",
    arguments={"format": "Modern", "current_days": 14}
)
```

## Testing Workflow

### Prerequisites

1. **Start the MCP Server**:
   ```bash
   uv run fastmcp run src/app/mcp/server.py --transport http --port 8000
   ```

2. **Start the FastAPI Server**:
   ```bash
   uv run uvicorn src.app.api.main:app --host 0.0.0.0 --port 8080
   ```

### Testing Steps

1. **Start with Health Check**
   - Run the "FastAPI Health Check" request to verify the FastAPI server is accessible
   - This confirms both servers are running

2. **Test FastAPI Endpoints** (Recommended)
   - Use "Get Archetype Rankings" to see current meta landscape
   - Try filtered variations (by color, strategy)
   - Try grouped variations (group_by parameter)
   - Get the complete matchup matrix

3. **Verify MCP Integration**
   - FastAPI endpoints internally call MCP tools via `MultiServerMCPClient`
   - The MCP server must be running for FastAPI endpoints to work
   - Check server logs to see MCP tool calls being made

### Running Postman Collection

```bash
# Install Newman (Postman CLI runner)
npm install -g newman

# Run the collection
newman run tests/postman/mcp_server/MTG_Meta_Mage_MCP_Server.postman_collection.json
```

## Troubleshooting

### Connection Refused
- Verify the MCP server is running
- Check the `mcp_base_url` variable matches your server address

### No Data Returned
- Ensure your database has tournament data for the specified format
- Try different time windows (e.g., 30 days instead of 14)

### Tool Not Found
- Verify tool names match exactly (case-sensitive)
- Check that the MCP server has registered all tools on startup

## Additional Resources

- [MCP Protocol Documentation](https://modelcontextprotocol.io)
- [FastMCP Documentation](https://github.com/jlowin/fastmcp)
- [MTG Meta Mage Documentation](../../README.md)


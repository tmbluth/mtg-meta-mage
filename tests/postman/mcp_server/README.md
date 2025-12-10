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
- **Get Enriched Deck** - Parse a deck and enrich with card details from database
- **Get Deck Matchup Stats** - Get matchup stats for a specific archetype
- **Generate Matchup Strategy** - Generate AI-powered strategy advice for a matchup

### Deck Optimization Tools
- **Optimize Mainboard** - Analyze mainboard and recommend flex spot replacements based on meta
- **Optimize Sideboard** - Suggest sideboard tweaks to answer top meta archetypes

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
# Start the MCP server (uses proper module imports for tool registration)
uv run python src/app/mcp/run_server.py --port 8000
```

> **Note**: Use `run_server.py` instead of `fastmcp run server.py` to ensure tools are properly registered. The `fastmcp run` command creates a separate MCP instance that doesn't have the tools registered via decorators.

## Architecture

The MCP server exposes tools via JSON-RPC protocol with `streamable_http` transport.

### MCP Server

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
    "mtg-meta-mage-mcp": {
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

**Start the MCP Server**:
```bash
uv run python src/app/mcp/run_server.py --port 8000
```

### Testing Steps

1. **Verify MCP Server**
   - Confirm the MCP server is running on port 8000
   - The server exposes tools via JSON-RPC protocol

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

## Tool Documentation

### Deck Optimization Tools

#### optimize_mainboard

Analyzes a user's maindeck and identifies non-critical cards (flex spots), recommending replacements based on the current meta.

**Parameters:**
- `card_details` (list): List of card objects with `name`, `section`, and `color_identity` fields
- `archetype` (string): The user's deck archetype name
- `format` (string): Format name (e.g., "Modern", "Standard")
- `top_n` (integer, default: 5): Number of top meta archetypes to analyze against

**Returns:**
- `flex_spots`: List of cards identified as flex spots with reasons
- `recommendations`: List of recommended replacement cards with justifications
- `archetype`: The user's archetype
- `format`: The format analyzed
- `top_n`: Number of meta archetypes analyzed

**Example Usage:**

```python
result = await client.call_tool(
    "meta_analytics",
    "optimize_mainboard",
    arguments={
        "card_details": [
            {"name": "Lightning Bolt", "section": "mainboard", "color_identity": ["R"]},
            {"name": "Spell Pierce", "section": "mainboard", "color_identity": ["U"]},
            {"name": "Murktide Regent", "section": "mainboard", "color_identity": ["U"]}
        ],
        "archetype": "Murktide",
        "format": "Modern",
        "top_n": 5
    }
)
```

**Example Response:**

```json
{
  "flex_spots": [
    {
      "card_name": "Spell Pierce",
      "quantity": 2,
      "reason": "Less effective against cascade and combo archetypes dominating the current meta"
    }
  ],
  "recommendations": [
    {
      "card_name": "Counterspell",
      "quantity": 2,
      "reason": "Unconditional counter better suited for stopping Crashing Footfalls and Colossus Hammer"
    }
  ],
  "archetype": "Murktide",
  "format": "Modern",
  "top_n": 5
}
```

#### optimize_sideboard

Suggests sideboard tweaks to better answer the top N most frequent archetypes in the meta, with specific sideboard plans for each matchup.

**Parameters:**
- `card_details` (list): List of card objects with `name`, `section`, and `color_identity` fields
- `archetype` (string): The user's deck archetype name
- `format` (string): Format name (e.g., "Modern", "Standard")
- `top_n` (integer, default: 5): Number of top meta archetypes to prepare for

**Returns:**
- `sideboard_changes`: List of additions/removals with reasons
- `sideboard_plans`: Matchup-specific sideboard guides
- `final_sideboard`: Complete 15-card sideboard after changes
- `archetype`: The user's archetype
- `format`: The format analyzed
- `top_n`: Number of archetypes analyzed

**Example Usage:**

```python
result = await client.call_tool(
    "meta_analytics",
    "optimize_sideboard",
    arguments={
        "card_details": [
            {"name": "Lightning Bolt", "section": "mainboard", "color_identity": ["R"]},
            {"name": "Murktide Regent", "section": "mainboard", "color_identity": ["U"]},
            {"name": "Engineered Explosives", "section": "sideboard", "color_identity": []},
            {"name": "Chalice of the Void", "section": "sideboard", "color_identity": []}
        ],
        "archetype": "Murktide",
        "format": "Modern",
        "top_n": 3
    }
)
```

**Example Response:**

```json
{
  "sideboard_changes": [
    {
      "action": "remove",
      "card_name": "Engineered Explosives",
      "quantity": 2,
      "reason": "Less relevant against current top archetypes"
    },
    {
      "action": "add",
      "card_name": "Grafdigger's Cage",
      "quantity": 2,
      "reason": "Strong against Rhinos' graveyard recursion and cascade strategy"
    }
  ],
  "sideboard_plans": [
    {
      "opponent_archetype": "Rhinos",
      "cards_in": ["Grafdigger's Cage", "Chalice of the Void"],
      "cards_out": ["Lightning Bolt", "Dragon's Rage Channeler"],
      "strategy": "Shut down their cascade spells with Chalice on 0, use Cage to prevent graveyard recursion"
    }
  ],
  "final_sideboard": [
    {"card_name": "Grafdigger's Cage", "quantity": 2},
    {"card_name": "Chalice of the Void", "quantity": 3},
    {"card_name": "Dress Down", "quantity": 2},
    {"card_name": "Spell Pierce", "quantity": 3},
    {"card_name": "Mystical Dispute", "quantity": 2},
    {"card_name": "Subtlety", "quantity": 3}
  ],
  "archetype": "Murktide",
  "format": "Modern",
  "top_n": 3
}
```

### Complete Deck Optimization Workflow

For best results, use both tools in sequence:

1. **Start with mainboard optimization** to identify weak cards and strengthen your game 1 strategy
2. **Follow with sideboard optimization** to prepare for the most common matchups
3. **Review recommendations** against your local meta and playstyle preferences

**Example Workflow:**

```python
# Step 1: Parse your deck
deck = await client.call_tool(
    "meta_analytics",
    "get_enriched_deck",
    arguments={"decklist_text": "4 Lightning Bolt\n4 Murktide Regent\n..."}
)

# Step 2: Optimize mainboard
mainboard_improvements = await client.call_tool(
    "meta_analytics",
    "optimize_mainboard",
    arguments={
        "card_details": deck["card_details"],
        "archetype": "Murktide",
        "format": "Modern",
        "top_n": 5
    }
)

# Step 3: Optimize sideboard
sideboard_improvements = await client.call_tool(
    "meta_analytics",
    "optimize_sideboard",
    arguments={
        "card_details": deck["card_details"],
        "archetype": "Murktide",
        "format": "Modern",
        "top_n": 5
    }
)
```

**Notes:**
- Both tools respect format legality constraints (cards must be legal in the specified format)
- Both tools filter cards to match your deck's color identity
- Both tools use actual meta decks (up to 5 recent lists per archetype) to inform recommendations
- Sideboard tool enforces the 15-card sideboard constraint and will retry if needed

## Additional Resources

- [MCP Protocol Documentation](https://modelcontextprotocol.io)
- [FastMCP Documentation](https://github.com/jlowin/fastmcp)
- [MTG Meta Mage Documentation](../../README.md)


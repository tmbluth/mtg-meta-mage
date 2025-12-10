"""FastMCP server initialization and configuration"""

import os

from dotenv import load_dotenv
from fastmcp import FastMCP

# Load environment variables from .env file
load_dotenv()

# Initialize MCP server with name from environment variable, default to "mtg-meta-mage-mcp"
server_name = os.getenv("MCP_SERVER_NAME", "mtg-meta-mage-mcp")
mcp = FastMCP(server_name, version="0.1.0")

# Tools will be registered via decorators in individual tool modules
# Import tools to register them with the server
# These imports must come after mcp initialization
from src.app.mcp.tools import meta_research_tools, deck_coaching_tools  # noqa: E402, F401

__all__ = ['mcp']


"""FastMCP server initialization and configuration"""

import os

from fastmcp import FastMCP

# Initialize MCP server with name from environment variable
server_name = os.getenv("MCP_SERVER_NAME")
mcp = FastMCP(server_name, version="0.1.0")

# Tools will be registered via decorators in individual tool modules
# Import tools to register them with the server
# These imports must come after mcp initialization
from src.app.mcp.tools import meta_research_tools, deck_coaching_tools  # noqa: E402, F401

__all__ = ['mcp']


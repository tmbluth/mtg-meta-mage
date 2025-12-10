"""Entry point for running the MCP server with properly registered tools.

This script imports the mcp instance from the module system, ensuring the same
instance that has tools registered is used when the server runs.

Usage:
    uv run python -m src.app.mcp.run_server
    # or
    uv run python src/app/mcp/run_server.py
"""

import os
import sys

# Ensure the project root is in the path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import the mcp instance from the module system - this ensures tools are registered
from src.app.mcp.server import mcp

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run the MCP server")
    parser.add_argument("--port", type=int, default=int(os.getenv("MCP_SERVER_PORT", "8000")), help="Port to run on")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    args = parser.parse_args()
    
    # Run the server
    mcp.run(transport="streamable-http", host=args.host, port=args.port)


"""Utility to fetch tool metadata from the MCP server."""

import logging
import os
from typing import List, Dict, Any

from langchain_mcp_adapters.client import MultiServerMCPClient

logger = logging.getLogger(__name__)

_catalog_cache: List[Dict[str, Any]] | None = None


async def fetch_tool_catalog() -> List[Dict[str, Any]]:
    """Fetch and cache tool metadata from the MCP server."""
    global _catalog_cache
    if _catalog_cache is not None:
        logger.debug("Returning cached tool catalog")
        return _catalog_cache

    # Check for test environment variables first, then fall back to regular ones
    server_name = os.getenv("TEST_MCP_SERVER_NAME") or os.getenv("MCP_SERVER_NAME", "mtg-meta-mage-mcp")
    server_port = os.getenv("TEST_MCP_SERVER_PORT") or os.getenv("MCP_SERVER_PORT", "8000")
    
    logger.info(f"Fetching tool catalog: server_name={server_name}, server_port={server_port}")
    
    # Construct URL from port if MCP_SERVER_URL is not explicitly set
    server_url = os.getenv("MCP_SERVER_URL")
    if not server_url:
        server_url = f"http://localhost:{server_port}/mcp"
    
    logger.info(f"Using MCP server URL: {server_url}")

    try:
        client = MultiServerMCPClient(
            {server_name: {"url": server_url, "transport": "streamable_http"}}
        )
        logger.debug(f"Created MCP client for server: {server_name}")

        tools = await client.get_tools()
        logger.info(f"Retrieved {len(tools)} tools from MCP server")

        catalog: List[Dict[str, Any]] = []
        for tool in tools:
            # langchain-mcp-adapters returns BaseTool like objects; access attributes defensively
            name = getattr(tool, "name", None) or getattr(tool, "tool_name", None)
            description = getattr(tool, "description", "") or ""
            catalog.append(
                {
                    "name": name,
                    "description": description,
                    "server": server_name,
                }
            )
            logger.debug(f"Added tool to catalog: {name}")

        _catalog_cache = catalog
        logger.info(f"Tool catalog cached with {len(catalog)} tools")
        return catalog
    except Exception as e:
        logger.error(f"Error fetching tool catalog from MCP server: {e}", exc_info=True)
        raise


async def get_tool_catalog_safe() -> List[Dict[str, Any]]:
    """Fetch catalog, but fall back to empty list on error."""
    try:
        return await fetch_tool_catalog()
    except Exception as exc:  # pragma: no cover - external dependency may fail
        logger.warning("Failed to fetch tool catalog from MCP server: %s", exc)
        return []


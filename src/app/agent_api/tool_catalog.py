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
        return _catalog_cache

    server_name = os.getenv("MCP_SERVER_NAME")
    server_url = os.getenv("MCP_SERVER_URL")

    client = MultiServerMCPClient(
        {server_name: {"url": server_url, "transport": "streamable_http"}}
    )

    tools = await client.get_tools()

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

    _catalog_cache = catalog
    return catalog


async def get_tool_catalog_safe() -> List[Dict[str, Any]]:
    """Fetch catalog, but fall back to empty list on error."""
    try:
        return await fetch_tool_catalog()
    except Exception as exc:  # pragma: no cover - external dependency may fail
        logger.warning("Failed to fetch tool catalog from MCP server: %s", exc)
        return []


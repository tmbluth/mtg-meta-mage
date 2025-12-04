"""
MCP Client for FastAPI Routes

This module provides a shared MultiServerMCPClient instance for calling MCP tools
from FastAPI routes over HTTP using the streamable_http transport.
"""

import logging
import os
from typing import Optional

from langchain_mcp_adapters.client import MultiServerMCPClient

logger = logging.getLogger(__name__)

# Get MCP server URL from environment or use default
MCP_SERVER_URL = os.getenv("MCP_SERVER_URL", "http://localhost:8000/mcp")
MCP_SERVER_NAME = "meta_analytics"

# Module-level client instance (initialized lazily)
_mcp_client: Optional[MultiServerMCPClient] = None

# Cache for tools to avoid repeated get_tools calls
_tools_cache: Optional[dict] = None


def get_mcp_client() -> MultiServerMCPClient:
    """
    Get or create the shared MCP client instance.
    
    The client is initialized on first access and reused for all requests.
    This ensures efficient connection pooling and avoids creating multiple clients.
    
    Returns:
        MultiServerMCPClient: Configured client for calling MCP tools
    """
    global _mcp_client
    
    if _mcp_client is None:
        logger.info(f"Initializing MCP client connecting to {MCP_SERVER_URL}")
        _mcp_client = MultiServerMCPClient({
            MCP_SERVER_NAME: {
                "transport": "streamable_http",
                "url": MCP_SERVER_URL,
            }
        })
        logger.info("MCP client initialized successfully")
    
    return _mcp_client


async def _get_tools() -> dict:
    """
    Get all MCP tools and cache them by name.
    
    Uses HTTP-based discovery via langchain-mcp-adapters, with session-based
    fallback if direct get_tools() returns empty.
    
    Returns:
        dict: Dictionary mapping tool names to LangChain tool objects
    """
    global _tools_cache
    
    if _tools_cache is None:
        client = get_mcp_client()
        logger.debug("Fetching MCP tools from server")
        
        try:
            # Try getting tools via HTTP first
            tools = await client.get_tools(server_name=MCP_SERVER_NAME)
            logger.debug(f"Got {len(tools)} tools via HTTP get_tools()")
            
            if len(tools) == 0:
                # HTTP discovery failed - try session-based discovery
                logger.debug("No tools via HTTP, trying with session...")
                try:
                    async with client.session(MCP_SERVER_NAME) as session:
                        from langchain_mcp_adapters.tools import load_mcp_tools
                        tools = await load_mcp_tools(session)
                        logger.debug(f"Got {len(tools)} tools via session")
                except Exception as session_error:
                    logger.debug(f"Session-based discovery also failed: {session_error}")
            
            if len(tools) > 0:
                # HTTP discovery succeeded
                _tools_cache = {tool.name: tool for tool in tools}
                logger.info(f"Cached {len(_tools_cache)} MCP tools via HTTP: {list(_tools_cache.keys())}")
            else:
                # Both HTTP methods failed
                logger.error("HTTP tool discovery failed - no tools available")
                _tools_cache = {}
                    
        except Exception as e:
            logger.error(f"Error fetching MCP tools: {e}", exc_info=True)
            _tools_cache = {}
    
    return _tools_cache


async def call_mcp_tool(tool_name: str, arguments: dict) -> dict:
    """
    Call an MCP tool via HTTP client.
    
    Args:
        tool_name: Name of the MCP tool to call
        arguments: Dictionary of arguments to pass to the tool
        
    Returns:
        dict: Tool result as returned by the MCP server
        
    Raises:
        KeyError: If the tool name is not found
        Exception: If the tool call fails or returns an error
    """
    logger.debug(f"Calling MCP tool '{tool_name}' with arguments: {arguments}")
    
    try:
        # Get tools (cached after first call)
        tools = await _get_tools()
        
        # Find the tool
        tool = tools.get(tool_name)
        if tool is None:
            available_tools = list(tools.keys())
            raise KeyError(
                f"Tool '{tool_name}' not found. Available tools: {available_tools}"
            )
        
        # Invoke tool via HTTP
        logger.debug(f"Invoking tool '{tool_name}' via HTTP")
        result = await tool.ainvoke(arguments)
        
        logger.debug(f"MCP tool '{tool_name}' returned successfully")
        return result
        
    except KeyError:
        raise
    except Exception as e:
        logger.error(f"Error calling MCP tool '{tool_name}': {e}", exc_info=True)
        raise


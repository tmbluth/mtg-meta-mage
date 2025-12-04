"""MCP tools for MTG Meta Mage."""

# Import tool modules to register them with the MCP server
from . import meta_research_tools  # noqa: F401
from . import deck_coaching_tools  # noqa: F401

__all__ = ['meta_research_tools', 'deck_coaching_tools']

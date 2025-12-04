"""
Deck Coaching MCP Tools - SPECIFIC DECK analysis and personalized advice.

This module provides MCP tools for analyzing user decklists and generating matchup coaching.
Use these tools for PERSONALIZED deck advice, not general meta research.
"""

import logging
import os
from typing import Optional

from src.app.mcp.server import mcp
from src.core_utils import normalize_card_name, parse_decklist
from src.database.connection import DatabaseConnection
from Levenshtein import distance as levenshtein_distance

logger = logging.getLogger(__name__)

@mcp.tool()
def parse_and_validate_decklist(decklist: str) -> dict:
    """
    Parse a decklist and enrich with card details from the database.
    
    Use this when users provide a decklist to analyze.
    This is for SPECIFIC DECK validation, not general meta queries.

    Args:
        decklist: Text representation of deck (e.g., "4 Lightning Bolt\\n3 Snapcaster Mage")

    Returns:
        Dictionary with enriched card data including:
        - card_details: List of card objects with name, oracle_text, type_line, mana_cost, cmc, color_identity
        - mainboard_count: Number of mainboard cards
        - sideboard_count: Number of sideboard cards
        - errors: List of cards that couldn't be found
    """
    # Parse the decklist string
    parsed_cards = parse_decklist(decklist)
    
    if not parsed_cards:
        return {
            "card_details": [],
            "mainboard_count": 0,
            "sideboard_count": 0,
            "errors": ["Empty or invalid decklist"]
        }
    
    # Fetch card data from database
    card_names = [card["card_name"] for card in parsed_cards]
    
    query = """
        SELECT name, oracle_text, type_line, mana_cost, cmc, color_identity
        FROM cards
        WHERE LOWER(name) = ANY(%s)
    """
    
    with DatabaseConnection.get_cursor() as cursor:
        # Normalize card names for lookup
        normalized_names = [normalize_card_name(name) for name in card_names]
        cursor.execute(query, (normalized_names,))
        db_cards = cursor.fetchall()
    
    # Create a lookup dictionary (normalized name -> card data)
    card_lookup = {}
    for row in db_cards:
        normalized = normalize_card_name(row[0])
        card_lookup[normalized] = {
            "name": row[0],
            "oracle_text": row[1],
            "type_line": row[2],
            "mana_cost": row[3],
            "cmc": row[4],
            "color_identity": row[5]
        }
    
    # Enrich parsed cards with database info
    enriched_cards = []
    errors = []
    mainboard_count = 0
    sideboard_count = 0
    
    for card in parsed_cards:
        normalized = normalize_card_name(card["card_name"])
        
        # First try exact match
        card_data = card_lookup.get(normalized)
        
        # If no exact match, try fuzzy matching against normalized names in lookup
        if not card_data:
            normalized_input = normalize_card_name(card["card_name"])
            best_match = None
            best_distance = float('inf')
            threshold = 2  # Max edit distance
            
            for norm_name in card_lookup.keys():
                dist = levenshtein_distance(normalized_input.lower(), norm_name.lower())
                if dist < best_distance and dist <= threshold:
                    best_distance = dist
                    best_match = norm_name
            
            if best_match:
                card_data = card_lookup[best_match]
                logger.info(f"Fuzzy matched '{card['card_name']}' to '{card_data['name']}'")
        
        if card_data:
            enriched_card = {
                **card_data,
                "quantity": card["quantity"],
                "section": card["section"]
            }
            enriched_cards.append(enriched_card)
            
            if card["section"] == "mainboard":
                mainboard_count += card["quantity"]
            else:
                sideboard_count += card["quantity"]
        else:
            errors.append(card["card_name"])
            logger.warning(f"Card not found in database: {card['card_name']}")
    
    return {
        "card_details": enriched_cards,
        "mainboard_count": mainboard_count,
        "sideboard_count": sideboard_count,
        "errors": errors
    }

@mcp.tool()
def get_deck_matchup_stats(archetype: str, format: str, days: int = 14) -> dict:
    """
    Get matchup statistics for a SPECIFIC archetype vs the meta.
    
    Use this when analyzing a user's deck against the field.
    This is for SPECIFIC DECK positioning, not general meta queries.
    
    Args:
        archetype: Your deck archetype name
        format: Tournament format (e.g., "Modern", "Pioneer")
        days: Number of days to analyze (default: 14)

    Returns:
        Dictionary with matchup_stats containing win rates and match counts against other archetypes
    """
    # Import here to avoid circular dependency
    from . import meta_research_tools
    
    # Get the full matchup matrix (use .fn to get unwrapped function)
    matrix_result = meta_research_tools.get_format_matchup_stats.fn(format=format, days=days)
    
    if not matrix_result.get("matrix"):
        return {
            "archetype": archetype,
            "format": format,
            "days": days,
            "matchup_stats": [],
            "message": "No matchup data available for this format and time period"
        }
    
    # Extract matchup stats for the specified archetype
    matrix = matrix_result["matrix"]
    matchup_stats = []
    
    if archetype in matrix:
        for opponent_archetype, data in matrix[archetype].items():
            matchup_stats.append({
                "opponent_archetype": opponent_archetype,
                "win_rate": data["win_rate"],
                "match_count": data["match_count"]
            })
        
        # Sort by match count descending (most played matchups first)
        matchup_stats.sort(key=lambda x: x["match_count"] if x["match_count"] is not None else 0, reverse=True)
    
    return {
        "archetype": archetype,
        "format": format,
        "days": days,
        "matchup_stats": matchup_stats,
        "metadata": matrix_result.get("metadata", {})
    }


@mcp.tool()
def generate_matchup_strategy(
    card_details: list,
    archetype: str,
    opponent_archetype: str,
    matchup_stats: Optional[dict] = None
) -> dict:
    """
    Generate AI-powered coaching for a specific matchup.
    
    Use this after getting matchup stats to provide actionable advice.
    This creates PERSONALIZED strategy based on the user's exact deck.

    Args:
        card_details: List of enriched card objects from parse_and_validate_decklist
        archetype: Your deck's archetype
        opponent_archetype: Opponent's archetype
        matchup_stats: Optional matchup statistics (win_rate, match_count)

    Returns:
        Dictionary with coaching advice including:
        - mulligan_guide: Tips on what to keep/mulligan
        - key_cards: Important cards for the matchup
        - game_plan: Strategy for each game phase
        - sideboard_guide: Recommended sideboard changes
    """
    from src.app.mcp.prompts.coaching_prompt import COACHING_PROMPT_TEMPLATE
    
    # Prepare deck summary for LLM
    mainboard = [c for c in card_details if c.get("section") == "mainboard"]
    sideboard = [c for c in card_details if c.get("section") == "sideboard"]
    
    deck_summary = f"""
Mainboard ({len(mainboard)} cards):
{_format_card_list(mainboard)}

Sideboard ({len(sideboard)} cards):
{_format_card_list(sideboard)}
"""
    
    # Format matchup stats if available
    matchup_context = ""
    if matchup_stats:
        win_rate = matchup_stats.get("win_rate")
        match_count = matchup_stats.get("match_count")
        if win_rate is not None:
            matchup_context = f"\n\nMatchup Statistics:\n- Win Rate: {win_rate:.1f}%\n- Sample Size: {match_count} matches"
    
    # Format prompt
    prompt = COACHING_PROMPT_TEMPLATE.format(
        archetype=archetype,
        opponent_archetype=opponent_archetype,
        deck_summary=deck_summary,
        matchup_context=matchup_context
    )
    
    # Call LLM (using environment variable for now, will be configurable later)
    try:
        # Import LLM client factory
        import os
        from src.clients.llm_client import get_llm_client
        
        # Get model name and provider from environment
        model_name = os.getenv("LLM_MODEL", "gpt-4o-mini")
        model_provider = os.getenv("LLM_PROVIDER", "openai")
        
        llm = get_llm_client(model_name, model_provider)
        response = llm.run(prompt)
        
        return {
            "archetype": archetype,
            "opponent_archetype": opponent_archetype,
            "advice": response.text,
            "matchup_stats": matchup_stats
        }
    
    except Exception as e:
        logger.error(f"Error generating piloting advice: {e}")
        return {
            "archetype": archetype,
            "opponent_archetype": opponent_archetype,
            "advice": None,
            "error": str(e),
            "matchup_stats": matchup_stats
        }


def _format_card_list(cards: list) -> str:
    """Format a list of cards for display in prompt."""
    lines = []
    for card in cards:
        quantity = card.get("quantity", 1)
        name = card.get("name", "Unknown")
        card_type = card.get("type_line", "")
        lines.append(f"  {quantity}x {name} ({card_type})")
    return "\n".join(lines)
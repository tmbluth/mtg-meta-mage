"""
Deck Coaching MCP Tools - SPECIFIC DECK analysis and personalized advice.

This module provides MCP tools for analyzing user decks and generating matchup coaching.
Use these tools for PERSONALIZED deck advice, not general meta research.
"""

import logging
import os
from typing import Optional

from src.app.mcp.server import mcp
from src.core_utils import normalize_card_name, parse_deck
from src.etl.database.connection import DatabaseConnection
from Levenshtein import distance as levenshtein_distance

logger = logging.getLogger(__name__)

@mcp.tool()
def get_enriched_deck(deck: str) -> dict:
    """
    Parse a deck and enrich with card details from the database.
    
    Use this when users provide a deck to analyze.
    This is for SPECIFIC DECK enrichment, not general meta queries.

    Args:
        deck: Text representation of deck (e.g., "4 Lightning Bolt\\n3 Snapcaster Mage")

    Returns:
        Dictionary with enriched card data including:
        - card_details: List of card objects with name, oracle_text, type_line, mana_cost, cmc, color_identity, rulings
        - mainboard_count: Number of mainboard cards
        - sideboard_count: Number of sideboard cards
        - errors: List of cards that couldn't be found
    """
    # Parse the deck string
    parsed_cards = parse_deck(deck)
    
    if not parsed_cards:
        return {
            "card_details": [],
            "mainboard_count": 0,
            "sideboard_count": 0,
            "errors": ["Empty or invalid deck"]
        }
    
    # Fetch card data from database
    card_names = [card["card_name"] for card in parsed_cards]
    
    query = """
        SELECT name, oracle_text, type_line, mana_cost, cmc, color_identity, rulings
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
            "color_identity": row[5],
            "rulings": row[6] if row[6] else ""
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
def generate_deck_matchup_strategy(
    card_details: list,
    archetype: str,
    opponent_archetype: str,
    matchup_stats: Optional[dict] = None
) -> dict:
    """
    Generate AI-powered coaching for a specific matchup given a user's deck.
    
    Use this after getting matchup stats to provide actionable advice.
    This creates PERSONALIZED strategy based on the user's exact deck.

    Args:
        card_details: List of enriched card objects from get_enriched_deck
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
    
    # Prepare full deck with all required card details for LLM
    deck_details = _format_full_deck(card_details)
    
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
        deck_details=deck_details,
        matchup_context=matchup_context
    )
    
    # Call LLM (using environment variable for now, will be configurable later)
    try:
        # Import LLM client factory
        import os
        from src.clients.llm_client import get_llm_client
        
        # Get model name and provider from environment
        model_name = os.getenv("LLM_MODEL")
        model_provider = os.getenv("LLM_PROVIDER")
        
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


def _format_card_list(card_details: list) -> str:
    """Format a list of card detail dicts for display in prompt."""
    lines = []
    for card in card_details:
        quantity = card.get("quantity", 1)
        name = card.get("name", "Unknown")
        card_type = card.get("type_line", "")
        lines.append(f"  {quantity}x {name} ({card_type})")
    return "\n".join(lines)


def _format_full_deck(card_details: list) -> str:
    """Format full deck with all required card details for LLM prompt.
    
    Includes: name, quantity, oracle_text, rulings, type_line, color_identity, mana_cost, cmc, section
    """
    lines = []
    for card in card_details:
        quantity = card.get("quantity", 1)
        name = card.get("name", "Unknown")
        section = card.get("section", "mainboard")
        oracle_text = card.get("oracle_text", "")
        rulings = card.get("rulings", "")
        type_line = card.get("type_line", "")
        color_identity = card.get("color_identity", [])
        mana_cost = card.get("mana_cost", "")
        cmc = card.get("cmc", 0)
        
        # Format color identity
        color_str = "".join(color_identity) if color_identity else "Colorless"
        
        lines.append(f"  {quantity}x {name} [{section}]")
        lines.append(f"    Type: {type_line}")
        lines.append(f"    Mana Cost: {mana_cost} (CMC: {cmc})")
        lines.append(f"    Color Identity: {color_str}")
        if oracle_text:
            lines.append(f"    Oracle Text: {oracle_text}")
        if rulings:
            lines.append(f"    Rulings: {rulings}")
        lines.append("")  # Empty line between cards
    
    return "\n".join(lines)


# ============================================================================
# Shared Helper Functions for Deck Optimization
# ============================================================================

def _get_legal_cards_for_format(format: str):
    """
    Query legal cards for the given format, filtered to commonly-played cards.
    
    Args:
        format: Format name (will be normalized to lowercase)
    
    Returns:
        List of dicts with card_id, name, oracle_text, type_line, mana_cost, cmc, color_identity
        
    Raises:
        ValueError: If legality data is unavailable or format is invalid
    """
    # Normalize format to lowercase for database query
    normalized_format = format.lower()
    
    # Query cards table for legal cards in this format
    # Join with deck_cards to filter to commonly-played cards (last 180 days)
    query = """
        SELECT DISTINCT 
            c.id as card_id,
            c.name,
            c.oracle_text,
            c.type_line,
            c.mana_cost,
            c.cmc,
            c.color_identity
        FROM cards c
        INNER JOIN deck_cards dc ON c.id = dc.card_id
        INNER JOIN decklists d ON dc.decklist_id = d.id
        INNER JOIN tournaments t ON d.tournament_id = t.id
        WHERE c.legalities->>%s = 'legal'
          AND t.start_date >= CURRENT_DATE - INTERVAL '180 days'
        ORDER BY c.name
    """
    
    try:
        with DatabaseConnection.get_cursor() as cursor:
            cursor.execute(query, (normalized_format,))
            results = cursor.fetchall()
            
            if not results:
                raise ValueError(
                    f"No legal cards found for format '{format}'. "
                    "This may indicate missing legality data or an invalid format name."
                )
            
            # Convert to list of card detail dicts
            legal_card_details = []
            for row in results:
                legal_card_details.append({
                    "card_id": row[0],
                    "name": row[1],
                    "oracle_text": row[2] if row[2] else "",
                    "type_line": row[3],
                    "mana_cost": row[4],
                    "cmc": row[5],
                    "color_identity": row[6]
                })
            
            return legal_card_details
    
    except Exception as e:
        logger.error(f"Error querying legal cards for format '{format}': {e}")
        raise ValueError(f"Failed to retrieve legal cards: {str(e)}")


def _determine_deck_color_identity(card_details: list) -> set:
    """
    Determine the color identity of a deck from its card list.
    
    Args:
        card_details: List of card dicts with color_identity field
    
    Returns:
        Set of color letters present in the deck (e.g., {'W', 'U', 'B'})
    """
    deck_colors = set()
    
    for card in card_details:
        color_identity = card.get("color_identity", [])
        if color_identity:
            deck_colors.update(color_identity)
    
    return deck_colors


def _filter_cards_by_color_identity(card_details: list, deck_colors: set) -> list:
    """
    Filter a list of cards to those castable with the given color identity.
    
    Includes:
    - Cards where color_identity is a subset of deck_colors
    - Colorless cards (empty color_identity)
    - Cards with only generic mana costs
    - Cards with phyrexian mana (treated as colorless/life payment)
    
    Args:
        card_details: List of card detail dicts with color_identity and mana_cost fields
        deck_colors: Set of color letters to filter by (e.g., {'W', 'U', 'B'})
    
    Returns:
        Filtered list of card details matching the color identity constraints
    """
    import re
    
    filtered = []
    
    for card in card_details:
        color_identity = card.get("color_identity", [])
        mana_cost = card.get("mana_cost", "")
        
        # Always include colorless cards
        if not color_identity:
            filtered.append(card)
            continue
        
        # Strip phyrexian mana symbols from mana_cost to determine actual color requirements
        # Phyrexian mana can be paid with life, so {W/P} doesn't require white sources
        stripped_cost = re.sub(r'\{[WUBRG]/P\}', '', mana_cost)
        
        # Check if card has only generic mana after stripping phyrexian
        generic_only = bool(re.match(r'^(\{[0-9X]\})*$', stripped_cost.strip()))
        
        if generic_only:
            filtered.append(card)
            continue
        
        # Include if card's color identity is a subset of deck colors
        if set(color_identity).issubset(deck_colors):
            filtered.append(card)
    
    return filtered


def _fetch_archetype_decks(
    archetype_group_ids: list,
    format: str,
    limit_per_archetype: int = 5
) -> dict:
    """
    Fetch recent decks for each archetype.
    
    Args:
        archetype_group_ids: List of archetype group IDs
        format: Tournament format
        limit_per_archetype: Max decks per archetype (default: 5)
    
    Returns:
        Dict mapping archetype_group_id to list of decks with card details:
        {
            archetype_id: [
                {
                    "decklist_id": int,  # DB primary key
                    "player": str,
                    "tournament_date": str,
                    "cards": [
                        {
                            "name": str,
                            "quantity": int,
                            "section": str (mainboard/sideboard)
                        }
                    ]
                }
            ]
        }
    """
    query = """
        WITH ranked_decklists AS (
            SELECT 
                d.id as decklist_id,
                d.archetype_group_id,
                d.player,
                t.start_date,
                ROW_NUMBER() OVER (
                    PARTITION BY d.archetype_group_id 
                    ORDER BY t.start_date DESC
                ) as rn
            FROM decklists d
            INNER JOIN tournaments t ON d.tournament_id = t.id
            WHERE d.archetype_group_id = ANY(%s)
              AND LOWER(t.format) = %s
        )
        SELECT 
            rd.decklist_id,
            rd.archetype_group_id,
            rd.player,
            rd.start_date,
            dc.quantity,
            dc.section,
            c.name
        FROM ranked_decklists rd
        INNER JOIN deck_cards dc ON rd.decklist_id = dc.decklist_id
        INNER JOIN cards c ON dc.card_id = c.id
        WHERE rd.rn <= %s
        ORDER BY rd.archetype_group_id, rd.start_date DESC, dc.section, c.name
    """
    
    result = {}
    
    try:
        with DatabaseConnection.get_cursor() as cursor:
            cursor.execute(
                query,
                (archetype_group_ids, format.lower(), limit_per_archetype)
            )
            rows = cursor.fetchall()
            
            # Group by deck
            decks_by_id = {}
            for row in rows:
                decklist_id = row[0]  # DB primary key
                archetype_id = row[1]
                player = row[2]
                tournament_date = row[3]
                quantity = row[4]
                section = row[5]
                card_name = row[6]
                
                if decklist_id not in decks_by_id:
                    decks_by_id[decklist_id] = {
                        "decklist_id": decklist_id,  # DB primary key
                        "archetype_group_id": archetype_id,
                        "player": player,
                        "tournament_date": str(tournament_date),
                        "cards": []
                    }
                
                decks_by_id[decklist_id]["cards"].append({
                    "name": card_name,
                    "quantity": quantity,
                    "section": section
                })
            
            # Group by archetype
            for deck in decks_by_id.values():
                archetype_id = deck["archetype_group_id"]
                if archetype_id not in result:
                    result[archetype_id] = []
                result[archetype_id].append(deck)
        
        return result
    
    except Exception as e:
        logger.error(f"Error fetching archetype decks: {e}")
        return {}


def _format_archetype_decks_for_prompt(
    archetype_decks: dict,
    archetype_metadata: list,
    include_sideboard: bool = False
) -> str:
    """
    Format archetype decks into readable text for LLM prompt.
    
    Args:
        archetype_decks: Dict from _fetch_archetype_decks
        archetype_metadata: List of archetype info dicts with name, meta_share
        include_sideboard: Whether to include sideboard cards (for sideboard optimization)
    
    Returns:
        Formatted string with archetype decks
    """
    lines = []
    
    # Create lookup for archetype metadata
    metadata_lookup = {
        arch["archetype_group_id"]: arch 
        for arch in archetype_metadata
    }
    
    for archetype_id, decks in archetype_decks.items():
        metadata = metadata_lookup.get(archetype_id, {})
        archetype_name = metadata.get("archetype", f"Archetype {archetype_id}")
        meta_share = metadata.get("meta_share")
        
        lines.append(f"\n## {archetype_name}")
        if meta_share:
            lines.append(f"Meta Share: {meta_share:.1f}%")
        
        lines.append(f"\nSample Decks ({len(decks)} recent):")
        
        for i, deck in enumerate(decks, 1):
            lines.append(f"\n### Sample {i} - {deck['player']} ({deck['tournament_date']})")
            
            # Group card details by section
            mainboard_cards = [c for c in deck["cards"] if c["section"] == "mainboard"]
            sideboard_cards = [c for c in deck["cards"] if c["section"] == "sideboard"]
            
            lines.append("\nMainboard:")
            for card_detail in mainboard_cards:
                lines.append(f"  {card_detail['quantity']}x {card_detail['name']}")
            
            if include_sideboard and sideboard_cards:
                lines.append("\nSideboard:")
                for card_detail in sideboard_cards:
                    lines.append(f"  {card_detail['quantity']}x {card_detail['name']}")
    
    return "\n".join(lines)


def _format_card_details_by_type(card_details: list, max_cards: int = 500) -> str:
    """
    Format a list of card details into concise text for LLM context.
    
    Groups cards by type to reduce output size and improve readability.
    
    Args:
        card_details: List of card detail dicts with name, oracle_text, mana_cost, and type_line fields
        max_cards: Maximum number of cards to include (default: 500)
    
    Returns:
        Formatted string for prompt inclusion, grouped by card type
    """
    lines = []
    
    # Group by card type
    creatures = []
    instants = []
    sorceries = []
    artifacts = []
    enchantments = []
    planeswalkers = []
    lands = []
    other = []
    
    for card in card_details[:max_cards]:
        type_line = card.get("type_line", "").lower()
        oracle_text = card.get("oracle_text", "")
        card_entry = f"{card['name']} ({card.get('mana_cost', '')}) - {card.get('type_line', '')}"
        if oracle_text:
            card_entry += f"\n  {oracle_text}"
        
        if "creature" in type_line:
            creatures.append(card_entry)
        elif "instant" in type_line:
            instants.append(card_entry)
        elif "sorcery" in type_line:
            sorceries.append(card_entry)
        elif "artifact" in type_line:
            artifacts.append(card_entry)
        elif "enchantment" in type_line:
            enchantments.append(card_entry)
        elif "planeswalker" in type_line:
            planeswalkers.append(card_entry)
        elif "land" in type_line:
            lands.append(card_entry)
        else:
            other.append(card_entry)
    
    if creatures:
        lines.append("\n### Creatures")
        lines.extend(creatures)
    
    if instants:
        lines.append("\n### Instants")
        lines.extend(instants)
    
    if sorceries:
        lines.append("\n### Sorceries")
        lines.extend(sorceries)
    
    if artifacts:
        lines.append("\n### Artifacts")
        lines.extend(artifacts)
    
    if enchantments:
        lines.append("\n### Enchantments")
        lines.extend(enchantments)
    
    if planeswalkers:
        lines.append("\n### Planeswalkers")
        lines.extend(planeswalkers)
    
    if lands:
        lines.append("\n### Lands")
        lines.extend(lands)
    
    if other:
        lines.append("\n### Other")
        lines.extend(other)
    
    return "\n".join(lines)


# ============================================================================
# Deck Optimization MCP Tools
# ============================================================================

@mcp.tool()
def optimize_mainboard(
    card_details: list,
    archetype: str,
    format: str,
    top_n: int = 5
) -> dict:
    """
    Optimize a deck's mainboard by identifying flex spots and recommending replacements.
    
    Analyzes the deck against the top N most frequent archetypes in the format,
    identifies non-essential cards (flex spots), and recommends format-legal
    replacements that improve matchups against the current meta.
    
    Args:
        card_details: List of enriched card objects from get_enriched_deck
        archetype: Your deck's archetype name
        format: Tournament format (e.g., "Modern", "Pioneer")
        top_n: Number of top archetypes to optimize against (default: 5)
    
    Returns:
        Dictionary with:
        - flex_spots: List of non-essential cards that can be replaced
        - recommendations: Suggested replacement cards with justifications
        - error: Error message if optimization fails
    """
    from src.app.mcp.prompts.mainboard_optimization_prompt import MAINBOARD_OPTIMIZATION_PROMPT_TEMPLATE
    from src.clients.llm_client import get_llm_client
    import os
    import json
    
    try:
        # Normalize format parameter
        normalized_format = format.lower()
        
        # Get top N archetypes from meta
        from . import meta_research_tools
        meta_result = meta_research_tools.get_format_meta_rankings.fn(format=normalized_format, days=30)
        
        if not meta_result.get("rankings"):
            return {
                "error": "Insufficient meta data available for this format",
                "archetype": archetype,
                "format": format
            }
        
        top_archetypes = meta_result["rankings"][:top_n]
        
        # Get format-legal, commonly-played cards (180-day filter)
        try:
            format_legal_card_details = _get_legal_cards_for_format(normalized_format)
        except ValueError as e:
            return {
                "error": f"Card legality data unavailable: {str(e)}",
                "archetype": archetype,
                "format": format
            }
        
        # Determine deck color identity
        deck_colors = _determine_deck_color_identity(card_details)
        
        # Filter to color-appropriate cards (subset of deck's colors + colorless)
        color_filtered_card_details = _filter_cards_by_color_identity(format_legal_card_details, deck_colors)
        
        # Extract archetype group IDs
        archetype_group_ids = [arch["archetype_group_id"] for arch in top_archetypes]
        
        # Fetch recent decks for top archetypes
        archetype_decks = _fetch_archetype_decks(
            archetype_group_ids=archetype_group_ids,
            format=normalized_format,
            limit_per_archetype=5
        )
        
        # Format full deck (main+side) with all required card details
        deck_details = _format_full_deck(card_details)
        
        # Format archetype decks (mainboard only)
        archetype_text = _format_archetype_decks_for_prompt(
            archetype_decks=archetype_decks,
            archetype_metadata=top_archetypes,
            include_sideboard=False
        )
        
        # Format available card pool (legal + commonly-played + color-filtered, max 500)
        available_cards_text = _format_card_details_by_type(color_filtered_card_details)
        
        # Build prompt
        prompt = MAINBOARD_OPTIMIZATION_PROMPT_TEMPLATE.format(
            format=normalized_format,
            archetype=archetype,
            deck_details=deck_details,
            top_n=top_n,
            top_n_archetype_decks=archetype_text,
            available_card_pool=available_cards_text
        )
        
        # Call LLM
        model_name = os.getenv("LLM_MODEL")
        model_provider = os.getenv("LLM_PROVIDER")
        llm = get_llm_client(model_name, model_provider)
        response = llm.run(prompt)
        
        # Parse JSON response
        try:
            result = json.loads(response.text)
            return {
                "archetype": archetype,
                "format": format,
                "top_n": top_n,
                "flex_spots": result.get("flex_spots", []),
                "recommendations": result.get("recommendations", [])
            }
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM JSON response: {e}")
            return {
                "error": "Failed to parse optimization recommendations",
                "raw_response": response.text,
                "archetype": archetype,
                "format": format
            }
    
    except Exception as e:
        logger.error(f"Error in optimize_mainboard: {e}")
        return {
            "error": str(e),
            "archetype": archetype,
            "format": format
        }


@mcp.tool()
def optimize_sideboard(
    card_details: list,
    archetype: str,
    format: str,
    top_n: int = 5
) -> dict:
    """
    Optimize a deck's sideboard to better answer the top N meta archetypes.
    
    Analyzes the sideboard against the most frequent archetypes, considering
    opponent sideboard plans in post-board games. Recommends additions,
    removals, and provides sideboard guides for each matchup.
    
    Args:
        card_details: List of enriched card objects from get_enriched_deck
        archetype: Your deck's archetype name
        format: Tournament format (e.g., "Modern", "Pioneer")
        top_n: Number of top archetypes to optimize against (default: 5)
    
    Returns:
        Dictionary with:
        - sideboard_changes: Recommended additions and removals
        - sideboard_plans: Game 2/3 plans for each top archetype
        - final_sideboard: Complete 15-card sideboard
        - error: Error message if optimization fails
    """
    from src.app.mcp.prompts.sideboard_optimization_prompt import SIDEBOARD_OPTIMIZATION_PROMPT_TEMPLATE
    from src.clients.llm_client import get_llm_client
    import os
    import json
    
    try:
        # Normalize format parameter
        normalized_format = format.lower()
        
        # Get top N archetypes from meta
        from . import meta_research_tools
        meta_result = meta_research_tools.get_format_meta_rankings.fn(format=normalized_format, days=30)
        
        if not meta_result.get("rankings"):
            return {
                "error": "Insufficient meta data available for this format",
                "archetype": archetype,
                "format": format
            }
        
        top_archetypes = meta_result["rankings"][:top_n]
        
        # Get format-legal, commonly-played cards (180-day filter)
        try:
            format_legal_card_details = _get_legal_cards_for_format(normalized_format)
        except ValueError as e:
            return {
                "error": f"Card legality data unavailable: {str(e)}",
                "archetype": archetype,
                "format": format
            }
        
        # Determine deck color identity
        deck_colors = _determine_deck_color_identity(card_details)
        
        # Filter to color-appropriate cards (subset of deck's colors + colorless)
        color_filtered_card_details = _filter_cards_by_color_identity(format_legal_card_details, deck_colors)
        
        # Extract archetype group IDs
        archetype_group_ids = [arch["archetype_group_id"] for arch in top_archetypes]
        
        # Fetch recent decks for top archetypes (including sideboards)
        archetype_decks = _fetch_archetype_decks(
            archetype_group_ids=archetype_group_ids,
            format=normalized_format,
            limit_per_archetype=5
        )
        
        # Format full deck (main+side) with all required card details
        deck_details = _format_full_deck(card_details)
        
        # Format archetype decks (include sideboards)
        archetype_text = _format_archetype_decks_for_prompt(
            archetype_decks=archetype_decks,
            archetype_metadata=top_archetypes,
            include_sideboard=True
        )
        
        # Format available card pool (legal + commonly-played + color-filtered, max 500)
        available_cards_text = _format_card_details_by_type(color_filtered_card_details)
        
        # Build prompt
        prompt = SIDEBOARD_OPTIMIZATION_PROMPT_TEMPLATE.format(
            format=normalized_format,
            archetype=archetype,
            deck_details=deck_details,
            top_n=top_n,
            top_n_archetype_decks=archetype_text,
            available_card_pool=available_cards_text
        )
        
        # Call LLM with retry logic for 15-card validation
        model_name = os.getenv("LLM_MODEL")
        model_provider = os.getenv("LLM_PROVIDER")
        llm = get_llm_client(model_name, model_provider)
        
        max_retries = 2
        for attempt in range(max_retries + 1):
            response = llm.run(prompt)
            
            # Parse JSON response
            try:
                result = json.loads(response.text)
                final_sideboard = result.get("final_sideboard", [])
                
                # Validate exactly 15 cards
                total_cards = sum(card.get("quantity", 0) for card in final_sideboard)
                
                if total_cards == 15:
                    return {
                        "archetype": archetype,
                        "format": format,
                        "top_n": top_n,
                        "sideboard_changes": result.get("sideboard_changes", []),
                        "sideboard_plans": result.get("sideboard_plans", []),
                        "final_sideboard": final_sideboard
                    }
                else:
                    logger.warning(
                        f"Sideboard validation failed (attempt {attempt + 1}): "
                        f"{total_cards} cards instead of 15"
                    )
                    
                    if attempt < max_retries:
                        # Retry with explicit requirement
                        prompt += f"\n\nCRITICAL: Your previous response had {total_cards} cards. The final_sideboard MUST contain exactly 15 cards total. Please recalculate."
                    else:
                        # Max retries reached
                        return {
                            "error": f"Failed to generate valid 15-card sideboard after {max_retries + 1} attempts",
                            "sideboard_changes": result.get("sideboard_changes", []),
                            "sideboard_plans": result.get("sideboard_plans", []),
                            "final_sideboard": final_sideboard,
                            "validation_error": f"Total cards: {total_cards}, expected: 15",
                            "archetype": archetype,
                            "format": format
                        }
            
            except json.JSONDecodeError as e:
                if attempt < max_retries:
                    logger.warning(f"Failed to parse JSON (attempt {attempt + 1}): {e}")
                    prompt += "\n\nYour previous response was not valid JSON. Please respond with properly formatted JSON only."
                else:
                    logger.error(f"Failed to parse LLM JSON response after {max_retries + 1} attempts: {e}")
                    return {
                        "error": "Failed to parse optimization recommendations",
                        "raw_response": response.text,
                        "archetype": archetype,
                        "format": format
                    }
    
    except Exception as e:
        logger.error(f"Error in optimize_sideboard: {e}")
        return {
            "error": str(e),
            "archetype": archetype,
            "format": format
        }
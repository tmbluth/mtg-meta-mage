"""Data filtering logic for TopDeck tournament data"""

from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


# Commander formats to exclude
COMMANDER_FORMATS = {
    'EDH',
    'Pauper EDH',
    'Duel Commander',
    'Tiny Leaders',
    'EDH Draft',
    'Oathbreaker'
}


def is_commander_format(format_name: str) -> bool:
    """
    Check if a format is a Commander format
    
    Args:
        format_name: Format name from API
    
    Returns:
        True if format is a Commander format, False otherwise
    """
    if not format_name:
        return False
    
    return format_name.strip() in COMMANDER_FORMATS


def should_include_tournament(tournament: Dict) -> bool:
    """
    Determine if a tournament should be included in the database
    
    Args:
        tournament: Tournament dictionary from API
    
    Returns:
        True if tournament should be included, False otherwise
    """
    format_name = tournament.get('format', '')
    
    # Exclude Commander formats
    if is_commander_format(format_name):
        logger.debug(f"Excluding Commander tournament: {tournament.get('TID')} - {format_name}")
        return False
    
    # Must be Magic: The Gathering
    game = tournament.get('game', '')
    if game != 'Magic: The Gathering':
        logger.debug(f"Excluding non-MTG tournament: {tournament.get('TID')} - {game}")
        return False
    
    return True


def is_valid_match(table_data: Dict) -> bool:
    """
    Check if a match table is a valid 1v1 match
    
    Args:
        table_data: Table dictionary from API rounds endpoint
    
    Returns:
        True if valid 1v1 match, False otherwise
    """
    players = table_data.get('players', [])
    
    # # Skip Byes
    # if table_data.get('table') == 'Byes' or table_data.get('status') == 'Bye':
    #     return False
    
    if len(players) > 2:
        logger.debug(f"Skipping match with {len(players)} players (not 1v1)")
        return False
    
    return True


def filter_tournaments(tournaments: List[Dict]) -> List[Dict]:
    """
    Filter tournaments to exclude Commander formats
    
    Args:
        tournaments: List of tournament dictionaries
    
    Returns:
        Filtered list of tournaments
    """
    filtered = [t for t in tournaments if should_include_tournament(t)]
    excluded_count = len(tournaments) - len(filtered)
    if excluded_count > 0:
        logger.info(f"Filtered out {excluded_count} Commander/non-MTG tournaments")
    return filtered


def filter_rounds_data(rounds_data: List[Dict]) -> List[Dict]:
    """
    Filter rounds data to only include valid 1v1 matches
    
    Args:
        rounds_data: List of round dictionaries from API
    
    Returns:
        Filtered list of rounds with only valid 1v1 matches
    """
    filtered_rounds = []
    
    for round_data in rounds_data:
        round_number = round_data.get('round')
        tables = round_data.get('tables', [])
        
        # Filter tables to only include valid 1v1 matches
        valid_tables = [t for t in tables if is_valid_match(t)]
        
        if valid_tables:
            filtered_round = round_data.copy()
            filtered_round['tables'] = valid_tables
            filtered_rounds.append(filtered_round)
    
    return filtered_rounds


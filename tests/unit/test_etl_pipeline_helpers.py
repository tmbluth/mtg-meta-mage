"""Helper functions for creating test data in ETL pipeline tests"""


def create_mock_tournament(**kwargs):
    """
    Helper to create standardized tournament dictionaries for testing.
    
    Args:
        **kwargs: Override default values
    
    Returns:
        Dictionary with tournament data
    """
    defaults = {
        'TID': 'test123',
        'tournamentName': 'Test Tournament',
        'format': 'Standard',
        'game': 'Magic: The Gathering',
        'startDate': 1234567890,
        'swissNum': 5,
        'topCut': 8,
        'eventData': {
            'city': 'Test City',
            'state': 'TS'
        }
    }
    defaults.update(kwargs)
    return defaults


def create_mock_player(**kwargs):
    """
    Helper to create standardized player dictionaries for testing.
    
    Args:
        **kwargs: Override default values
    
    Returns:
        Dictionary with player data
    """
    defaults = {
        'id': 'player1',
        'name': 'Alice',
        'wins': 0,
        'winsSwiss': 0,
        'winsBracket': 0,
        'winRate': 0.0,
        'losses': 0,
        'lossesSwiss': 0,
        'lossesBracket': 0,
        'draws': 0,
        'points': 0,
        'standing': 1
    }
    defaults.update(kwargs)
    return defaults


def create_mock_round(**kwargs):
    """
    Helper to create standardized round dictionaries for testing.
    
    Args:
        **kwargs: Override default values
    
    Returns:
        Dictionary with round data
    """
    defaults = {
        'round': 1,
        'tables': []
    }
    defaults.update(kwargs)
    return defaults


def create_mock_table(**kwargs):
    """
    Helper to create standardized table/match dictionaries for testing.
    
    Args:
        **kwargs: Override default values
    
    Returns:
        Dictionary with table/match data
    """
    defaults = {
        'table': 1,
        'players': [
            {'id': 'p1', 'name': 'Player 1'},
            {'id': 'p2', 'name': 'Player 2'}
        ],
        'winner_id': 'p1',
        'status': 'complete'
    }
    defaults.update(kwargs)
    return defaults


def create_mock_card(**kwargs):
    """
    Helper to create standardized card dictionaries for testing.
    
    Args:
        **kwargs: Override default values
    
    Returns:
        Dictionary with card data
    """
    defaults = {
        'id': 'card1-uuid',
        'oracle_id': 'oracle1',
        'name': 'Lightning Bolt',
        'set': 'M21',
        'collector_number': '161',
        'oracle_text': 'Deal 3 damage to any target',
        'type_line': 'Instant',
        'mana_cost': '{R}',
        'cmc': 1.0,
        'color_identity': ['R'],
        'scryfall_uri': 'https://scryfall.com/card/m21/161',
        'rulings': []
    }
    defaults.update(kwargs)
    return defaults


def create_mock_bulk_data(cards=None):
    """
    Helper to create standardized Scryfall bulk data response.
    
    Args:
        cards: List of card dictionaries (defaults to single test card)
    
    Returns:
        Dictionary with bulk data structure
    """
    if cards is None:
        cards = [create_mock_card()]
    
    return {
        'data': cards,
        'file_path': '/path/to/test_data.json'
    }


def create_mock_decklist_entry(**kwargs):
    """
    Helper to create standardized decklist entry dictionaries for testing.
    
    Args:
        **kwargs: Override default values
    
    Returns:
        Dictionary with decklist entry data
    """
    defaults = {
        'quantity': 4,
        'card_name': 'Lightning Bolt',
        'section': 'mainboard'
    }
    defaults.update(kwargs)
    return defaults


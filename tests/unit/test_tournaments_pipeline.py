"""Unit tests for tournaments_pipeline module"""

import pytest
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch, call
from typing import Dict, List

from src.etl.tournaments_pipeline import TournamentsPipeline


@pytest.fixture
def mock_topdeck_client():
    """Create a mock TopDeckClient"""
    with patch('src.etl.tournaments_pipeline.TopDeckClient') as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        yield mock_client


@pytest.fixture
def mock_db_connection():
    """Create a mock database connection"""
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = None
    mock_cursor.fetchall.return_value = []
    return mock_conn


@pytest.fixture
def pipeline(mock_topdeck_client):
    """Create a TournamentsPipeline instance with mocked dependencies"""
    with patch('src.etl.tournaments_pipeline.DatabaseConnection.initialize_pool'):
        pipeline = TournamentsPipeline(api_key='test_key')
        return pipeline


def test_is_commander_format_returns_true_for_commander_formats(pipeline):
    """Test that commander formats are correctly identified"""
    assert pipeline.is_commander_format('EDH') is True
    assert pipeline.is_commander_format('Pauper EDH') is True
    assert pipeline.is_commander_format('Duel Commander') is True
    assert pipeline.is_commander_format('Tiny Leaders') is True
    assert pipeline.is_commander_format('EDH Draft') is True
    assert pipeline.is_commander_format('Oathbreaker') is True


def test_is_commander_format_returns_false_for_non_commander_formats(pipeline):
    """Test that non-commander formats return False"""
    assert pipeline.is_commander_format('Standard') is False
    assert pipeline.is_commander_format('Modern') is False
    assert pipeline.is_commander_format('Legacy') is False
    assert pipeline.is_commander_format('') is False
    assert pipeline.is_commander_format(None) is False


def test_is_commander_format_handles_whitespace(pipeline):
    """Test that commander format check handles whitespace correctly"""
    assert pipeline.is_commander_format(' EDH ') is True
    assert pipeline.is_commander_format('EDH') is True


def test_is_limited_format_returns_true_for_limited_formats(pipeline):
    """Test that limited formats are correctly identified"""
    assert pipeline.is_limited_format('Draft') is True
    assert pipeline.is_limited_format('Sealed') is True
    assert pipeline.is_limited_format('Limited') is True
    assert pipeline.is_limited_format('Booster Draft') is True
    assert pipeline.is_limited_format('Sealed Deck') is True
    assert pipeline.is_limited_format('Cube Draft') is True
    assert pipeline.is_limited_format('Team Draft') is True
    assert pipeline.is_limited_format('Team Sealed') is True


def test_is_limited_format_returns_false_for_non_limited_formats(pipeline):
    """Test that non-limited formats return False"""
    assert pipeline.is_limited_format('Standard') is False
    assert pipeline.is_limited_format('Modern') is False
    assert pipeline.is_limited_format('Legacy') is False
    assert pipeline.is_limited_format('') is False
    assert pipeline.is_limited_format(None) is False


def test_should_include_tournament_excludes_commander_formats(pipeline):
    """Test that commander format tournaments are excluded"""
    tournament = {
        'TID': '123',
        'format': 'EDH',
        'game': 'Magic: The Gathering'
    }
    assert pipeline.should_include_tournament(tournament) is False


def test_should_include_tournament_excludes_limited_formats(pipeline):
    """Test that limited format tournaments are excluded"""
    tournament = {
        'TID': '123',
        'format': 'Draft',
        'game': 'Magic: The Gathering'
    }
    assert pipeline.should_include_tournament(tournament) is False


def test_should_include_tournament_excludes_non_mtg_games(pipeline):
    """Test that non-MTG tournaments are excluded"""
    tournament = {
        'TID': '123',
        'format': 'Standard',
        'game': 'Pokemon'
    }
    assert pipeline.should_include_tournament(tournament) is False


def test_should_include_tournament_includes_valid_tournaments(pipeline):
    """Test that valid constructed MTG tournaments are included"""
    tournament = {
        'TID': '123',
        'format': 'Standard',
        'game': 'Magic: The Gathering'
    }
    assert pipeline.should_include_tournament(tournament) is True


def test_is_valid_match_returns_true_for_1v1_matches(pipeline):
    """Test that valid 1v1 matches return True"""
    table_data = {
        'players': [
            {'id': 'player1'},
            {'id': 'player2'}
        ]
    }
    assert pipeline.is_valid_match(table_data) is True


def test_is_valid_match_returns_false_for_multiplayer_matches(pipeline):
    """Test that matches with more than 2 players return False"""
    table_data = {
        'players': [
            {'id': 'player1'},
            {'id': 'player2'},
            {'id': 'player3'}
        ]
    }
    assert pipeline.is_valid_match(table_data) is False


def test_is_valid_match_handles_empty_players(pipeline):
    """Test that matches with no players return True (edge case)"""
    table_data = {'players': []}
    assert pipeline.is_valid_match(table_data) is True


def test_filter_tournaments_excludes_commander_and_limited(pipeline):
    """Test that filter_tournaments excludes commander and limited formats"""
    tournaments = [
        {'TID': '1', 'format': 'Standard', 'game': 'Magic: The Gathering'},
        {'TID': '2', 'format': 'EDH', 'game': 'Magic: The Gathering'},
        {'TID': '3', 'format': 'Draft', 'game': 'Magic: The Gathering'},
        {'TID': '4', 'format': 'Modern', 'game': 'Magic: The Gathering'},
        {'TID': '5', 'format': 'Standard', 'game': 'Pokemon'}
    ]
    
    filtered = pipeline.filter_tournaments(tournaments)
    
    assert len(filtered) == 2
    assert filtered[0]['TID'] == '1'
    assert filtered[1]['TID'] == '4'


def test_filter_tournaments_handles_empty_list(pipeline):
    """Test that filter_tournaments handles empty list"""
    filtered = pipeline.filter_tournaments([])
    assert filtered == []


def test_filter_rounds_data_filters_invalid_matches(pipeline):
    """Test that filter_rounds_data filters out invalid matches"""
    rounds_data = [
        {
            'round': 1,
            'tables': [
                {'players': [{'id': 'p1'}, {'id': 'p2'}]},
                {'players': [{'id': 'p3'}, {'id': 'p4'}, {'id': 'p5'}]}
            ]
        },
        {
            'round': 2,
            'tables': [
                {'players': [{'id': 'p6'}, {'id': 'p7'}]}
            ]
        }
    ]
    
    filtered = pipeline.filter_rounds_data(rounds_data)
    
    assert len(filtered) == 2
    assert len(filtered[0]['tables']) == 1
    assert len(filtered[1]['tables']) == 1


def test_filter_rounds_data_handles_empty_rounds(pipeline):
    """Test that filter_rounds_data handles empty rounds"""
    rounds_data = [
        {
            'round': 1,
            'tables': []
        }
    ]
    
    filtered = pipeline.filter_rounds_data(rounds_data)
    assert len(filtered) == 0


def test_insert_tournament_success(mock_db_connection, pipeline, mock_topdeck_client):
    """Test successful tournament insertion"""
    tournament = {
        'TID': '123',
        'tournamentName': 'Test Tournament',
        'format': 'Standard',
        'startDate': 1234567890,  # Unix timestamp - pipeline converts to datetime
        'swissNum': 5,
        'topCut': 8,
        'eventData': {
            'city': 'Seattle',
            'state': 'WA'
        }
    }
    
    with patch('src.etl.tournaments_pipeline.DatabaseConnection.transaction') as mock_transaction:
        mock_transaction.return_value.__enter__.return_value = mock_db_connection
        mock_transaction.return_value.__exit__.return_value = None
        
        pipeline.insert_tournament(tournament, mock_db_connection)
        
        # Verify execute was called with datetime object for start_date
        execute_call = mock_db_connection.cursor.return_value.execute
        assert execute_call.called
        call_args = execute_call.call_args[0][1]
        assert isinstance(call_args[3], type(None)) or isinstance(call_args[3], datetime)  # start_date


def test_insert_tournament_handles_missing_fields(mock_db_connection, pipeline):
    """Test tournament insertion with missing optional fields"""
    tournament = {
        'TID': '123',
        'tournamentName': 'Test Tournament',
        'format': 'Standard'
    }
    
    with patch('src.etl.tournaments_pipeline.DatabaseConnection.transaction'):
        pipeline.insert_tournament(tournament, mock_db_connection)
        
        # Should not raise an error
        mock_db_connection.cursor.return_value.execute.assert_called_once()


def test_insert_players_success(mock_db_connection, pipeline):
    """Test successful player insertion"""
    players = [
        {
            'id': 'p1',
            'name': 'Player 1',
            'wins': 5,
            'winsSwiss': 4,
            'winsBracket': 1,
            'winRate': 0.8,
            'losses': 2,
            'lossesSwiss': 1,
            'lossesBracket': 1,
            'draws': 0,
            'points': 15,
            'standing': 1
        },
        {
            'id': 'p2',
            'name': 'Player 2',
            'wins': 3,
            'successRate': 0.6
        }
    ]
    
    with patch('src.etl.tournaments_pipeline.execute_batch') as mock_batch:
        pipeline.insert_players('tournament_123', players, mock_db_connection)
        
        mock_batch.assert_called_once()
        call_args = mock_batch.call_args
        assert len(call_args[0][2]) == 2  # Two players in batch (execute_batch(cursor, query, data))


def test_insert_players_handles_empty_list(mock_db_connection, pipeline):
    """Test that insert_players handles empty player list"""
    with patch('src.etl.tournaments_pipeline.execute_batch') as mock_batch:
        pipeline.insert_players('tournament_123', [], mock_db_connection)
        
        mock_batch.assert_not_called()


def test_insert_decklists_success(mock_db_connection, pipeline):
    """Test successful decklist insertion"""
    players = [
        {'id': 'p1', 'decklist': '4 Lightning Bolt\n2 Mountain'},
        {'id': 'p2', 'decklist': '4 Counterspell\n2 Island'},
        {'id': 'p3'}  # No decklist
    ]
    
    with patch('src.etl.tournaments_pipeline.execute_batch') as mock_batch:
        pipeline.insert_decklists('tournament_123', players, mock_db_connection)
        
        mock_batch.assert_called_once()
        call_args = mock_batch.call_args
        assert len(call_args[0][2]) == 2  # Two decklists (execute_batch(cursor, query, data))


def test_insert_decklists_handles_empty_list(mock_db_connection, pipeline):
    """Test that insert_decklists handles empty player list"""
    with patch('src.etl.tournaments_pipeline.execute_batch') as mock_batch:
        pipeline.insert_decklists('tournament_123', [], mock_db_connection)
        
        mock_batch.assert_not_called()


def test_insert_deck_cards_success(mock_db_connection, pipeline):
    """Test successful deck card insertion"""
    decklist_text = '4 Lightning Bolt\n2 Mountain'
    
    with patch('src.etl.tournaments_pipeline.parse_deck') as mock_parse, \
         patch('src.etl.tournaments_pipeline.execute_batch') as mock_batch:
        
        mock_parse.return_value = [
            {'card_name': 'Lightning Bolt', 'quantity': 4, 'section': 'mainboard'},
            {'card_name': 'Mountain', 'quantity': 2, 'section': 'mainboard'}
        ]
        
        mock_db_connection.cursor.return_value.fetchone.return_value = ('decklist_123',)
        mock_db_connection.cursor.return_value.fetchone.side_effect = [
            ('decklist_123',),  # First call for decklist_id
            ('card_1',),  # Second call for Lightning Bolt
            ('card_2',)   # Third call for Mountain
        ]
        
        pipeline.insert_deck_cards('player_123', 'tournament_123', decklist_text, mock_db_connection)
        
        mock_parse.assert_called_once_with(decklist_text)
        mock_batch.assert_called_once()


def test_insert_deck_cards_handles_missing_decklist(mock_db_connection, pipeline):
    """Test that insert_deck_cards handles missing decklist"""
    mock_db_connection.cursor.return_value.fetchone.return_value = None
    
    pipeline.insert_deck_cards('player_123', 'tournament_123', 'non-empty-text', mock_db_connection)
    
    # Should check for decklist in database, then return early when not found
    mock_db_connection.cursor.return_value.execute.assert_called()


def test_insert_deck_cards_handles_missing_cards(mock_db_connection, pipeline):
    """Test that insert_deck_cards handles cards not found in database"""
    decklist_text = '4 Lightning Bolt\n2 Unknown Card'
    
    with patch('src.etl.tournaments_pipeline.parse_deck') as mock_parse, \
         patch('src.etl.tournaments_pipeline.execute_batch') as mock_batch, \
         patch('src.core_utils.find_fuzzy_card_match') as mock_fuzzy:
        
        mock_parse.return_value = [
            {'card_name': 'Lightning Bolt', 'quantity': 4, 'section': 'mainboard'},
            {'card_name': 'Unknown Card', 'quantity': 2, 'section': 'mainboard'}
        ]
        
        mock_fuzzy.return_value = None  # No fuzzy match found
        
        mock_db_connection.cursor.return_value.fetchone.side_effect = [
            ('decklist_123',),  # decklist_id
            ('card_1',),  # Lightning Bolt exact match found
            None,  # Unknown Card exact match not found
            None,  # Unknown Card front face match not found
            None,  # Unknown Card back face match not found
            None,  # Unknown Card case-insensitive match not found
        ]
        
        pipeline.insert_deck_cards('player_123', 'tournament_123', decklist_text, mock_db_connection)
        
        # Should still call execute_batch with only the found card
        mock_batch.assert_called_once()
        call_args = mock_batch.call_args
        assert len(call_args[0][2]) == 1  # Only one card found (execute_batch(cursor, query, data))


def test_insert_match_rounds_success(mock_db_connection, pipeline):
    """Test successful match rounds insertion"""
    rounds_data = [
        {
            'round': 1,
            'tables': [
                {
                    'table': 1,
                    'players': [
                        {'id': 'p1'},
                        {'id': 'p2'}
                    ],
                    'winner_id': 'p1',
                    'status': 'completed'
                }
            ]
        }
    ]
    
    # Mock existing players
    mock_db_connection.cursor.return_value.fetchall.return_value = [
        ('p1',),
        ('p2',)
    ]
    
    with patch('src.etl.tournaments_pipeline.execute_batch') as mock_batch:
        pipeline.insert_match_rounds('tournament_123', rounds_data, mock_db_connection)
        
        # Should call execute_batch twice: once for rounds, once for matches
        assert mock_batch.call_count == 2


def test_insert_match_rounds_filters_invalid_matches(mock_db_connection, pipeline):
    """Test that insert_match_rounds filters invalid matches"""
    rounds_data = [
        {
            'round': 1,
            'tables': [
                {
                    'table': 1,
                    'players': [
                        {'id': 'p1'},
                        {'id': 'p2'},
                        {'id': 'p3'}  # Invalid: 3 players
                    ]
                }
            ]
        }
    ]
    
    mock_db_connection.cursor.return_value.fetchall.return_value = []
    
    with patch('src.etl.tournaments_pipeline.execute_batch') as mock_batch:
        pipeline.insert_match_rounds('tournament_123', rounds_data, mock_db_connection)
        
        # Should only insert rounds, not matches (no valid matches)
        assert mock_batch.call_count == 1  # Only rounds


def test_insert_match_rounds_handles_string_rounds(mock_db_connection, pipeline):
    """Test that insert_match_rounds handles string round identifiers"""
    rounds_data = [
        {
            'round': 'Top 8',
            'tables': [
                {
                    'table': 1,
                    'players': [
                        {'id': 'p1'},
                        {'id': 'p2'}
                    ],
                    'winner_id': 'p1',
                    'status': 'completed'
                }
            ]
        }
    ]
    
    mock_db_connection.cursor.return_value.fetchall.return_value = [
        ('p1',),
        ('p2',)
    ]
    
    with patch('src.etl.tournaments_pipeline.execute_batch') as mock_batch:
        pipeline.insert_match_rounds('tournament_123', rounds_data, mock_db_connection)
        
        # Should handle string rounds correctly
        assert mock_batch.call_count == 2


def test_insert_all_success(pipeline, mock_topdeck_client, mock_db_connection):
    """Test successful insert_all operation"""
    tournament = {
        'TID': '123',
        'tournamentName': 'Test Tournament',
        'format': 'Standard',
        'startDate': 1234567890
    }
    
    tournament_details = {
        'standings': [
            {'id': 'p1', 'name': 'Player 1', 'decklist': '4 Lightning Bolt'}
        ]
    }
    
    rounds_data = [
        {
            'round': 1,
            'tables': [
                {
                    'table': 1,
                    'players': [{'id': 'p1'}, {'id': 'p2'}],
                    'winner_id': 'p1'
                }
            ]
        }
    ]
    
    mock_topdeck_client.get_tournament_details.return_value = tournament_details
    mock_topdeck_client.get_tournament_rounds.return_value = rounds_data
    
    with patch('src.etl.tournaments_pipeline.DatabaseConnection.transaction') as mock_transaction, \
         patch('src.etl.tournaments_pipeline.parse_deck') as mock_parse, \
         patch('src.etl.tournaments_pipeline.execute_batch'):
        
        mock_transaction.return_value.__enter__.return_value = mock_db_connection
        mock_transaction.return_value.__exit__.return_value = None
        
        mock_db_connection.cursor.return_value.fetchone.side_effect = [
            ('decklist_123',),  # For decklist lookup
            ('card_1',)  # For card lookup
        ]
        mock_db_connection.cursor.return_value.fetchall.return_value = [
            ('p1',),
            ('p2',)
        ]
        
        mock_parse.return_value = [
            {'card_name': 'Lightning Bolt', 'quantity': 4, 'section': 'mainboard'}
        ]
        
        result = pipeline.insert_all(tournament, include_rounds=True)
        
        assert result is True
        mock_topdeck_client.get_tournament_details.assert_called_once_with('123')
        mock_topdeck_client.get_tournament_rounds.assert_called_once_with('123')


def test_insert_all_handles_missing_tid(pipeline):
    """Test that insert_all handles missing TID"""
    tournament = {
        'tournamentName': 'Test Tournament'
    }
    
    result = pipeline.insert_all(tournament)
    assert result is False


def test_insert_deck_cards_matches_double_faced_cards(pipeline, mock_db_connection):
    """Test that insert_deck_cards can match double-faced cards by front face name"""
    player_id = 'test_player'
    tournament_id = 'test_tournament'
    decklist_text = '4 Fable of the Mirror-Breaker\n2 Sink into Stupor'
    
    # Mock cursor behavior
    mock_cursor = mock_db_connection.cursor.return_value
    
    # First call: get decklist_id
    mock_cursor.fetchone.side_effect = [
        ('decklist_123',),  # decklist_id lookup
        None,  # First card exact match fails
        ('card_fable',),  # First card front face match succeeds  
        None,  # Second card exact match fails
        ('card_sink',),  # Second card front face match succeeds
    ]
    
    from unittest.mock import patch
    with patch('src.etl.tournaments_pipeline.DatabaseConnection.transaction') as mock_transaction, \
         patch('src.etl.tournaments_pipeline.parse_deck') as mock_parse, \
         patch('src.etl.tournaments_pipeline.execute_batch'):
        
        mock_transaction.return_value.__enter__.return_value = mock_db_connection
        mock_transaction.return_value.__exit__.return_value = None
        
        mock_parse.return_value = [
            {'card_name': 'Fable of the Mirror-Breaker', 'quantity': 4, 'section': 'mainboard'},
            {'card_name': 'Sink into Stupor', 'quantity': 2, 'section': 'mainboard'}
        ]
        
        pipeline.insert_deck_cards(player_id, tournament_id, decklist_text, mock_db_connection)
        
        # Verify we tried to match cards (exact match + front face match for each card)
        assert mock_cursor.execute.call_count >= 5  # decklist lookup + 2 cards * 2 queries each


def test_insert_deck_cards_matches_back_face_cards(pipeline, mock_db_connection):
    """Test that insert_deck_cards can match double-faced cards by back face name"""
    player_id = 'test_player'
    tournament_id = 'test_tournament'
    decklist_text = '2 Soporific Springs\n1 Tear'
    
    # Mock cursor behavior
    mock_cursor = mock_db_connection.cursor.return_value
    
    mock_cursor.fetchone.side_effect = [
        ('decklist_123',),  # decklist_id lookup
        None,  # First card exact match fails
        None,  # First card front face match fails
        ('card_soporific',),  # First card back face match succeeds
        None,  # Second card exact match fails
        None,  # Second card front face match fails
        ('card_tear',),  # Second card back face match succeeds
    ]
    
    from unittest.mock import patch
    with patch('src.etl.tournaments_pipeline.DatabaseConnection.transaction') as mock_transaction, \
         patch('src.etl.tournaments_pipeline.parse_deck') as mock_parse, \
         patch('src.etl.tournaments_pipeline.execute_batch'):
        
        mock_transaction.return_value.__enter__.return_value = mock_db_connection
        mock_transaction.return_value.__exit__.return_value = None
        
        mock_parse.return_value = [
            {'card_name': 'Soporific Springs', 'quantity': 2, 'section': 'mainboard'},
            {'card_name': 'Tear', 'quantity': 1, 'section': 'mainboard'}
        ]
        
        pipeline.insert_deck_cards(player_id, tournament_id, decklist_text, mock_db_connection)
        
        # Verify we tried all three lookup methods for each card
        assert mock_cursor.execute.call_count >= 7  # decklist lookup + 2 cards * 3 queries each


def test_insert_all_handles_no_players(pipeline, mock_topdeck_client, mock_db_connection):
    """Test that insert_all handles tournaments with no players"""
    tournament = {
        'TID': '123',
        'tournamentName': 'Test Tournament',
        'format': 'Standard'
    }
    
    tournament_details = {'standings': []}
    mock_topdeck_client.get_tournament_details.return_value = tournament_details
    
    with patch('src.etl.tournaments_pipeline.DatabaseConnection.transaction') as mock_transaction:
        mock_transaction.return_value.__enter__.return_value = mock_db_connection
        mock_transaction.return_value.__exit__.return_value = None
        
        result = pipeline.insert_all(tournament, include_rounds=False)
        
        assert result is True


def test_load_initial_success(pipeline, mock_topdeck_client, mock_db_connection):
    """Test successful initial load"""
    tournaments = [
        {
            'TID': '1',
            'tournamentName': 'Tournament 1',
            'format': 'Standard',
            'game': 'Magic: The Gathering',
            'startDate': 1234567890  # Unix timestamp
        },
        {
            'TID': '2',
            'tournamentName': 'Tournament 2',
            'format': 'Modern',
            'game': 'Magic: The Gathering',
            'startDate': 1234567900  # Unix timestamp
        }
    ]
    
    mock_topdeck_client.get_tournaments.return_value = tournaments
    mock_topdeck_client.get_tournament_details.return_value = {'players': []}
    mock_topdeck_client.get_tournament_rounds.return_value = []
    
    with patch('src.etl.tournaments_pipeline.DatabaseConnection.transaction') as mock_transaction, \
         patch('src.etl.tournaments_pipeline.update_load_metadata') as mock_update_metadata, \
         patch('src.etl.tournaments_pipeline.execute_batch'):
        
        mock_transaction.return_value.__enter__.return_value = mock_db_connection
        mock_transaction.return_value.__exit__.return_value = None
        
        result = pipeline.load_initial(days_back=90)
        
        assert result['success'] is True
        assert result['objects_loaded'] == 2
        assert result['objects_processed'] == 2
        assert result['errors'] == 0
        mock_update_metadata.assert_called_once()
        # Verify metadata was called with datetime object
        call_args = mock_update_metadata.call_args
        assert isinstance(call_args[1]['last_timestamp'], datetime)


def test_load_initial_filters_tournaments(pipeline, mock_topdeck_client, mock_db_connection):
    """Test that load_initial filters out excluded tournaments"""
    tournaments = [
        {
            'TID': '1',
            'tournamentName': 'Standard Tournament',
            'format': 'Standard',
            'game': 'Magic: The Gathering',
            'startDate': 1234567890
        },
        {
            'TID': '2',
            'tournamentName': 'EDH Tournament',
            'format': 'EDH',
            'game': 'Magic: The Gathering',
            'startDate': 1234567900
        }
    ]
    
    mock_topdeck_client.get_tournaments.return_value = tournaments
    mock_topdeck_client.get_tournament_details.return_value = {'players': []}
    mock_topdeck_client.get_tournament_rounds.return_value = []
    
    with patch('src.etl.tournaments_pipeline.DatabaseConnection.transaction') as mock_transaction, \
         patch('src.etl.tournaments_pipeline.update_load_metadata') as mock_update_metadata, \
         patch('src.etl.tournaments_pipeline.execute_batch'):
        
        mock_transaction.return_value.__enter__.return_value = mock_db_connection
        mock_transaction.return_value.__exit__.return_value = None
        
        result = pipeline.load_initial(days_back=90)
        
        # Should only load 1 tournament (EDH filtered out)
        assert result['objects_loaded'] == 1
        assert result['objects_processed'] == 1


def test_load_incremental_success(pipeline, mock_topdeck_client, mock_db_connection):
    """Test successful incremental load"""
    tournaments = [
        {
            'TID': '1',
            'tournamentName': 'New Tournament',
            'format': 'Standard',
            'game': 'Magic: The Gathering',
            'startDate': 1234567890  # Unix timestamp
        }
    ]
    
    mock_topdeck_client.get_tournaments.return_value = tournaments
    mock_topdeck_client.get_tournament_details.return_value = {'players': []}
    mock_topdeck_client.get_tournament_rounds.return_value = []
    
    with patch('src.etl.tournaments_pipeline.DatabaseConnection.transaction') as mock_transaction, \
         patch('src.etl.tournaments_pipeline.get_last_load_timestamp') as mock_get_timestamp, \
         patch('src.etl.tournaments_pipeline.update_load_metadata') as mock_update_metadata, \
         patch('src.etl.tournaments_pipeline.execute_batch'):
        
        mock_get_timestamp.return_value = datetime.fromtimestamp(1234560000)
        mock_transaction.return_value.__enter__.return_value = mock_db_connection
        mock_transaction.return_value.__exit__.return_value = None
        
        result = pipeline.load_incremental()
        
        assert result['success'] is True
        assert result['objects_loaded'] == 1
        mock_update_metadata.assert_called_once()
        # Verify metadata was called with datetime object
        call_args = mock_update_metadata.call_args
        assert isinstance(call_args[1]['last_timestamp'], datetime)


def test_load_incremental_falls_back_to_initial(pipeline, mock_topdeck_client):
    """Test that incremental load falls back to initial if no previous load"""
    with patch('src.etl.tournaments_pipeline.get_last_load_timestamp') as mock_get_timestamp, \
         patch.object(pipeline, 'load_initial') as mock_load_initial:
        
        mock_get_timestamp.return_value = None
        mock_load_initial.return_value = {
            'success': True,
            'objects_loaded': 0,
            'objects_processed': 0,
            'errors': 0
        }
        
        result = pipeline.load_incremental()
        
        mock_load_initial.assert_called_once()
        assert result == mock_load_initial.return_value


def test_load_incremental_handles_no_new_tournaments(pipeline, mock_topdeck_client):
    """Test incremental load when no new tournaments exist"""
    mock_topdeck_client.get_tournaments.return_value = []
    
    with patch('src.etl.tournaments_pipeline.get_last_load_timestamp') as mock_get_timestamp:
        mock_get_timestamp.return_value = datetime.fromtimestamp(1234560000)
        
        result = pipeline.load_incremental()
        
        assert result['success'] is True
        assert result['objects_loaded'] == 0
        assert result['objects_processed'] == 0


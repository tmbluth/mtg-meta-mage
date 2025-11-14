"""Unit tests for ETL pipeline"""

import pytest
from unittest.mock import Mock, patch, MagicMock, call
from datetime import datetime, timedelta

from src.etl.etl_pipeline import (
    ETLPipeline,
    load_cards_from_bulk_data,
    parse_decklist,
    is_commander_format,
    is_limited_format,
    should_include_tournament,
    is_valid_match,
    filter_tournaments,
    filter_rounds_data,
    COMMANDER_FORMATS,
    LIMITED_FORMATS
)
from src.database.connection import DatabaseConnection


class TestETLPipelineInit:
    """Tests for ETLPipeline initialization"""
    
    def test_init_creates_topdeck_client(self):
        """Test that initialization creates TopDeck client"""
        with patch('src.etl.etl_pipeline.TopDeckClient') as mock_client_class:
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
                
                mock_client_class.assert_called_once_with("test_key")
    
    def test_init_initializes_database_pool(self):
        """Test that initialization sets up database connection pool"""
        with patch('src.etl.etl_pipeline.TopDeckClient'):
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool') as mock_init:
                pipeline = ETLPipeline()
                
                mock_init.assert_called_once()
    
    def test_init_uses_env_api_key_when_not_provided(self):
        """Test that API key from environment is used when not explicitly provided"""
        with patch('src.etl.etl_pipeline.TopDeckClient') as mock_client_class:
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline()
                
                mock_client_class.assert_called_once_with(None)


class TestGetLastLoadTimestamp:
    """Tests for get_last_load_timestamp method"""
    
    def test_get_last_load_timestamp_returns_timestamp(self):
        """Test that last load timestamp is retrieved correctly"""
        with patch('src.etl.etl_pipeline.TopDeckClient'):
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (1234567890,)
        
        with patch('src.etl.etl_pipeline.DatabaseConnection.get_cursor') as mock_get_cursor:
            mock_get_cursor.return_value.__enter__.return_value = mock_cursor
            
            result = pipeline.get_last_load_timestamp()
            
            assert result == 1234567890
    
    def test_get_last_load_timestamp_returns_none_when_no_data(self):
        """Test that None is returned when no previous loads exist"""
        with patch('src.etl.etl_pipeline.TopDeckClient'):
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = None
        
        with patch('src.etl.etl_pipeline.DatabaseConnection.get_cursor') as mock_get_cursor:
            mock_get_cursor.return_value.__enter__.return_value = mock_cursor
            
            result = pipeline.get_last_load_timestamp()
            
            assert result is None
    
    def test_get_last_load_timestamp_handles_error(self):
        """Test that errors are handled gracefully"""
        with patch('src.etl.etl_pipeline.TopDeckClient'):
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        with patch('src.etl.etl_pipeline.DatabaseConnection.get_cursor', side_effect=Exception("DB Error")):
            result = pipeline.get_last_load_timestamp()
            
            assert result is None


class TestUpdateLoadMetadata:
    """Tests for update_load_metadata method"""
    
    def test_update_load_metadata_inserts_record(self):
        """Test that load metadata is inserted correctly"""
        with patch('src.etl.etl_pipeline.TopDeckClient'):
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        mock_cursor = Mock()
        
        with patch('src.etl.etl_pipeline.DatabaseConnection.get_cursor') as mock_get_cursor:
            mock_get_cursor.return_value.__enter__.return_value = mock_cursor
            
            pipeline.update_load_metadata(1234567890, 10)
            
            mock_cursor.execute.assert_called_once()
            call_args = mock_cursor.execute.call_args
            assert 1234567890 in call_args[0][1]
            assert 10 in call_args[0][1]
            assert 'incremental' in call_args[0][1]
    
    def test_update_load_metadata_raises_on_error(self):
        """Test that errors are raised"""
        with patch('src.etl.etl_pipeline.TopDeckClient'):
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        with patch('src.etl.etl_pipeline.DatabaseConnection.get_cursor', side_effect=Exception("DB Error")):
            with pytest.raises(Exception, match="DB Error"):
                pipeline.update_load_metadata(1234567890, 10)


class TestInsertTournament:
    """Tests for insert_tournament method"""
    
    def test_insert_tournament_executes_insert(self):
        """Test that tournament is inserted with correct data"""
        with patch('src.etl.etl_pipeline.TopDeckClient'):
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        tournament = {
            'TID': 'test123',
            'tournamentName': 'Test Tournament',
            'format': 'Standard',
            'startDate': 1234567890,
            'swissNum': 5,
            'topCut': 8,
            'eventData': {
                'city': 'Test City',
                'state': 'TS'
            }
        }
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        pipeline.insert_tournament(tournament, mock_conn)
        
        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args[0]
        assert 'test123' in call_args[1]
        assert 'Test Tournament' in call_args[1]
        mock_cursor.close.assert_called_once()
    
    def test_insert_tournament_handles_missing_event_data(self):
        """Test that missing event data is handled gracefully"""
        with patch('src.etl.etl_pipeline.TopDeckClient'):
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        tournament = {
            'TID': 'test123',
            'tournamentName': 'Test Tournament',
            'format': 'Standard',
            'startDate': 1234567890
        }
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        pipeline.insert_tournament(tournament, mock_conn)
        
        mock_cursor.execute.assert_called_once()
    
    def test_insert_tournament_closes_cursor_on_error(self):
        """Test that cursor is closed even on error"""
        with patch('src.etl.etl_pipeline.TopDeckClient'):
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        tournament = {'TID': 'test123'}
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = Exception("DB Error")
        mock_conn.cursor.return_value = mock_cursor
        
        with pytest.raises(Exception):
            pipeline.insert_tournament(tournament, mock_conn)
        
        mock_cursor.close.assert_called_once()


class TestInsertPlayers:
    """Tests for insert_players method"""
    
    def test_insert_players_executes_batch_insert(self):
        """Test that players are inserted in batch"""
        with patch('src.etl.etl_pipeline.TopDeckClient'):
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        players = [
            {
                'id': 'player1',
                'name': 'Alice',
                'wins': 3,
                'winsSwiss': 3,
                'winsBracket': 0,
                'winRate': 0.75,
                'losses': 1,
                'lossesSwiss': 1,
                'lossesBracket': 0,
                'draws': 0,
                'points': 9,
                'standing': 1
            }
        ]
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        with patch('src.etl.etl_pipeline.execute_batch') as mock_execute_batch:
            pipeline.insert_players('test123', players, mock_conn)
            
            mock_execute_batch.assert_called_once()
            call_args = mock_execute_batch.call_args
            assert mock_cursor == call_args[0][0]
            assert len(call_args[0][2]) == 1
            assert call_args[0][2][0][0] == 'player1'
    
    def test_insert_players_handles_empty_list(self):
        """Test that empty player list is handled gracefully"""
        with patch('src.etl.etl_pipeline.TopDeckClient'):
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        mock_conn = Mock()
        
        # Should not raise error
        pipeline.insert_players('test123', [], mock_conn)
        
        # Should not create cursor for empty list
        mock_conn.cursor.assert_not_called()
    
    def test_insert_players_handles_missing_fields(self):
        """Test that missing player fields default correctly"""
        with patch('src.etl.etl_pipeline.TopDeckClient'):
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        players = [{'id': 'player1'}]  # Minimal player data
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        with patch('src.etl.etl_pipeline.execute_batch') as mock_execute_batch:
            pipeline.insert_players('test123', players, mock_conn)
            
            call_args = mock_execute_batch.call_args[0][2]
            player_data = call_args[0]
            assert player_data[0] == 'player1'
            assert player_data[2] == ''  # name defaults to empty string
            assert player_data[3] == 0   # wins defaults to 0


class TestInsertDecklists:
    """Tests for insert_decklists method"""
    
    def test_insert_decklists_executes_batch_insert(self):
        """Test that decklists are inserted in batch"""
        with patch('src.etl.etl_pipeline.TopDeckClient'):
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        players = [
            {'id': 'player1', 'decklist': '4 Lightning Bolt\n4 Counterspell'},
            {'id': 'player2', 'decklist': '4 Llanowar Elves'}
        ]
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        with patch('src.etl.etl_pipeline.execute_batch') as mock_execute_batch:
            pipeline.insert_decklists('test123', players, mock_conn)
            
            mock_execute_batch.assert_called_once()
            call_args = mock_execute_batch.call_args[0][2]
            assert len(call_args) == 2
            assert call_args[0][0] == 'player1'
    
    def test_insert_decklists_skips_players_without_decklist(self):
        """Test that players without decklists are skipped"""
        with patch('src.etl.etl_pipeline.TopDeckClient'):
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        players = [
            {'id': 'player1', 'decklist': '4 Lightning Bolt'},
            {'id': 'player2'}  # No decklist
        ]
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        with patch('src.etl.etl_pipeline.execute_batch') as mock_execute_batch:
            pipeline.insert_decklists('test123', players, mock_conn)
            
            call_args = mock_execute_batch.call_args[0][2]
            assert len(call_args) == 1
    
    def test_insert_decklists_handles_empty_list(self):
        """Test that empty player list is handled gracefully"""
        with patch('src.etl.etl_pipeline.TopDeckClient'):
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        mock_conn = Mock()
        
        # Should not raise error
        pipeline.insert_decklists('test123', [], mock_conn)
        
        mock_conn.cursor.assert_not_called()


class TestInsertMatchRounds:
    """Tests for insert_match_rounds method"""
    
    def test_insert_match_rounds_handles_numeric_rounds(self):
        """Test that numeric round numbers are handled correctly"""
        with patch('src.etl.etl_pipeline.TopDeckClient'):
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        rounds_data = [
            {
                'round': 1,
                'tables': [
                    {
                        'table': 1,
                        'players': [{'id': 'p1'}, {'id': 'p2'}],
                        'winner_id': 'p1',
                        'status': 'complete'
                    }
                ]
            }
        ]
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        # Mock existing players query
        mock_cursor.fetchall.return_value = [('p1',), ('p2',)]
        
        with patch('src.etl.etl_pipeline.execute_batch'):
            with patch('src.etl.etl_pipeline.is_valid_match', return_value=True):
                pipeline.insert_match_rounds('test123', rounds_data, mock_conn)
    
    def test_insert_match_rounds_handles_string_rounds(self):
        """Test that string round names are converted to numbers"""
        with patch('src.etl.etl_pipeline.TopDeckClient'):
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        rounds_data = [
            {
                'round': 'Top 8',
                'tables': [
                    {
                        'table': 1,
                        'players': [{'id': 'p1'}, {'id': 'p2'}],
                        'winner_id': 'p1',
                        'status': 'complete'
                    }
                ]
            }
        ]
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        # Mock existing players query
        mock_cursor.fetchall.return_value = [('p1',), ('p2',)]
        
        with patch('src.etl.etl_pipeline.execute_batch') as mock_execute_batch:
            with patch('src.etl.etl_pipeline.is_valid_match', return_value=True):
                pipeline.insert_match_rounds('test123', rounds_data, mock_conn)
                
                # Check that Top 8 was converted to 1000
                round_data_call = mock_execute_batch.call_args_list[0][0][2]
                assert round_data_call[0][0] == 1000
    
    def test_insert_match_rounds_filters_invalid_matches(self):
        """Test that invalid matches are filtered out"""
        with patch('src.etl.etl_pipeline.TopDeckClient'):
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        rounds_data = [
            {
                'round': 1,
                'tables': [
                    {'table': 1, 'players': [{'id': 'p1'}, {'id': 'p2'}]},
                    {'table': 2, 'players': [{'id': 'p3'}, {'id': 'p4'}, {'id': 'p5'}]}  # 3 players
                ]
            }
        ]
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        # Mock existing players query
        mock_cursor.fetchall.return_value = [('p1',), ('p2',)]
        
        with patch('src.etl.etl_pipeline.execute_batch') as mock_execute_batch:
            with patch('src.etl.etl_pipeline.is_valid_match', side_effect=[True, False]):
                pipeline.insert_match_rounds('test123', rounds_data, mock_conn)
                
                # Should have 2 execute_batch calls (rounds and matches)
                matches_call = mock_execute_batch.call_args_list[1][0][2]
                assert len(matches_call) == 1  # Only 1 valid match
    
    def test_insert_match_rounds_handles_empty_list(self):
        """Test that empty rounds list is handled gracefully"""
        with patch('src.etl.etl_pipeline.TopDeckClient'):
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        mock_conn = Mock()
        
        # Should not raise error
        pipeline.insert_match_rounds('test123', [], mock_conn)
        
        mock_conn.cursor.assert_not_called()
    
    def test_insert_match_rounds_skips_matches_with_missing_players(self):
        """Test that matches with players not in database are skipped to avoid FK violations"""
        with patch('src.etl.etl_pipeline.TopDeckClient'):
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        rounds_data = [
            {
                'round': 1,
                'tables': [
                    {
                        'table': 1,
                        'players': [
                            {'id': 'player1'},  # This player exists
                            {'id': 'player2'}   # This player exists
                        ],
                        'winner_id': 'player1',
                        'status': 'complete'
                    },
                    {
                        'table': 2,
                        'players': [
                            {'id': 'missing_player1'},  # This player doesn't exist
                            {'id': 'player2'}
                        ],
                        'winner_id': 'missing_player1',
                        'status': 'complete'
                    },
                    {
                        'table': 3,
                        'players': [
                            {'id': 'player1'},
                            {'id': 'missing_player2'}  # This player doesn't exist
                        ],
                        'winner_id': 'player1',
                        'status': 'complete'
                    }
                ]
            }
        ]
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock existing players query - only player1 and player2 exist
        mock_cursor.fetchall.return_value = [('player1',), ('player2',)]
        
        with patch('src.etl.etl_pipeline.execute_batch') as mock_execute_batch:
            with patch('src.etl.etl_pipeline.is_valid_match', return_value=True):
                pipeline.insert_match_rounds('test123', rounds_data, mock_conn)
                
                # Should only insert 1 match (table 1), skipping tables 2 and 3
                assert mock_execute_batch.call_count == 2  # One for rounds, one for matches
                matches_call = mock_execute_batch.call_args_list[1]
                match_data = matches_call[0][2]  # Third argument is the data
                assert len(match_data) == 1, "Should only insert 1 match with valid players"
                assert match_data[0][3] == 'player1'  # player1_id
                assert match_data[0][4] == 'player2'  # player2_id
    
    def test_insert_match_rounds_validates_player_existence(self):
        """Test that insert_match_rounds queries existing players before inserting matches"""
        with patch('src.etl.etl_pipeline.TopDeckClient'):
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        rounds_data = [
            {
                'round': 1,
                'tables': [
                    {
                        'table': 1,
                        'players': [{'id': 'player1'}, {'id': 'player2'}],
                        'winner_id': 'player1',
                        'status': 'complete'
                    }
                ]
            }
        ]
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [('player1',), ('player2',)]
        
        with patch('src.etl.etl_pipeline.execute_batch'):
            with patch('src.etl.etl_pipeline.is_valid_match', return_value=True):
                pipeline.insert_match_rounds('test123', rounds_data, mock_conn)
                
                # Verify that we queried for existing players
                execute_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
                player_query_found = any(
                    'SELECT player_id FROM players' in str(call) 
                    for call in execute_calls
                )
                assert player_query_found, "Should query for existing players before inserting matches"


class TestLoadTournament:
    """Tests for load_tournament method"""
    
    def test_load_tournament_success(self):
        """Test successful tournament load"""
        with patch('src.etl.etl_pipeline.TopDeckClient') as mock_client_class:
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                mock_client = Mock()
                mock_client_class.return_value = mock_client
                
                pipeline = ETLPipeline(api_key="test_key")
                pipeline.client = mock_client
        
        tournament = {'TID': 'test123', 'tournamentName': 'Test'}
        
        mock_client.get_tournament_details.return_value = {
            'players': [{'id': 'p1', 'name': 'Alice'}]
        }
        mock_client.get_tournament_rounds.return_value = [
            {'round': 1, 'tables': []}
        ]
        
        mock_conn = Mock()
        
        with patch('src.etl.etl_pipeline.DatabaseConnection.transaction') as mock_transaction:
            mock_transaction.return_value.__enter__.return_value = mock_conn
            with patch.object(pipeline, 'insert_tournament'):
                with patch.object(pipeline, 'insert_players'):
                    with patch.object(pipeline, 'insert_decklists'):
                        with patch.object(pipeline, 'insert_match_rounds'):
                            with patch('src.etl.etl_pipeline.filter_rounds_data', return_value=[{'round': 1}]):
                                result = pipeline.load_tournament(tournament, include_rounds=True)
                                
                                assert result is True
    
    def test_load_tournament_returns_false_on_missing_tid(self):
        """Test that missing TID returns False"""
        with patch('src.etl.etl_pipeline.TopDeckClient'):
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        tournament = {'tournamentName': 'Test'}  # No TID
        
        result = pipeline.load_tournament(tournament)
        
        assert result is False
    
    def test_load_tournament_handles_error(self):
        """Test that errors are handled gracefully"""
        with patch('src.etl.etl_pipeline.TopDeckClient'):
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        tournament = {'TID': 'test123'}
        
        with patch('src.etl.etl_pipeline.DatabaseConnection.transaction', side_effect=Exception("DB Error")):
            result = pipeline.load_tournament(tournament)
            
            assert result is False
    
    def test_load_tournament_skips_rounds_when_not_requested(self):
        """Test that rounds are skipped when include_rounds=False"""
        with patch('src.etl.etl_pipeline.TopDeckClient') as mock_client_class:
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                mock_client = Mock()
                mock_client_class.return_value = mock_client
                
                pipeline = ETLPipeline(api_key="test_key")
                pipeline.client = mock_client
        
        tournament = {'TID': 'test123'}
        
        mock_client.get_tournament_details.return_value = None
        mock_conn = Mock()
        
        with patch('src.etl.etl_pipeline.DatabaseConnection.transaction') as mock_transaction:
            mock_transaction.return_value.__enter__.return_value = mock_conn
            with patch.object(pipeline, 'insert_tournament'):
                pipeline.load_tournament(tournament, include_rounds=False)
                
                # Should not call get_tournament_rounds
                mock_client.get_tournament_rounds.assert_not_called()


class TestLoadInitial:
    """Tests for load_initial method"""
    
    def test_load_initial_fetches_tournaments_for_multiple_formats(self):
        """Test that tournaments are fetched for multiple formats using 'last' parameter"""
        with patch('src.etl.etl_pipeline.TopDeckClient') as mock_client_class:
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                mock_client = Mock()
                mock_client_class.return_value = mock_client
                
                pipeline = ETLPipeline(api_key="test_key")
                pipeline.client = mock_client
        
        # Mock get_tournaments to return tournaments for some formats
        def mock_get_tournaments(**kwargs):
            format_name = kwargs.get('format')
            if format_name == 'Standard':
                return [{'TID': 't1', 'format': 'Standard', 'game': 'Magic: The Gathering', 'startDate': 1000}]
            elif format_name == 'Modern':
                return [{'TID': 't2', 'format': 'Modern', 'game': 'Magic: The Gathering', 'startDate': 2000}]
            return []
        
        mock_client.get_tournaments.side_effect = mock_get_tournaments
        
        with patch.object(pipeline, 'load_tournament', return_value=True):
            with patch.object(pipeline, 'update_load_metadata'):
                with patch('src.etl.etl_pipeline.filter_tournaments', side_effect=lambda x: x):
                    result = pipeline.load_initial(days_back=30)
                    
                    assert result == 2  # Should load both tournaments
                    # Verify it was called multiple times (once per format)
                    assert mock_client.get_tournaments.call_count > 1
                    # Verify each call includes 'format', 'game', and 'last' parameters
                    for call in mock_client.get_tournaments.call_args_list:
                        call_kwargs = call[1]
                        assert 'format' in call_kwargs
                        assert call_kwargs['game'] == 'Magic: The Gathering'
                        assert call_kwargs['last'] == 30
                        assert 'start' not in call_kwargs
                        assert 'end' not in call_kwargs
    
    def test_load_initial_returns_zero_when_no_tournaments(self):
        """Test that 0 is returned when no tournaments found"""
        with patch('src.etl.etl_pipeline.TopDeckClient') as mock_client_class:
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                mock_client = Mock()
                mock_client_class.return_value = mock_client
                
                pipeline = ETLPipeline(api_key="test_key")
                pipeline.client = mock_client
        
        # Mock to return empty list for all formats
        mock_client.get_tournaments.return_value = []
        
        result = pipeline.load_initial(days_back=30)
        
        assert result == 0
    
    def test_load_initial_deduplicates_tournaments_by_tid(self):
        """Test that duplicate tournaments (same TID) are deduplicated across formats"""
        with patch('src.etl.etl_pipeline.TopDeckClient') as mock_client_class:
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                mock_client = Mock()
                mock_client_class.return_value = mock_client
                
                pipeline = ETLPipeline(api_key="test_key")
                pipeline.client = mock_client
        
        # Mock to return same tournament ID for multiple formats
        def mock_get_tournaments(**kwargs):
            return [{'TID': 't1', 'format': kwargs.get('format'), 'game': 'Magic: The Gathering', 'startDate': 1000}]
        
        mock_client.get_tournaments.side_effect = mock_get_tournaments
        
        with patch.object(pipeline, 'load_tournament', return_value=True):
            with patch.object(pipeline, 'update_load_metadata'):
                with patch('src.etl.etl_pipeline.filter_tournaments', side_effect=lambda x: x):
                    result = pipeline.load_initial(days_back=30)
                    
                    # Should only load once despite being returned by multiple formats
                    assert pipeline.load_tournament.call_count == 1
    
    def test_load_initial_updates_metadata(self):
        """Test that load metadata is updated after successful load"""
        with patch('src.etl.etl_pipeline.TopDeckClient') as mock_client_class:
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                mock_client = Mock()
                mock_client_class.return_value = mock_client
                
                pipeline = ETLPipeline(api_key="test_key")
                pipeline.client = mock_client
        
        # Mock to return tournaments for at least one format
        def mock_get_tournaments(**kwargs):
            if kwargs.get('format') == 'Standard':
                return [{'TID': 't1', 'format': 'Standard', 'game': 'Magic: The Gathering', 'startDate': 1000}]
            return []
        
        mock_client.get_tournaments.side_effect = mock_get_tournaments
        
        with patch.object(pipeline, 'load_tournament', return_value=True):
            with patch.object(pipeline, 'update_load_metadata') as mock_update:
                with patch('src.etl.etl_pipeline.filter_tournaments', side_effect=lambda x: x):
                    pipeline.load_initial(days_back=30)
                    
                    mock_update.assert_called_once()


class TestLoadIncremental:
    """Tests for load_incremental method"""
    
    def test_load_incremental_fetches_since_last_timestamp(self):
        """Test that incremental load fetches tournaments for multiple formats since last load"""
        with patch('src.etl.etl_pipeline.TopDeckClient') as mock_client_class:
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                mock_client = Mock()
                mock_client_class.return_value = mock_client
                
                pipeline = ETLPipeline(api_key="test_key")
                pipeline.client = mock_client
        
        # Mock get_tournaments to return tournaments for some formats
        def mock_get_tournaments(**kwargs):
            format_name = kwargs.get('format')
            if format_name == 'Standard':
                return [{'TID': 't1', 'format': 'Standard', 'game': 'Magic: The Gathering', 'startDate': 2000}]
            return []
        
        mock_client.get_tournaments.side_effect = mock_get_tournaments
        
        # Mock time.time() to return a fixed timestamp for predictable test
        with patch('src.etl.etl_pipeline.time.time', return_value=1000000):
            with patch.object(pipeline, 'get_last_load_timestamp', return_value=900000):
                with patch.object(pipeline, 'load_tournament', return_value=True):
                    with patch.object(pipeline, 'update_load_metadata'):
                        with patch('src.etl.etl_pipeline.filter_tournaments', side_effect=lambda x: x):
                            result = pipeline.load_incremental()
                            
                            assert result == 1
                            # Verify it was called multiple times (once per format)
                            assert mock_client.get_tournaments.call_count > 1
                            # Verify each call includes 'format', 'game', and 'last' parameters
                            for call in mock_client.get_tournaments.call_args_list:
                                call_kwargs = call[1]
                                assert 'format' in call_kwargs
                                assert call_kwargs['game'] == 'Magic: The Gathering'
                                assert 'last' in call_kwargs or call_kwargs.get('last') is not None
    
    def test_load_incremental_calls_initial_when_no_previous_load(self):
        """Test that initial load is called when no previous load exists"""
        with patch('src.etl.etl_pipeline.TopDeckClient') as mock_client_class:
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                mock_client = Mock()
                mock_client_class.return_value = mock_client
                
                pipeline = ETLPipeline(api_key="test_key")
                pipeline.client = mock_client
        
        with patch.object(pipeline, 'get_last_load_timestamp', return_value=None):
            with patch.object(pipeline, 'load_initial', return_value=5) as mock_initial:
                result = pipeline.load_incremental()
                
                mock_initial.assert_called_once()
                assert result == 5
    
    def test_load_incremental_returns_zero_when_no_new_tournaments(self):
        """Test that 0 is returned when no new tournaments found"""
        with patch('src.etl.etl_pipeline.TopDeckClient') as mock_client_class:
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                mock_client = Mock()
                mock_client_class.return_value = mock_client
                
                pipeline = ETLPipeline(api_key="test_key")
                pipeline.client = mock_client
        
        # Mock to return empty list for all formats
        mock_client.get_tournaments.return_value = []
        
        with patch.object(pipeline, 'get_last_load_timestamp', return_value=1000):
            result = pipeline.load_incremental()
            
            assert result == 0
    
    def test_load_initial_handles_format_errors_gracefully(self):
        """Test that errors for individual formats don't stop the entire load"""
        with patch('src.etl.etl_pipeline.TopDeckClient') as mock_client_class:
            with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
                mock_client = Mock()
                mock_client_class.return_value = mock_client
                
                pipeline = ETLPipeline(api_key="test_key")
                pipeline.client = mock_client
        
        # Mock to raise error for some formats but return data for others
        call_count = 0
        def mock_get_tournaments(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("API Error")
            return [{'TID': 't1', 'format': kwargs.get('format'), 'game': 'Magic: The Gathering', 'startDate': 1000}]
        
        mock_client.get_tournaments.side_effect = mock_get_tournaments
        
        with patch.object(pipeline, 'load_tournament', return_value=True):
            with patch.object(pipeline, 'update_load_metadata'):
                with patch('src.etl.etl_pipeline.filter_tournaments', side_effect=lambda x: x):
                    result = pipeline.load_initial(days_back=30)
                    
                    # Should still load tournaments from formats that succeeded
                    assert result >= 1


# Tests for load_cards_from_bulk_data function (from test_card_loader.py)
class TestLoadCardsFromBulkData:
    """Tests for loading cards from Scryfall bulk data into database"""
    
    def test_load_cards_from_bulk_data_success(self):
        """Test successful loading of cards from bulk data"""
        mock_cards = [
            {
                'id': 'card1-uuid',
                'oracle_id': 'oracle1',
                'name': 'Lightning Bolt',
                'set': 'M21',
                'collector_number': '161',
                'oracle_text': 'Deal 3 damage',
                'type_line': 'Instant',
                'mana_cost': '{R}',
                'cmc': 1.0,
                'color_identity': ['R'],
                'scryfall_uri': 'https://scryfall.com/card/m21/161',
                'rulings': []
            },
            {
                'id': 'card2-uuid',
                'oracle_id': 'oracle2',
                'name': 'Counterspell',
                'set': 'M21',
                'collector_number': '48',
                'oracle_text': 'Counter target spell',
                'type_line': 'Instant',
                'mana_cost': '{U}{U}',
                'cmc': 2.0,
                'color_identity': ['U'],
                'scryfall_uri': 'https://scryfall.com/card/m21/48',
                'rulings': []
            }
        ]
        
        mock_bulk_data = {'data': mock_cards, 'file_path': '/path/to/data.json'}
        
        with patch('src.etl.etl_pipeline.ScryfallClient') as mock_client_class:
            mock_client = mock_client_class.return_value
            mock_client.download_oracle_cards.return_value = mock_bulk_data
            mock_client.download_rulings.return_value = {'data': [], 'file_path': '/path/to/rulings.json'}
            mock_client.join_cards_with_rulings.return_value = mock_cards
            
            with patch('src.etl.etl_pipeline.DatabaseConnection') as mock_db:
                mock_conn = MagicMock()
                mock_db.transaction.return_value.__enter__.return_value = mock_conn
                mock_db.transaction.return_value.__exit__.return_value = None
                mock_cur = MagicMock()
                mock_conn.cursor.return_value = mock_cur
                
                with patch('src.etl.etl_pipeline.execute_batch') as mock_execute_batch:
                    result = load_cards_from_bulk_data()
                    
                    assert result['cards_loaded'] == 2
                    assert result['cards_processed'] == 2
                    mock_execute_batch.assert_called_once()
    
    def test_load_cards_from_bulk_data_with_rulings(self):
        """Test loading cards with rulings joined"""
        mock_cards = [
            {
                'id': 'card1-uuid',
                'oracle_id': 'oracle1',
                'name': 'Lightning Bolt',
                'rulings': ['First ruling', 'Second ruling']
            }
        ]
        
        mock_rulings = [
            {'oracle_id': 'oracle1', 'comment': 'First ruling'},
            {'oracle_id': 'oracle1', 'comment': 'Second ruling'}
        ]
        
        mock_bulk_data = {'data': mock_cards, 'file_path': '/path/to/data.json'}
        mock_rulings_data = {'data': mock_rulings, 'file_path': '/path/to/rulings.json'}
        
        with patch('src.etl.etl_pipeline.ScryfallClient') as mock_client_class:
            mock_client = mock_client_class.return_value
            mock_client.download_oracle_cards.return_value = mock_bulk_data
            mock_client.download_rulings.return_value = mock_rulings_data
            mock_client.join_cards_with_rulings.return_value = mock_cards
            
            with patch('src.etl.etl_pipeline.DatabaseConnection') as mock_db:
                mock_conn = MagicMock()
                mock_db.transaction.return_value.__enter__.return_value = mock_conn
                mock_db.transaction.return_value.__exit__.return_value = None
                mock_cur = MagicMock()
                mock_conn.cursor.return_value = mock_cur
                
                with patch('src.etl.etl_pipeline.execute_batch'):
                    result = load_cards_from_bulk_data()
                    
                    # Verify join_cards_with_rulings was called
                    mock_client.join_cards_with_rulings.assert_called_once()
                    assert result['cards_loaded'] == 1


class TestHandleDuplicateCards:
    """Tests for handling duplicate cards (upsert logic)"""
    
    def test_load_cards_handles_duplicate_card_ids(self):
        """Test that duplicate cards are handled via upsert"""
        mock_cards = [
            {
                'id': 'card1-uuid',
                'name': 'Lightning Bolt',
                'set': 'M21',
                'collector_number': '161',
                'oracle_text': 'Deal 3 damage',
                'type_line': 'Instant',
                'mana_cost': '{R}',
                'cmc': 1.0,
                'color_identity': ['R'],
                'scryfall_uri': 'https://scryfall.com/card/m21/161',
                'rulings': []
            }
        ]
        
        mock_bulk_data = {'data': mock_cards, 'file_path': '/path/to/data.json'}
        
        with patch('src.etl.etl_pipeline.ScryfallClient') as mock_client_class:
            mock_client = mock_client_class.return_value
            mock_client.download_oracle_cards.return_value = mock_bulk_data
            mock_client.download_rulings.return_value = {'data': [], 'file_path': '/path/to/rulings.json'}
            mock_client.join_cards_with_rulings.return_value = mock_cards
            
            with patch('src.etl.etl_pipeline.DatabaseConnection') as mock_db:
                mock_conn = MagicMock()
                mock_db.transaction.return_value.__enter__.return_value = mock_conn
                mock_db.transaction.return_value.__exit__.return_value = None
                mock_cur = MagicMock()
                mock_conn.cursor.return_value = mock_cur
                
                with patch('src.etl.etl_pipeline.execute_batch') as mock_execute_batch:
                    # Load same cards twice
                    load_cards_from_bulk_data()
                    result = load_cards_from_bulk_data()
                    
                    # Should handle duplicates via upsert (ON CONFLICT DO UPDATE)
                    assert result['cards_loaded'] == 1
                    # Verify execute_batch was called (for upsert)
                    assert mock_execute_batch.call_count == 2
    
    def test_load_cards_updates_existing_card(self):
        """Test that existing cards are updated when reloaded"""
        mock_cards = [
            {
                'id': 'card1-uuid',
                'name': 'Lightning Bolt',
                'oracle_text': 'Updated oracle text',
                'rulings': []
            }
        ]
        
        mock_bulk_data = {'data': mock_cards, 'file_path': '/path/to/data.json'}
        
        with patch('src.etl.etl_pipeline.ScryfallClient') as mock_client_class:
            mock_client = mock_client_class.return_value
            mock_client.download_oracle_cards.return_value = mock_bulk_data
            mock_client.download_rulings.return_value = {'data': [], 'file_path': '/path/to/rulings.json'}
            mock_client.join_cards_with_rulings.return_value = mock_cards
            
            with patch('src.etl.etl_pipeline.DatabaseConnection') as mock_db:
                mock_conn = MagicMock()
                mock_db.transaction.return_value.__enter__.return_value = mock_conn
                mock_db.transaction.return_value.__exit__.return_value = None
                mock_cur = MagicMock()
                mock_conn.cursor.return_value = mock_cur
                
                with patch('src.etl.etl_pipeline.execute_batch') as mock_execute_batch:
                    result = load_cards_from_bulk_data()
                    
                    # Should use ON CONFLICT DO UPDATE for upsert
                    assert result['cards_loaded'] == 1
                    mock_execute_batch.assert_called_once()


class TestJoinAndStoreRulings:
    """Tests for joining and storing rulings"""
    
    def test_load_cards_joins_rulings_with_cards(self):
        """Test that rulings are joined with cards by oracle_id"""
        mock_cards = [
            {
                'id': 'card1-uuid',
                'oracle_id': 'oracle1',
                'name': 'Lightning Bolt',
                'rulings': []
            }
        ]
        
        mock_rulings = [
            {'oracle_id': 'oracle1', 'comment': 'First ruling'},
            {'oracle_id': 'oracle1', 'comment': 'Second ruling'}
        ]
        
        mock_cards_with_rulings = [
            {
                'id': 'card1-uuid',
                'oracle_id': 'oracle1',
                'name': 'Lightning Bolt',
                'rulings': ['First ruling', 'Second ruling']
            }
        ]
        
        mock_bulk_data = {'data': mock_cards, 'file_path': '/path/to/data.json'}
        mock_rulings_data = {'data': mock_rulings, 'file_path': '/path/to/rulings.json'}
        
        with patch('src.etl.etl_pipeline.ScryfallClient') as mock_client_class:
            mock_client = mock_client_class.return_value
            mock_client.download_oracle_cards.return_value = mock_bulk_data
            mock_client.download_rulings.return_value = mock_rulings_data
            mock_client.join_cards_with_rulings.return_value = mock_cards_with_rulings
            
            with patch('src.etl.etl_pipeline.DatabaseConnection') as mock_db:
                mock_conn = MagicMock()
                mock_db.transaction.return_value.__enter__.return_value = mock_conn
                mock_db.transaction.return_value.__exit__.return_value = None
                mock_cur = MagicMock()
                mock_conn.cursor.return_value = mock_cur
                
                with patch('src.etl.etl_pipeline.execute_batch'):
                    result = load_cards_from_bulk_data()
                    
                    # Verify join_cards_with_rulings was called with correct arguments
                    mock_client.join_cards_with_rulings.assert_called_once_with(mock_cards, mock_rulings)
                    assert result['cards_loaded'] == 1
    
    def test_load_cards_stores_rulings_as_comma_separated(self):
        """Test that rulings are stored as comma-separated string"""
        mock_cards = [
            {
                'id': 'card1-uuid',
                'oracle_id': 'oracle1',
                'name': 'Lightning Bolt',
                'rulings': ['First ruling', 'Second ruling']
            }
        ]
        
        mock_bulk_data = {'data': mock_cards, 'file_path': '/path/to/data.json'}
        
        with patch('src.etl.etl_pipeline.ScryfallClient') as mock_client_class:
            mock_client = mock_client_class.return_value
            mock_client.download_oracle_cards.return_value = mock_bulk_data
            mock_client.download_rulings.return_value = {'data': [], 'file_path': '/path/to/rulings.json'}
            mock_client.join_cards_with_rulings.return_value = mock_cards
            mock_client.transform_card_to_db_row.return_value = {
                'card_id': 'card1-uuid',
                'name': 'Lightning Bolt',
                'rulings': 'First ruling, Second ruling'
            }
            
            with patch('src.etl.etl_pipeline.DatabaseConnection') as mock_db:
                mock_conn = MagicMock()
                mock_db.transaction.return_value.__enter__.return_value = mock_conn
                mock_db.transaction.return_value.__exit__.return_value = None
                mock_cur = MagicMock()
                mock_conn.cursor.return_value = mock_cur
                
                with patch('src.etl.etl_pipeline.execute_batch'):
                    result = load_cards_from_bulk_data()
                    
                    # Verify transform_card_to_db_row was called (which concatenates rulings)
                    mock_client.transform_card_to_db_row.assert_called()
                    assert result['cards_loaded'] == 1
    
    def test_load_cards_handles_cards_without_rulings(self):
        """Test that cards without rulings are handled correctly"""
        mock_cards = [
            {
                'id': 'card1-uuid',
                'oracle_id': 'oracle1',
                'name': 'Lightning Bolt',
                'rulings': []
            }
        ]
        
        mock_bulk_data = {'data': mock_cards, 'file_path': '/path/to/data.json'}
        
        with patch('src.etl.etl_pipeline.ScryfallClient') as mock_client_class:
            mock_client = mock_client_class.return_value
            mock_client.download_oracle_cards.return_value = mock_bulk_data
            mock_client.download_rulings.return_value = {'data': [], 'file_path': '/path/to/rulings.json'}
            mock_client.join_cards_with_rulings.return_value = mock_cards
            mock_client.transform_card_to_db_row.return_value = {
                'card_id': 'card1-uuid',
                'name': 'Lightning Bolt',
                'rulings': ''
            }
            
            with patch('src.etl.etl_pipeline.DatabaseConnection') as mock_db:
                mock_conn = MagicMock()
                mock_db.transaction.return_value.__enter__.return_value = mock_conn
                mock_db.transaction.return_value.__exit__.return_value = None
                mock_cur = MagicMock()
                mock_conn.cursor.return_value = mock_cur
                
                with patch('src.etl.etl_pipeline.execute_batch'):
                    result = load_cards_from_bulk_data()
                    
                    assert result['cards_loaded'] == 1


# Tests for parse_decklist function (from test_decklist_parser.py)
class TestParseDecklistStandardFormat:
    """Tests for parsing standard MTG decklist format (quantity + card name)"""
    
    def test_parse_simple_decklist(self):
        """Test parsing a simple decklist with quantity and card name"""
        decklist = "4 Lightning Bolt\n2 Mountain\n1 Island"
        result = parse_decklist(decklist)
        
        assert len(result) == 3
        assert result[0] == {'quantity': 4, 'card_name': 'Lightning Bolt', 'section': 'mainboard'}
        assert result[1] == {'quantity': 2, 'card_name': 'Mountain', 'section': 'mainboard'}
        assert result[2] == {'quantity': 1, 'card_name': 'Island', 'section': 'mainboard'}
    
    def test_parse_decklist_with_tabs(self):
        """Test parsing decklist with tab-separated quantity and name"""
        decklist = "4\tLightning Bolt\n2\tMountain"
        result = parse_decklist(decklist)
        
        assert len(result) == 2
        assert result[0] == {'quantity': 4, 'card_name': 'Lightning Bolt', 'section': 'mainboard'}
        assert result[1] == {'quantity': 2, 'card_name': 'Mountain', 'section': 'mainboard'}
    
    def test_parse_decklist_with_multiple_spaces(self):
        """Test parsing decklist with multiple spaces between quantity and name"""
        decklist = "4   Lightning Bolt\n2    Mountain"
        result = parse_decklist(decklist)
        
        assert len(result) == 2
        assert result[0] == {'quantity': 4, 'card_name': 'Lightning Bolt', 'section': 'mainboard'}
        assert result[1] == {'quantity': 2, 'card_name': 'Mountain', 'section': 'mainboard'}
    
    def test_parse_decklist_with_card_names_containing_numbers(self):
        """Test parsing cards with numbers in their names"""
        decklist = "4 Lightning Bolt\n2 Mountain\n1 Sol Ring"
        result = parse_decklist(decklist)
        
        assert len(result) == 3
        assert result[0]['card_name'] == 'Lightning Bolt'
        assert result[1]['card_name'] == 'Mountain'
        assert result[2]['card_name'] == 'Sol Ring'


class TestParseDecklistMainboardSideboard:
    """Tests for identifying mainboard vs sideboard sections"""
    
    def test_parse_decklist_with_sideboard_separator(self):
        """Test parsing decklist with sideboard section"""
        decklist = "4 Lightning Bolt\n2 Mountain\n\nSideboard\n2 Counterspell\n1 Negate"
        result = parse_decklist(decklist)
        
        assert len(result) == 4
        assert result[0] == {'quantity': 4, 'card_name': 'Lightning Bolt', 'section': 'mainboard'}
        assert result[1] == {'quantity': 2, 'card_name': 'Mountain', 'section': 'mainboard'}
        assert result[2] == {'quantity': 2, 'card_name': 'Counterspell', 'section': 'sideboard'}
        assert result[3] == {'quantity': 1, 'card_name': 'Negate', 'section': 'sideboard'}
    
    def test_parse_decklist_with_sideboard_colon(self):
        """Test parsing decklist with 'Sideboard:' separator"""
        decklist = "4 Lightning Bolt\nSideboard:\n2 Counterspell"
        result = parse_decklist(decklist)
        
        assert len(result) == 2
        assert result[0] == {'quantity': 4, 'card_name': 'Lightning Bolt', 'section': 'mainboard'}
        assert result[1] == {'quantity': 2, 'card_name': 'Counterspell', 'section': 'sideboard'}
    
    def test_parse_decklist_with_sb_prefix(self):
        """Test parsing decklist with 'SB:' prefix"""
        decklist = "4 Lightning Bolt\nSB: 2 Counterspell"
        result = parse_decklist(decklist)
        
        assert len(result) == 2
        assert result[0] == {'quantity': 4, 'card_name': 'Lightning Bolt', 'section': 'mainboard'}
        assert result[1] == {'quantity': 2, 'card_name': 'Counterspell', 'section': 'sideboard'}
    
    def test_parse_decklist_with_comment_sideboard(self):
        """Test parsing decklist with '// Sideboard' comment"""
        decklist = "4 Lightning Bolt\n// Sideboard\n2 Counterspell"
        result = parse_decklist(decklist)
        
        assert len(result) == 2
        assert result[0] == {'quantity': 4, 'card_name': 'Lightning Bolt', 'section': 'mainboard'}
        assert result[1] == {'quantity': 2, 'card_name': 'Counterspell', 'section': 'sideboard'}
    
    def test_parse_decklist_all_mainboard(self):
        """Test parsing decklist with no sideboard section"""
        decklist = "4 Lightning Bolt\n2 Mountain\n1 Island"
        result = parse_decklist(decklist)
        
        assert all(card['section'] == 'mainboard' for card in result)
    
    def test_parse_decklist_all_sideboard(self):
        """Test parsing decklist that starts with sideboard"""
        decklist = "Sideboard\n2 Counterspell\n1 Negate"
        result = parse_decklist(decklist)
        
        assert len(result) == 2
        assert all(card['section'] == 'sideboard' for card in result)


class TestParseDecklistEdgeCases:
    """Tests for handling edge cases (empty decklists, malformed entries, special characters)"""
    
    def test_parse_empty_decklist(self):
        """Test parsing empty decklist"""
        result = parse_decklist("")
        assert result == []
    
    def test_parse_decklist_with_only_whitespace(self):
        """Test parsing decklist with only whitespace"""
        result = parse_decklist("   \n\n  \t  ")
        assert result == []
    
    def test_parse_decklist_with_empty_lines(self):
        """Test parsing decklist with empty lines"""
        decklist = "4 Lightning Bolt\n\n\n2 Mountain\n\n1 Island"
        result = parse_decklist(decklist)
        
        assert len(result) == 3
        assert result[0]['card_name'] == 'Lightning Bolt'
        assert result[1]['card_name'] == 'Mountain'
        assert result[2]['card_name'] == 'Island'
    
    def test_parse_decklist_with_comments(self):
        """Test parsing decklist with comment lines"""
        decklist = "4 Lightning Bolt\n// This is a comment\n2 Mountain\n# Another comment"
        result = parse_decklist(decklist)
        
        assert len(result) == 2
        assert result[0]['card_name'] == 'Lightning Bolt'
        assert result[1]['card_name'] == 'Mountain'
    
    def test_parse_decklist_with_malformed_entries(self):
        """Test parsing decklist with malformed entries"""
        decklist = "4 Lightning Bolt\nInvalid Entry\n2 Mountain\nNo Quantity Here"
        result = parse_decklist(decklist)
        
        assert len(result) == 2
        assert result[0]['card_name'] == 'Lightning Bolt'
        assert result[1]['card_name'] == 'Mountain'
    
    def test_parse_decklist_with_special_characters_in_name(self):
        """Test parsing decklist with special characters in card names"""
        decklist = "4 Jace, the Mind Sculptor\n2 Æther Vial"
        result = parse_decklist(decklist)
        
        assert len(result) == 2
        assert result[0]['card_name'] == 'Jace, the Mind Sculptor'
        assert result[1]['card_name'] == 'Æther Vial'
    
    def test_parse_decklist_with_zero_quantity(self):
        """Test parsing decklist with zero quantity (should be skipped)"""
        decklist = "4 Lightning Bolt\n0 Mountain\n2 Island"
        result = parse_decklist(decklist)
        
        assert len(result) == 2
        assert result[0]['card_name'] == 'Lightning Bolt'
        assert result[1]['card_name'] == 'Island'
    
    def test_parse_decklist_with_negative_quantity(self):
        """Test parsing decklist with negative quantity (should be skipped)"""
        decklist = "4 Lightning Bolt\n-1 Mountain\n2 Island"
        result = parse_decklist(decklist)
        
        assert len(result) == 2
        assert result[0]['card_name'] == 'Lightning Bolt'
        assert result[1]['card_name'] == 'Island'
    
    def test_parse_decklist_with_quantity_only(self):
        """Test parsing decklist with line containing only quantity"""
        decklist = "4 Lightning Bolt\n4\n2 Mountain"
        result = parse_decklist(decklist)
        
        assert len(result) == 2
        assert result[0]['card_name'] == 'Lightning Bolt'
        assert result[1]['card_name'] == 'Mountain'
    
    def test_parse_decklist_with_card_name_only(self):
        """Test parsing decklist with line containing only card name (no quantity)"""
        decklist = "4 Lightning Bolt\nMountain\n2 Island"
        result = parse_decklist(decklist)
        
        assert len(result) == 2
        assert result[0]['card_name'] == 'Lightning Bolt'
        assert result[1]['card_name'] == 'Island'
    
    def test_parse_decklist_with_very_large_quantity(self):
        """Test parsing decklist with very large quantity"""
        decklist = "999 Lightning Bolt"
        result = parse_decklist(decklist)
        
        assert len(result) == 1
        assert result[0]['quantity'] == 999
        assert result[0]['card_name'] == 'Lightning Bolt'
    
    def test_parse_decklist_with_trailing_whitespace(self):
        """Test parsing decklist with trailing whitespace on lines"""
        decklist = "4 Lightning Bolt  \n  2 Mountain  "
        result = parse_decklist(decklist)
        
        assert len(result) == 2
        assert result[0]['card_name'] == 'Lightning Bolt'
        assert result[1]['card_name'] == 'Mountain'


# Tests for filter functions (from test_filters.py)
class TestIsCommanderFormat:
    """Tests for is_commander_format function"""
    
    def test_identifies_commander_formats(self):
        """Test that all commander formats are correctly identified"""
        for format_name in COMMANDER_FORMATS:
            assert is_commander_format(format_name) is True, f"Failed to identify {format_name} as commander format"
    
    def test_identifies_non_commander_formats(self):
        """Test that non-commander formats are not incorrectly identified"""
        non_commander_formats = ['Standard', 'Modern', 'Legacy', 'Vintage', 'Pioneer', 'Pauper']
        for format_name in non_commander_formats:
            assert is_commander_format(format_name) is False, f"Incorrectly identified {format_name} as commander format"
    
    def test_handles_empty_string(self):
        """Test that empty string returns False"""
        assert is_commander_format('') is False
    
    def test_handles_none(self):
        """Test that None returns False"""
        assert is_commander_format(None) is False
    
    def test_handles_whitespace(self):
        """Test that whitespace is stripped correctly"""
        assert is_commander_format('  EDH  ') is True
        assert is_commander_format('  Standard  ') is False
    
    def test_case_sensitivity(self):
        """Test that format checking is case sensitive"""
        assert is_commander_format('edh') is False  # lowercase should not match
        assert is_commander_format('EDH') is True


class TestIsLimitedFormat:
    """Tests for is_limited_format function"""
    
    def test_identifies_limited_formats(self):
        """Test that all limited formats are correctly identified"""
        for format_name in LIMITED_FORMATS:
            assert is_limited_format(format_name) is True, f"Failed to identify {format_name} as limited format"
    
    def test_identifies_non_limited_formats(self):
        """Test that non-limited formats are not incorrectly identified"""
        non_limited_formats = ['Standard', 'Modern', 'Legacy', 'Vintage', 'Pioneer', 'Pauper', 'EDH']
        for format_name in non_limited_formats:
            assert is_limited_format(format_name) is False, f"Incorrectly identified {format_name} as limited format"
    
    def test_handles_empty_string(self):
        """Test that empty string returns False"""
        assert is_limited_format('') is False
    
    def test_handles_none(self):
        """Test that None returns False"""
        assert is_limited_format(None) is False
    
    def test_handles_whitespace(self):
        """Test that whitespace is stripped correctly"""
        assert is_limited_format('  Draft  ') is True
        assert is_limited_format('  Standard  ') is False
    
    def test_case_sensitivity(self):
        """Test that format checking is case sensitive"""
        assert is_limited_format('draft') is False  # lowercase should not match
        assert is_limited_format('Draft') is True


class TestShouldIncludeTournament:
    """Tests for should_include_tournament function"""
    
    def test_includes_standard_mtg_tournament(self):
        """Test that valid MTG tournaments are included"""
        tournament = {
            'TID': 'test123',
            'format': 'Standard',
            'game': 'Magic: The Gathering'
        }
        assert should_include_tournament(tournament) is True
    
    def test_excludes_commander_tournament(self):
        """Test that commander tournaments are excluded"""
        tournament = {
            'TID': 'test123',
            'format': 'EDH',
            'game': 'Magic: The Gathering'
        }
        assert should_include_tournament(tournament) is False
    
    def test_excludes_non_mtg_game(self):
        """Test that non-MTG games are excluded"""
        tournament = {
            'TID': 'test123',
            'format': 'Standard',
            'game': 'Pokemon'
        }
        assert should_include_tournament(tournament) is False
    
    def test_handles_missing_format(self):
        """Test that tournaments with missing format are included if MTG"""
        tournament = {
            'TID': 'test123',
            'game': 'Magic: The Gathering'
        }
        assert should_include_tournament(tournament) is True
    
    def test_handles_missing_game(self):
        """Test that tournaments with missing game field are excluded"""
        tournament = {
            'TID': 'test123',
            'format': 'Standard'
        }
        assert should_include_tournament(tournament) is False
    
    def test_excludes_all_commander_formats(self):
        """Test that all commander formats are excluded"""
        for cmd_format in COMMANDER_FORMATS:
            tournament = {
                'TID': 'test123',
                'format': cmd_format,
                'game': 'Magic: The Gathering'
            }
            assert should_include_tournament(tournament) is False, f"Failed to exclude {cmd_format}"
    
    def test_excludes_limited_tournament(self):
        """Test that limited tournaments are excluded"""
        tournament = {
            'TID': 'test123',
            'format': 'Draft',
            'game': 'Magic: The Gathering'
        }
        assert should_include_tournament(tournament) is False
    
    def test_excludes_all_limited_formats(self):
        """Test that all limited formats are excluded"""
        for limited_format in LIMITED_FORMATS:
            tournament = {
                'TID': 'test123',
                'format': limited_format,
                'game': 'Magic: The Gathering'
            }
            assert should_include_tournament(tournament) is False, f"Failed to exclude {limited_format}"


class TestIsValidMatch:
    """Tests for is_valid_match function"""
    
    def test_valid_two_player_match(self):
        """Test that a valid 2-player match is accepted"""
        table_data = {
            'table': 1,
            'players': [
                {'id': 'player1', 'name': 'Alice'},
                {'id': 'player2', 'name': 'Bob'}
            ]
        }
        assert is_valid_match(table_data) is True
    
    def test_valid_one_player_match(self):
        """Test that a 1-player match (bye) is accepted"""
        table_data = {
            'table': 1,
            'players': [
                {'id': 'player1', 'name': 'Alice'}
            ]
        }
        assert is_valid_match(table_data) is True
    
    def test_invalid_three_player_match(self):
        """Test that a 3-player match is rejected"""
        table_data = {
            'table': 1,
            'players': [
                {'id': 'player1', 'name': 'Alice'},
                {'id': 'player2', 'name': 'Bob'},
                {'id': 'player3', 'name': 'Charlie'}
            ]
        }
        assert is_valid_match(table_data) is False
    
    def test_invalid_four_player_match(self):
        """Test that a 4-player match is rejected"""
        table_data = {
            'table': 1,
            'players': [
                {'id': f'player{i}', 'name': f'Player{i}'} for i in range(4)
            ]
        }
        assert is_valid_match(table_data) is False
    
    def test_handles_empty_players_list(self):
        """Test that empty players list is accepted"""
        table_data = {
            'table': 1,
            'players': []
        }
        assert is_valid_match(table_data) is True
    
    def test_handles_missing_players_key(self):
        """Test that missing players key returns empty list (True)"""
        table_data = {
            'table': 1
        }
        assert is_valid_match(table_data) is True


class TestFilterTournaments:
    """Tests for filter_tournaments function"""
    
    def test_filters_out_commander_tournaments(self):
        """Test that commander tournaments are filtered out"""
        tournaments = [
            {'TID': '1', 'format': 'Standard', 'game': 'Magic: The Gathering'},
            {'TID': '2', 'format': 'EDH', 'game': 'Magic: The Gathering'},
            {'TID': '3', 'format': 'Modern', 'game': 'Magic: The Gathering'},
        ]
        filtered = filter_tournaments(tournaments)
        assert len(filtered) == 2
        assert all(t['format'] not in COMMANDER_FORMATS for t in filtered)
    
    def test_filters_out_limited_tournaments(self):
        """Test that limited tournaments are filtered out"""
        tournaments = [
            {'TID': '1', 'format': 'Standard', 'game': 'Magic: The Gathering'},
            {'TID': '2', 'format': 'Draft', 'game': 'Magic: The Gathering'},
            {'TID': '3', 'format': 'Modern', 'game': 'Magic: The Gathering'},
        ]
        filtered = filter_tournaments(tournaments)
        assert len(filtered) == 2
        assert all(t['format'] not in LIMITED_FORMATS for t in filtered)
    
    def test_filters_out_both_commander_and_limited(self):
        """Test that both commander and limited tournaments are filtered out"""
        tournaments = [
            {'TID': '1', 'format': 'Standard', 'game': 'Magic: The Gathering'},
            {'TID': '2', 'format': 'EDH', 'game': 'Magic: The Gathering'},
            {'TID': '3', 'format': 'Draft', 'game': 'Magic: The Gathering'},
            {'TID': '4', 'format': 'Sealed', 'game': 'Magic: The Gathering'},
            {'TID': '5', 'format': 'Modern', 'game': 'Magic: The Gathering'},
        ]
        filtered = filter_tournaments(tournaments)
        assert len(filtered) == 2
        assert all(t['format'] not in COMMANDER_FORMATS for t in filtered)
        assert all(t['format'] not in LIMITED_FORMATS for t in filtered)
    
    def test_filters_out_non_mtg_games(self):
        """Test that non-MTG games are filtered out"""
        tournaments = [
            {'TID': '1', 'format': 'Standard', 'game': 'Magic: The Gathering'},
            {'TID': '2', 'format': 'Standard', 'game': 'Pokemon'},
            {'TID': '3', 'format': 'Modern', 'game': 'Magic: The Gathering'},
        ]
        filtered = filter_tournaments(tournaments)
        assert len(filtered) == 2
        assert all(t['game'] == 'Magic: The Gathering' for t in filtered)
    
    def test_returns_empty_list_for_empty_input(self):
        """Test that empty input returns empty list"""
        assert filter_tournaments([]) == []
    
    def test_keeps_all_valid_tournaments(self):
        """Test that all valid tournaments are kept"""
        tournaments = [
            {'TID': '1', 'format': 'Standard', 'game': 'Magic: The Gathering'},
            {'TID': '2', 'format': 'Modern', 'game': 'Magic: The Gathering'},
            {'TID': '3', 'format': 'Legacy', 'game': 'Magic: The Gathering'},
        ]
        filtered = filter_tournaments(tournaments)
        assert len(filtered) == 3


class TestFilterRoundsData:
    """Tests for filter_rounds_data function"""
    
    def test_filters_multiplayer_matches(self):
        """Test that multiplayer matches are filtered out"""
        rounds_data = [
            {
                'round': 1,
                'tables': [
                    {'table': 1, 'players': [{'id': '1'}, {'id': '2'}]},  # valid
                    {'table': 2, 'players': [{'id': '3'}, {'id': '4'}, {'id': '5'}]},  # invalid
                ]
            }
        ]
        filtered = filter_rounds_data(rounds_data)
        assert len(filtered) == 1
        assert len(filtered[0]['tables']) == 1
        assert filtered[0]['tables'][0]['table'] == 1
    
    def test_removes_rounds_with_no_valid_matches(self):
        """Test that rounds with no valid matches are removed"""
        rounds_data = [
            {
                'round': 1,
                'tables': [
                    {'table': 1, 'players': [{'id': '1'}, {'id': '2'}, {'id': '3'}]},
                    {'table': 2, 'players': [{'id': '4'}, {'id': '5'}, {'id': '6'}]},
                ]
            }
        ]
        filtered = filter_rounds_data(rounds_data)
        assert len(filtered) == 0
    
    def test_keeps_all_valid_matches(self):
        """Test that all valid matches are kept"""
        rounds_data = [
            {
                'round': 1,
                'tables': [
                    {'table': 1, 'players': [{'id': '1'}, {'id': '2'}]},
                    {'table': 2, 'players': [{'id': '3'}, {'id': '4'}]},
                ]
            }
        ]
        filtered = filter_rounds_data(rounds_data)
        assert len(filtered) == 1
        assert len(filtered[0]['tables']) == 2
    
    def test_handles_empty_input(self):
        """Test that empty input returns empty list"""
        assert filter_rounds_data([]) == []
    
    def test_handles_rounds_with_empty_tables(self):
        """Test that rounds with empty tables list are removed"""
        rounds_data = [
            {'round': 1, 'tables': []},
            {'round': 2, 'tables': [{'table': 1, 'players': [{'id': '1'}, {'id': '2'}]}]}
        ]
        filtered = filter_rounds_data(rounds_data)
        assert len(filtered) == 1
        assert filtered[0]['round'] == 2
    
    def test_preserves_round_data_structure(self):
        """Test that round data structure is preserved"""
        rounds_data = [
            {
                'round': 1,
                'extra_field': 'test_value',
                'tables': [
                    {'table': 1, 'players': [{'id': '1'}, {'id': '2'}], 'status': 'complete'}
                ]
            }
        ]
        filtered = filter_rounds_data(rounds_data)
        assert filtered[0]['round'] == 1
        assert filtered[0]['extra_field'] == 'test_value'
        assert filtered[0]['tables'][0]['status'] == 'complete'


# Tests for ETLPipeline.parse_and_store_decklist_cards (from test_etl_decklist_cards.py)
class TestExtractCardsFromDecklist:
    """Tests for extracting cards from decklist_text and matching to cards table"""
    
    def test_parse_and_store_decklist_cards_extracts_cards(self):
        """Test that cards are extracted from decklist_text"""
        decklist_text = "4 Lightning Bolt\n2 Mountain\n1 Island"
        
        with patch('src.etl.etl_pipeline.DatabaseConnection') as mock_db:
            mock_conn = MagicMock()
            mock_db.transaction.return_value.__enter__.return_value = mock_conn
            mock_db.transaction.return_value.__exit__.return_value = None
            mock_cur = MagicMock()
            mock_conn.cursor.return_value = mock_cur
            
            # Mock decklist_id lookup
            mock_cur.fetchone.return_value = (1,)  # decklist_id = 1
            
            # Mock card lookups - return card_ids for matching cards
            def mock_execute_side_effect(query, params):
                if 'SELECT decklist_id' in query:
                    mock_cur.fetchone.return_value = (1,)
                elif 'SELECT card_id' in query:
                    if params and 'Lightning Bolt' in params[0]:
                        mock_cur.fetchone.return_value = ('card1-uuid',)
                    elif params and 'Mountain' in params[0]:
                        mock_cur.fetchone.return_value = ('card2-uuid',)
                    elif params and 'Island' in params[0]:
                        mock_cur.fetchone.return_value = ('card3-uuid',)
                    else:
                        mock_cur.fetchone.return_value = None
            
            mock_cur.execute.side_effect = mock_execute_side_effect
            
            pipeline = ETLPipeline()
            
            with patch('src.etl.etl_pipeline.parse_decklist') as mock_parse:
                mock_parse.return_value = [
                    {'quantity': 4, 'card_name': 'Lightning Bolt', 'section': 'mainboard'},
                    {'quantity': 2, 'card_name': 'Mountain', 'section': 'mainboard'},
                    {'quantity': 1, 'card_name': 'Island', 'section': 'mainboard'}
                ]
                
                with patch('src.etl.etl_pipeline.execute_batch'):
                    pipeline.parse_and_store_decklist_cards('player1', 'tournament1', decklist_text, mock_conn)
                    
                    # Verify parse_decklist was called
                    mock_parse.assert_called_once_with(decklist_text)
    
    def test_parse_and_store_decklist_cards_matches_cards_by_name(self):
        """Test that card names are matched to cards table by name"""
        decklist_text = "4 Lightning Bolt"
        
        with patch('src.etl.etl_pipeline.DatabaseConnection') as mock_db:
            mock_conn = MagicMock()
            mock_db.transaction.return_value.__enter__.return_value = mock_conn
            mock_db.transaction.return_value.__exit__.return_value = None
            mock_cur = MagicMock()
            mock_conn.cursor.return_value = mock_cur
            
            # Mock decklist_id lookup
            mock_cur.fetchone.return_value = (1,)
            
            # Mock card lookup - return card_id for Lightning Bolt
            def mock_execute_side_effect(query, params):
                if 'SELECT decklist_id' in query:
                    mock_cur.fetchone.return_value = (1,)
                elif 'SELECT card_id' in query:
                    mock_cur.fetchone.return_value = ('lightning-bolt-uuid',)
            
            mock_cur.execute.side_effect = mock_execute_side_effect
            
            pipeline = ETLPipeline()
            
            with patch('src.etl.etl_pipeline.parse_decklist') as mock_parse:
                mock_parse.return_value = [
                    {'quantity': 4, 'card_name': 'Lightning Bolt', 'section': 'mainboard'}
                ]
                
                with patch('src.etl.etl_pipeline.execute_batch') as mock_execute_batch:
                    pipeline.parse_and_store_decklist_cards('player1', 'tournament1', decklist_text, mock_conn)
                    
                    # Verify card lookup query was executed
                    assert mock_cur.execute.call_count >= 2  # decklist_id lookup + card lookup
                    
                    # Verify execute_batch was called with correct card_id
                    mock_execute_batch.assert_called_once()
                    call_args = mock_execute_batch.call_args
                    # execute_batch signature: (cur, sql, batch_data)
                    # Batch data is the third positional argument (index 2)
                    batch_data = call_args[0][2]
                    assert any('lightning-bolt-uuid' in str(item) for item in batch_data)


class TestHandleCardsNotFound:
    """Tests for handling cards not found in cards table"""
    
    def test_parse_and_store_decklist_cards_logs_missing_cards(self):
        """Test that missing cards are logged but processing continues"""
        decklist_text = "4 Lightning Bolt\n2 Unknown Card"
        
        with patch('src.etl.etl_pipeline.DatabaseConnection') as mock_db:
            mock_conn = MagicMock()
            mock_db.transaction.return_value.__enter__.return_value = mock_conn
            mock_db.transaction.return_value.__exit__.return_value = None
            mock_cur = MagicMock()
            mock_conn.cursor.return_value = mock_cur
            
            # Mock decklist_id lookup
            mock_cur.fetchone.return_value = (1,)
            
            # Mock card lookups - Lightning Bolt found, Unknown Card not found
            def mock_execute_side_effect(query, params):
                if 'SELECT decklist_id' in query:
                    mock_cur.fetchone.return_value = (1,)
                elif 'SELECT card_id' in query:
                    if params and 'Lightning Bolt' in params[0]:
                        mock_cur.fetchone.return_value = ('card1-uuid',)
                    else:
                        mock_cur.fetchone.return_value = None  # Card not found
            
            mock_cur.execute.side_effect = mock_execute_side_effect
            
            pipeline = ETLPipeline()
            
            with patch('src.etl.etl_pipeline.parse_decklist') as mock_parse:
                mock_parse.return_value = [
                    {'quantity': 4, 'card_name': 'Lightning Bolt', 'section': 'mainboard'},
                    {'quantity': 2, 'card_name': 'Unknown Card', 'section': 'mainboard'}
                ]
                
                with patch('src.etl.etl_pipeline.execute_batch') as mock_execute_batch:
                    with patch('src.etl.etl_pipeline.logger') as mock_logger:
                        pipeline.parse_and_store_decklist_cards('player1', 'tournament1', decklist_text, mock_conn)
                        
                        # Verify warning was logged for missing card
                        mock_logger.warning.assert_called()
                        warning_calls = [str(call) for call in mock_logger.warning.call_args_list]
                        assert any('Unknown Card' in str(call) or 'not found' in str(call).lower() for call in warning_calls)
                        
                        # Verify execute_batch was still called for found card
                        mock_execute_batch.assert_called_once()
    
    def test_parse_and_store_decklist_cards_continues_on_missing_cards(self):
        """Test that processing continues when cards are not found"""
        decklist_text = "4 Missing Card 1\n2 Missing Card 2\n1 Found Card"
        
        with patch('src.etl.etl_pipeline.DatabaseConnection') as mock_db:
            mock_conn = MagicMock()
            mock_db.transaction.return_value.__enter__.return_value = mock_conn
            mock_db.transaction.return_value.__exit__.return_value = None
            mock_cur = MagicMock()
            mock_conn.cursor.return_value = mock_cur
            
            # Mock decklist_id lookup
            mock_cur.fetchone.return_value = (1,)
            
            # Mock card lookups - only Found Card exists
            def mock_execute_side_effect(query, params):
                if 'SELECT decklist_id' in query:
                    mock_cur.fetchone.return_value = (1,)
                elif 'SELECT card_id' in query:
                    if params and 'Found Card' in params[0]:
                        mock_cur.fetchone.return_value = ('found-card-uuid',)
                    else:
                        mock_cur.fetchone.return_value = None
            
            mock_cur.execute.side_effect = mock_execute_side_effect
            
            pipeline = ETLPipeline()
            
            with patch('src.etl.etl_pipeline.parse_decklist') as mock_parse:
                mock_parse.return_value = [
                    {'quantity': 4, 'card_name': 'Missing Card 1', 'section': 'mainboard'},
                    {'quantity': 2, 'card_name': 'Missing Card 2', 'section': 'mainboard'},
                    {'quantity': 1, 'card_name': 'Found Card', 'section': 'mainboard'}
                ]
                
                with patch('src.etl.etl_pipeline.execute_batch') as mock_execute_batch:
                    pipeline.parse_and_store_decklist_cards('player1', 'tournament1', decklist_text, mock_conn)
                    
                    # Verify execute_batch was called (for the found card)
                    mock_execute_batch.assert_called_once()
                    
                    # Verify batch data contains only the found card
                    call_args = mock_execute_batch.call_args
                    batch_data = call_args[0][2]  # Third positional argument is the batch data
                    assert len(batch_data) == 1
                    assert batch_data[0][1] == 'found-card-uuid'  # card_id


class TestPopulateDeckCardsTable:
    """Tests for populating deck_cards table with quantities and sections"""
    
    def test_parse_and_store_decklist_cards_populates_deck_cards(self):
        """Test that deck_cards table is populated with correct data"""
        decklist_text = "4 Lightning Bolt\n2 Mountain"
        
        with patch('src.etl.etl_pipeline.DatabaseConnection') as mock_db:
            mock_conn = MagicMock()
            mock_db.transaction.return_value.__enter__.return_value = mock_conn
            mock_db.transaction.return_value.__exit__.return_value = None
            mock_cur = MagicMock()
            mock_conn.cursor.return_value = mock_cur
            
            # Mock decklist_id lookup
            mock_cur.fetchone.return_value = (1,)
            
            # Mock card lookups
            def mock_execute_side_effect(query, params):
                if 'SELECT decklist_id' in query:
                    mock_cur.fetchone.return_value = (1,)
                elif 'SELECT card_id' in query:
                    if params and 'Lightning Bolt' in params[0]:
                        mock_cur.fetchone.return_value = ('card1-uuid',)
                    elif params and 'Mountain' in params[0]:
                        mock_cur.fetchone.return_value = ('card2-uuid',)
            
            mock_cur.execute.side_effect = mock_execute_side_effect
            
            pipeline = ETLPipeline()
            
            with patch('src.etl.etl_pipeline.parse_decklist') as mock_parse:
                mock_parse.return_value = [
                    {'quantity': 4, 'card_name': 'Lightning Bolt', 'section': 'mainboard'},
                    {'quantity': 2, 'card_name': 'Mountain', 'section': 'mainboard'}
                ]
                
                with patch('src.etl.etl_pipeline.execute_batch') as mock_execute_batch:
                    pipeline.parse_and_store_decklist_cards('player1', 'tournament1', decklist_text, mock_conn)
                    
                    # Verify execute_batch was called
                    mock_execute_batch.assert_called_once()
                    
                    # Verify batch data structure
                    call_args = mock_execute_batch.call_args
                    batch_data = call_args[0][2]  # Third positional argument is the batch data (execute_batch: cur, sql, batch_data)
                    
                    assert len(batch_data) == 2
                    # Check first card
                    assert batch_data[0][0] == 1  # decklist_id
                    assert batch_data[0][1] == 'card1-uuid'  # card_id
                    assert batch_data[0][2] == 'mainboard'  # section
                    assert batch_data[0][3] == 4  # quantity
                    # Check second card
                    assert batch_data[1][0] == 1  # decklist_id
                    assert batch_data[1][1] == 'card2-uuid'  # card_id
                    assert batch_data[1][2] == 'mainboard'  # section
                    assert batch_data[1][3] == 2  # quantity
    
    def test_parse_and_store_decklist_cards_handles_mainboard_and_sideboard(self):
        """Test that mainboard and sideboard sections are correctly stored"""
        decklist_text = "4 Lightning Bolt\nSideboard\n2 Counterspell"
        
        with patch('src.etl.etl_pipeline.DatabaseConnection') as mock_db:
            mock_conn = MagicMock()
            mock_db.transaction.return_value.__enter__.return_value = mock_conn
            mock_db.transaction.return_value.__exit__.return_value = None
            mock_cur = MagicMock()
            mock_conn.cursor.return_value = mock_cur
            
            # Mock decklist_id lookup
            mock_cur.fetchone.return_value = (1,)
            
            # Mock card lookups
            def mock_execute_side_effect(query, params):
                if 'SELECT decklist_id' in query:
                    mock_cur.fetchone.return_value = (1,)
                elif 'SELECT card_id' in query:
                    if params and 'Lightning Bolt' in params[0]:
                        mock_cur.fetchone.return_value = ('card1-uuid',)
                    elif params and 'Counterspell' in params[0]:
                        mock_cur.fetchone.return_value = ('card2-uuid',)
            
            mock_cur.execute.side_effect = mock_execute_side_effect
            
            pipeline = ETLPipeline()
            
            with patch('src.etl.etl_pipeline.parse_decklist') as mock_parse:
                mock_parse.return_value = [
                    {'quantity': 4, 'card_name': 'Lightning Bolt', 'section': 'mainboard'},
                    {'quantity': 2, 'card_name': 'Counterspell', 'section': 'sideboard'}
                ]
                
                with patch('src.etl.etl_pipeline.execute_batch') as mock_execute_batch:
                    pipeline.parse_and_store_decklist_cards('player1', 'tournament1', decklist_text, mock_conn)
                    
                    # Verify execute_batch was called
                    mock_execute_batch.assert_called_once()
                    
                    # Verify batch data has correct sections
                    call_args = mock_execute_batch.call_args
                    batch_data = call_args[0][2]  # Third positional argument is the batch data (execute_batch: cur, sql, batch_data)
                    
                    assert len(batch_data) == 2
                    assert batch_data[0][2] == 'mainboard'
                    assert batch_data[1][2] == 'sideboard'
    
    def test_parse_and_store_decklist_cards_handles_decklist_not_found(self):
        """Test that method handles case when decklist_id is not found"""
        decklist_text = "4 Lightning Bolt"
        
        with patch('src.etl.etl_pipeline.DatabaseConnection') as mock_db:
            mock_conn = MagicMock()
            mock_db.transaction.return_value.__enter__.return_value = mock_conn
            mock_db.transaction.return_value.__exit__.return_value = None
            mock_cur = MagicMock()
            mock_conn.cursor.return_value = mock_cur
            
            # Mock decklist_id lookup - not found
            mock_cur.fetchone.return_value = None
            
            pipeline = ETLPipeline()
            
            with patch('src.etl.etl_pipeline.parse_decklist') as mock_parse:
                mock_parse.return_value = [
                    {'quantity': 4, 'card_name': 'Lightning Bolt', 'section': 'mainboard'}
                ]
                
                with patch('src.etl.etl_pipeline.execute_batch') as mock_execute_batch:
                    with patch('src.etl.etl_pipeline.logger') as mock_logger:
                        pipeline.parse_and_store_decklist_cards('player1', 'tournament1', decklist_text, mock_conn)
                        
                        # Verify warning was logged
                        mock_logger.warning.assert_called()
                        
                        # Verify execute_batch was not called
                        mock_execute_batch.assert_not_called()


# Tests for error handling (from test_card_data_error_handling.py)
class TestScryfallBulkDataUnavailable:
    """Tests for error handling when Scryfall bulk data is unavailable"""
    
    def test_load_cards_handles_oracle_cards_download_failure(self):
        """Test that error is handled when oracle cards bulk data download fails"""
        with patch('src.etl.etl_pipeline.ScryfallClient') as mock_client_class:
            mock_client = mock_client_class.return_value
            mock_client.download_oracle_cards.return_value = None  # Download failed
            
            result = load_cards_from_bulk_data()
            
            assert result['cards_loaded'] == 0
            assert result['cards_processed'] == 0
            assert result['errors'] == 1
    
    def test_load_cards_handles_rulings_download_failure(self):
        """Test that processing continues when rulings download fails"""
        mock_cards = [
            {
                'id': 'card1-uuid',
                'name': 'Lightning Bolt',
                'rulings': []
            }
        ]
        
        mock_bulk_data = {'data': mock_cards, 'file_path': '/path/to/data.json'}
        
        with patch('src.etl.etl_pipeline.ScryfallClient') as mock_client_class:
            mock_client = mock_client_class.return_value
            mock_client.download_oracle_cards.return_value = mock_bulk_data
            mock_client.download_rulings.return_value = None  # Rulings download failed
            mock_client.join_cards_with_rulings.return_value = mock_cards
            
            with patch('src.etl.etl_pipeline.DatabaseConnection') as mock_db:
                mock_conn = MagicMock()
                mock_db.transaction.return_value.__enter__.return_value = mock_conn
                mock_db.transaction.return_value.__exit__.return_value = None
                mock_cur = MagicMock()
                mock_conn.cursor.return_value = mock_cur
                
                with patch('src.etl.etl_pipeline.execute_batch'):
                    result = load_cards_from_bulk_data()
                    
                    # Should continue processing without rulings
                    assert result['cards_loaded'] == 1
                    # Verify join_cards_with_rulings was called with empty rulings
                    mock_client.join_cards_with_rulings.assert_called_once_with(mock_cards, [])


class TestDecklistParsingErrors:
    """Tests for error handling when decklist parsing fails"""
    
    def test_parse_and_store_decklist_cards_handles_parsing_exception(self):
        """Test that parsing exceptions are handled gracefully"""
        decklist_text = "4 Lightning Bolt"
        
        with patch('src.etl.etl_pipeline.DatabaseConnection') as mock_db:
            mock_conn = MagicMock()
            mock_db.transaction.return_value.__enter__.return_value = mock_conn
            mock_db.transaction.return_value.__exit__.return_value = None
            mock_cur = MagicMock()
            mock_conn.cursor.return_value = mock_cur
        
        pipeline = ETLPipeline()
        
        with patch('src.etl.etl_pipeline.parse_decklist') as mock_parse:
            mock_parse.side_effect = Exception("Parsing error")
            
            with patch('src.etl.etl_pipeline.logger') as mock_logger:
                # Should not raise exception, but log error
                try:
                    pipeline.parse_and_store_decklist_cards('player1', 'tournament1', decklist_text, mock_conn)
                except Exception:
                    # If exception is raised, verify it's logged
                    mock_logger.error.assert_called()
    
    def test_parse_and_store_decklist_cards_handles_empty_decklist(self):
        """Test that empty decklist is handled gracefully"""
        decklist_text = ""
        
        with patch('src.etl.etl_pipeline.DatabaseConnection') as mock_db:
            mock_conn = MagicMock()
            mock_db.transaction.return_value.__enter__.return_value = mock_conn
            mock_db.transaction.return_value.__exit__.return_value = None
            mock_cur = MagicMock()
            mock_conn.cursor.return_value = mock_cur
            
            # Mock decklist_id lookup
            mock_cur.fetchone.return_value = (1,)
        
        pipeline = ETLPipeline()
        
        with patch('src.etl.etl_pipeline.parse_decklist') as mock_parse:
            mock_parse.return_value = []  # Empty decklist
            
            with patch('src.etl.etl_pipeline.execute_batch') as mock_execute_batch:
                pipeline.parse_and_store_decklist_cards('player1', 'tournament1', decklist_text, mock_conn)
                
                # Should not call execute_batch for empty decklist
                mock_execute_batch.assert_not_called()


class TestTransactionRollback:
    """Tests for transaction rollback on card loading errors"""
    
    def test_load_cards_rolls_back_on_database_error(self):
        """Test that transaction is rolled back on database error"""
        mock_cards = [
            {
                'id': 'card1-uuid',
                'name': 'Lightning Bolt',
                'rulings': []
            }
        ]
        
        mock_bulk_data = {'data': mock_cards, 'file_path': '/path/to/data.json'}
        
        with patch('src.etl.etl_pipeline.ScryfallClient') as mock_client_class:
            mock_client = mock_client_class.return_value
            mock_client.download_oracle_cards.return_value = mock_bulk_data
            mock_client.download_rulings.return_value = {'data': [], 'file_path': '/path/to/rulings.json'}
            mock_client.join_cards_with_rulings.return_value = mock_cards
            mock_client.transform_card_to_db_row.return_value = {
                'card_id': 'card1-uuid',
                'name': 'Lightning Bolt',
                'rulings': ''
            }
            
            with patch('src.etl.etl_pipeline.DatabaseConnection') as mock_db:
                mock_conn = MagicMock()
                mock_transaction = MagicMock()
                mock_db.transaction.return_value = mock_transaction
                mock_transaction.__enter__.return_value = mock_conn
                mock_transaction.__exit__.return_value = None
                mock_cur = MagicMock()
                mock_conn.cursor.return_value = mock_cur
                
                # Simulate database error
                with patch('src.etl.etl_pipeline.execute_batch') as mock_execute_batch:
                    mock_execute_batch.side_effect = Exception("Database error")
                    
                    # The function catches batch errors and continues, but the transaction
                    # will still rollback if an exception escapes. Let's verify error handling.
                    result = load_cards_from_bulk_data()
                    
                    # Should have errors recorded
                    assert result['errors'] > 0
                    assert result['cards_loaded'] == 0
                    
                    # Verify transaction context manager was used (rollback happens in __exit__)
                    mock_transaction.__exit__.assert_called()
    
    def test_load_cards_handles_batch_insertion_error(self):
        """Test that batch insertion errors are handled and logged"""
        mock_cards = [
            {
                'id': 'card1-uuid',
                'name': 'Lightning Bolt',
                'rulings': []
            },
            {
                'id': 'card2-uuid',
                'name': 'Mountain',
                'rulings': []
            }
        ]
        
        mock_bulk_data = {'data': mock_cards, 'file_path': '/path/to/data.json'}
        
        with patch('src.etl.etl_pipeline.ScryfallClient') as mock_client_class:
            mock_client = mock_client_class.return_value
            mock_client.download_oracle_cards.return_value = mock_bulk_data
            mock_client.download_rulings.return_value = {'data': [], 'file_path': '/path/to/rulings.json'}
            mock_client.join_cards_with_rulings.return_value = mock_cards
            mock_client.transform_card_to_db_row.side_effect = lambda c: {
                'card_id': c['id'],
                'name': c['name'],
                'rulings': ''
            }
            
            with patch('src.etl.etl_pipeline.DatabaseConnection') as mock_db:
                mock_conn = MagicMock()
                mock_db.transaction.return_value.__enter__.return_value = mock_conn
                mock_db.transaction.return_value.__exit__.return_value = None
                mock_cur = MagicMock()
                mock_conn.cursor.return_value = mock_cur
                
                with patch('src.etl.etl_pipeline.execute_batch') as mock_execute_batch:
                    mock_execute_batch.side_effect = Exception("Batch insertion error")
                    
                    with patch('src.etl.etl_pipeline.logger') as mock_logger:
                        # Function catches batch errors and continues
                        result = load_cards_from_bulk_data()
                        
                        # Verify error was logged
                        mock_logger.error.assert_called()
                        
                        # Verify errors are recorded
                        assert result['errors'] > 0


class TestComprehensiveLogging:
    """Tests for comprehensive logging in card data operations"""
    
    def test_load_cards_logs_download_progress(self):
        """Test that card loading logs download progress"""
        mock_cards = [{'id': 'card1-uuid', 'name': 'Lightning Bolt', 'rulings': []}]
        mock_bulk_data = {'data': mock_cards, 'file_path': '/path/to/data.json'}
        
        with patch('src.etl.etl_pipeline.ScryfallClient') as mock_client_class:
            mock_client = mock_client_class.return_value
            mock_client.download_oracle_cards.return_value = mock_bulk_data
            mock_client.download_rulings.return_value = {'data': [], 'file_path': '/path/to/rulings.json'}
            mock_client.join_cards_with_rulings.return_value = mock_cards
            
            with patch('src.etl.etl_pipeline.DatabaseConnection') as mock_db:
                mock_conn = MagicMock()
                mock_db.transaction.return_value.__enter__.return_value = mock_conn
                mock_db.transaction.return_value.__exit__.return_value = None
                mock_cur = MagicMock()
                mock_conn.cursor.return_value = mock_cur
                
                with patch('src.etl.etl_pipeline.execute_batch'):
                    with patch('src.etl.etl_pipeline.logger') as mock_logger:
                        load_cards_from_bulk_data()
                        
                        # Verify info logs were called
                        info_calls = [str(call) for call in mock_logger.info.call_args_list]
                        assert any('Downloading' in str(call) or 'Downloaded' in str(call) for call in info_calls)


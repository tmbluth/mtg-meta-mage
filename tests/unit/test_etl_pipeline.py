"""Unit tests for ETL pipeline"""

import pytest
from unittest.mock import Mock, patch, MagicMock, call
from datetime import datetime, timedelta

from src.data.etl_pipeline import ETLPipeline


class TestETLPipelineInit:
    """Tests for ETLPipeline initialization"""
    
    def test_init_creates_topdeck_client(self):
        """Test that initialization creates TopDeck client"""
        with patch('src.data.etl_pipeline.TopDeckClient') as mock_client_class:
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
                
                mock_client_class.assert_called_once_with("test_key")
    
    def test_init_initializes_database_pool(self):
        """Test that initialization sets up database connection pool"""
        with patch('src.data.etl_pipeline.TopDeckClient'):
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool') as mock_init:
                pipeline = ETLPipeline()
                
                mock_init.assert_called_once()
    
    def test_init_uses_env_api_key_when_not_provided(self):
        """Test that API key from environment is used when not explicitly provided"""
        with patch('src.data.etl_pipeline.TopDeckClient') as mock_client_class:
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline()
                
                mock_client_class.assert_called_once_with(None)


class TestGetLastLoadTimestamp:
    """Tests for get_last_load_timestamp method"""
    
    def test_get_last_load_timestamp_returns_timestamp(self):
        """Test that last load timestamp is retrieved correctly"""
        with patch('src.data.etl_pipeline.TopDeckClient'):
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (1234567890,)
        
        with patch('src.data.etl_pipeline.DatabaseConnection.get_cursor') as mock_get_cursor:
            mock_get_cursor.return_value.__enter__.return_value = mock_cursor
            
            result = pipeline.get_last_load_timestamp()
            
            assert result == 1234567890
    
    def test_get_last_load_timestamp_returns_none_when_no_data(self):
        """Test that None is returned when no previous loads exist"""
        with patch('src.data.etl_pipeline.TopDeckClient'):
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = None
        
        with patch('src.data.etl_pipeline.DatabaseConnection.get_cursor') as mock_get_cursor:
            mock_get_cursor.return_value.__enter__.return_value = mock_cursor
            
            result = pipeline.get_last_load_timestamp()
            
            assert result is None
    
    def test_get_last_load_timestamp_handles_error(self):
        """Test that errors are handled gracefully"""
        with patch('src.data.etl_pipeline.TopDeckClient'):
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        with patch('src.data.etl_pipeline.DatabaseConnection.get_cursor', side_effect=Exception("DB Error")):
            result = pipeline.get_last_load_timestamp()
            
            assert result is None


class TestUpdateLoadMetadata:
    """Tests for update_load_metadata method"""
    
    def test_update_load_metadata_inserts_record(self):
        """Test that load metadata is inserted correctly"""
        with patch('src.data.etl_pipeline.TopDeckClient'):
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        mock_cursor = Mock()
        
        with patch('src.data.etl_pipeline.DatabaseConnection.get_cursor') as mock_get_cursor:
            mock_get_cursor.return_value.__enter__.return_value = mock_cursor
            
            pipeline.update_load_metadata(1234567890, 10)
            
            mock_cursor.execute.assert_called_once()
            call_args = mock_cursor.execute.call_args
            assert 1234567890 in call_args[0][1]
            assert 10 in call_args[0][1]
            assert 'incremental' in call_args[0][1]
    
    def test_update_load_metadata_raises_on_error(self):
        """Test that errors are raised"""
        with patch('src.data.etl_pipeline.TopDeckClient'):
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        with patch('src.data.etl_pipeline.DatabaseConnection.get_cursor', side_effect=Exception("DB Error")):
            with pytest.raises(Exception, match="DB Error"):
                pipeline.update_load_metadata(1234567890, 10)


class TestInsertTournament:
    """Tests for insert_tournament method"""
    
    def test_insert_tournament_executes_insert(self):
        """Test that tournament is inserted with correct data"""
        with patch('src.data.etl_pipeline.TopDeckClient'):
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
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
        with patch('src.data.etl_pipeline.TopDeckClient'):
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
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
        with patch('src.data.etl_pipeline.TopDeckClient'):
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
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
        with patch('src.data.etl_pipeline.TopDeckClient'):
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
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
        
        with patch('src.data.etl_pipeline.execute_batch') as mock_execute_batch:
            pipeline.insert_players('test123', players, mock_conn)
            
            mock_execute_batch.assert_called_once()
            call_args = mock_execute_batch.call_args
            assert mock_cursor == call_args[0][0]
            assert len(call_args[0][2]) == 1
            assert call_args[0][2][0][0] == 'player1'
    
    def test_insert_players_handles_empty_list(self):
        """Test that empty player list is handled gracefully"""
        with patch('src.data.etl_pipeline.TopDeckClient'):
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        mock_conn = Mock()
        
        # Should not raise error
        pipeline.insert_players('test123', [], mock_conn)
        
        # Should not create cursor for empty list
        mock_conn.cursor.assert_not_called()
    
    def test_insert_players_handles_missing_fields(self):
        """Test that missing player fields default correctly"""
        with patch('src.data.etl_pipeline.TopDeckClient'):
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        players = [{'id': 'player1'}]  # Minimal player data
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        with patch('src.data.etl_pipeline.execute_batch') as mock_execute_batch:
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
        with patch('src.data.etl_pipeline.TopDeckClient'):
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        players = [
            {'id': 'player1', 'decklist': '4 Lightning Bolt\n4 Counterspell'},
            {'id': 'player2', 'decklist': '4 Llanowar Elves'}
        ]
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        with patch('src.data.etl_pipeline.execute_batch') as mock_execute_batch:
            pipeline.insert_decklists('test123', players, mock_conn)
            
            mock_execute_batch.assert_called_once()
            call_args = mock_execute_batch.call_args[0][2]
            assert len(call_args) == 2
            assert call_args[0][0] == 'player1'
    
    def test_insert_decklists_skips_players_without_decklist(self):
        """Test that players without decklists are skipped"""
        with patch('src.data.etl_pipeline.TopDeckClient'):
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        players = [
            {'id': 'player1', 'decklist': '4 Lightning Bolt'},
            {'id': 'player2'}  # No decklist
        ]
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        with patch('src.data.etl_pipeline.execute_batch') as mock_execute_batch:
            pipeline.insert_decklists('test123', players, mock_conn)
            
            call_args = mock_execute_batch.call_args[0][2]
            assert len(call_args) == 1
    
    def test_insert_decklists_handles_empty_list(self):
        """Test that empty player list is handled gracefully"""
        with patch('src.data.etl_pipeline.TopDeckClient'):
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        mock_conn = Mock()
        
        # Should not raise error
        pipeline.insert_decklists('test123', [], mock_conn)
        
        mock_conn.cursor.assert_not_called()


class TestInsertMatchRounds:
    """Tests for insert_match_rounds method"""
    
    def test_insert_match_rounds_handles_numeric_rounds(self):
        """Test that numeric round numbers are handled correctly"""
        with patch('src.data.etl_pipeline.TopDeckClient'):
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
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
        
        with patch('src.data.etl_pipeline.execute_batch'):
            with patch('src.data.etl_pipeline.is_valid_match', return_value=True):
                pipeline.insert_match_rounds('test123', rounds_data, mock_conn)
    
    def test_insert_match_rounds_handles_string_rounds(self):
        """Test that string round names are converted to numbers"""
        with patch('src.data.etl_pipeline.TopDeckClient'):
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
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
        
        with patch('src.data.etl_pipeline.execute_batch') as mock_execute_batch:
            with patch('src.data.etl_pipeline.is_valid_match', return_value=True):
                pipeline.insert_match_rounds('test123', rounds_data, mock_conn)
                
                # Check that Top 8 was converted to 1000
                round_data_call = mock_execute_batch.call_args_list[0][0][2]
                assert round_data_call[0][0] == 1000
    
    def test_insert_match_rounds_filters_invalid_matches(self):
        """Test that invalid matches are filtered out"""
        with patch('src.data.etl_pipeline.TopDeckClient'):
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
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
        
        with patch('src.data.etl_pipeline.execute_batch') as mock_execute_batch:
            with patch('src.data.etl_pipeline.is_valid_match', side_effect=[True, False]):
                pipeline.insert_match_rounds('test123', rounds_data, mock_conn)
                
                # Should have 2 execute_batch calls (rounds and matches)
                matches_call = mock_execute_batch.call_args_list[1][0][2]
                assert len(matches_call) == 1  # Only 1 valid match
    
    def test_insert_match_rounds_handles_empty_list(self):
        """Test that empty rounds list is handled gracefully"""
        with patch('src.data.etl_pipeline.TopDeckClient'):
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        mock_conn = Mock()
        
        # Should not raise error
        pipeline.insert_match_rounds('test123', [], mock_conn)
        
        mock_conn.cursor.assert_not_called()
    
    def test_insert_match_rounds_skips_matches_with_missing_players(self):
        """Test that matches with players not in database are skipped to avoid FK violations"""
        with patch('src.data.etl_pipeline.TopDeckClient'):
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
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
        
        with patch('src.data.etl_pipeline.execute_batch') as mock_execute_batch:
            with patch('src.data.etl_pipeline.is_valid_match', return_value=True):
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
        with patch('src.data.etl_pipeline.TopDeckClient'):
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
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
        
        with patch('src.data.etl_pipeline.execute_batch'):
            with patch('src.data.etl_pipeline.is_valid_match', return_value=True):
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
        with patch('src.data.etl_pipeline.TopDeckClient') as mock_client_class:
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
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
        
        with patch('src.data.etl_pipeline.DatabaseConnection.transaction') as mock_transaction:
            mock_transaction.return_value.__enter__.return_value = mock_conn
            with patch.object(pipeline, 'insert_tournament'):
                with patch.object(pipeline, 'insert_players'):
                    with patch.object(pipeline, 'insert_decklists'):
                        with patch.object(pipeline, 'insert_match_rounds'):
                            with patch('src.data.etl_pipeline.filter_rounds_data', return_value=[{'round': 1}]):
                                result = pipeline.load_tournament(tournament, include_rounds=True)
                                
                                assert result is True
    
    def test_load_tournament_returns_false_on_missing_tid(self):
        """Test that missing TID returns False"""
        with patch('src.data.etl_pipeline.TopDeckClient'):
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        tournament = {'tournamentName': 'Test'}  # No TID
        
        result = pipeline.load_tournament(tournament)
        
        assert result is False
    
    def test_load_tournament_handles_error(self):
        """Test that errors are handled gracefully"""
        with patch('src.data.etl_pipeline.TopDeckClient'):
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
                pipeline = ETLPipeline(api_key="test_key")
        
        tournament = {'TID': 'test123'}
        
        with patch('src.data.etl_pipeline.DatabaseConnection.transaction', side_effect=Exception("DB Error")):
            result = pipeline.load_tournament(tournament)
            
            assert result is False
    
    def test_load_tournament_skips_rounds_when_not_requested(self):
        """Test that rounds are skipped when include_rounds=False"""
        with patch('src.data.etl_pipeline.TopDeckClient') as mock_client_class:
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
                mock_client = Mock()
                mock_client_class.return_value = mock_client
                
                pipeline = ETLPipeline(api_key="test_key")
                pipeline.client = mock_client
        
        tournament = {'TID': 'test123'}
        
        mock_client.get_tournament_details.return_value = None
        mock_conn = Mock()
        
        with patch('src.data.etl_pipeline.DatabaseConnection.transaction') as mock_transaction:
            mock_transaction.return_value.__enter__.return_value = mock_conn
            with patch.object(pipeline, 'insert_tournament'):
                pipeline.load_tournament(tournament, include_rounds=False)
                
                # Should not call get_tournament_rounds
                mock_client.get_tournament_rounds.assert_not_called()


class TestLoadInitial:
    """Tests for load_initial method"""
    
    def test_load_initial_fetches_tournaments_for_multiple_formats(self):
        """Test that tournaments are fetched for multiple formats using 'last' parameter"""
        with patch('src.data.etl_pipeline.TopDeckClient') as mock_client_class:
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
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
                with patch('src.data.etl_pipeline.filter_tournaments', side_effect=lambda x: x):
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
        with patch('src.data.etl_pipeline.TopDeckClient') as mock_client_class:
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
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
        with patch('src.data.etl_pipeline.TopDeckClient') as mock_client_class:
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
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
                with patch('src.data.etl_pipeline.filter_tournaments', side_effect=lambda x: x):
                    result = pipeline.load_initial(days_back=30)
                    
                    # Should only load once despite being returned by multiple formats
                    assert pipeline.load_tournament.call_count == 1
    
    def test_load_initial_updates_metadata(self):
        """Test that load metadata is updated after successful load"""
        with patch('src.data.etl_pipeline.TopDeckClient') as mock_client_class:
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
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
                with patch('src.data.etl_pipeline.filter_tournaments', side_effect=lambda x: x):
                    pipeline.load_initial(days_back=30)
                    
                    mock_update.assert_called_once()


class TestLoadIncremental:
    """Tests for load_incremental method"""
    
    def test_load_incremental_fetches_since_last_timestamp(self):
        """Test that incremental load fetches tournaments for multiple formats since last load"""
        with patch('src.data.etl_pipeline.TopDeckClient') as mock_client_class:
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
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
        with patch('src.data.etl_pipeline.time.time', return_value=1000000):
            with patch.object(pipeline, 'get_last_load_timestamp', return_value=900000):
                with patch.object(pipeline, 'load_tournament', return_value=True):
                    with patch.object(pipeline, 'update_load_metadata'):
                        with patch('src.data.etl_pipeline.filter_tournaments', side_effect=lambda x: x):
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
        with patch('src.data.etl_pipeline.TopDeckClient') as mock_client_class:
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
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
        with patch('src.data.etl_pipeline.TopDeckClient') as mock_client_class:
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
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
        with patch('src.data.etl_pipeline.TopDeckClient') as mock_client_class:
            with patch('src.data.etl_pipeline.DatabaseConnection.initialize_pool'):
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
                with patch('src.data.etl_pipeline.filter_tournaments', side_effect=lambda x: x):
                    result = pipeline.load_initial(days_back=30)
                    
                    # Should still load tournaments from formats that succeeded
                    assert result >= 1


"""Unit tests for core ETL pipeline class methods"""

import pytest
from unittest.mock import Mock, MagicMock, patch

from src.etl.etl_pipeline import ETLPipeline, parse_decklist
from tests.unit.test_etl_pipeline_helpers import (
    create_mock_tournament,
    create_mock_player,
    create_mock_round
)


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
    
    def test_get_last_load_timestamp_returns_timestamp(self, mock_pipeline):
        """Test that last load timestamp is retrieved correctly"""
        pipeline, _ = mock_pipeline
        
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (1234567890,)
        
        with patch('src.etl.etl_pipeline.DatabaseConnection.get_cursor') as mock_get_cursor:
            mock_get_cursor.return_value.__enter__.return_value = mock_cursor
            
            result = pipeline.get_last_load_timestamp()
            
            assert result == 1234567890
    
    def test_get_last_load_timestamp_returns_none_when_no_data(self, mock_pipeline):
        """Test that None is returned when no previous loads exist"""
        pipeline, _ = mock_pipeline
        
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = None
        
        with patch('src.etl.etl_pipeline.DatabaseConnection.get_cursor') as mock_get_cursor:
            mock_get_cursor.return_value.__enter__.return_value = mock_cursor
            
            result = pipeline.get_last_load_timestamp()
            
            assert result is None
    
    def test_get_last_load_timestamp_handles_error(self, mock_pipeline):
        """Test that errors are handled gracefully"""
        pipeline, _ = mock_pipeline
        
        with patch('src.etl.etl_pipeline.DatabaseConnection.get_cursor', side_effect=Exception("DB Error")):
            result = pipeline.get_last_load_timestamp()
            
            assert result is None


class TestUpdateLoadMetadata:
    """Tests for update_load_metadata method"""
    
    def test_update_load_metadata_inserts_record(self, mock_pipeline):
        """Test that load metadata is inserted correctly"""
        pipeline, _ = mock_pipeline
        
        mock_cursor = Mock()
        
        with patch('src.etl.etl_pipeline.DatabaseConnection.get_cursor') as mock_get_cursor:
            mock_get_cursor.return_value.__enter__.return_value = mock_cursor
            
            pipeline.update_load_metadata(1234567890, 10)
            
            mock_cursor.execute.assert_called_once()
            call_args = mock_cursor.execute.call_args
            assert 1234567890 in call_args[0][1]
            assert 10 in call_args[0][1]
            assert 'incremental' in call_args[0][1]
    
    def test_update_load_metadata_raises_on_error(self, mock_pipeline):
        """Test that errors are raised"""
        pipeline, _ = mock_pipeline
        
        with patch('src.etl.etl_pipeline.DatabaseConnection.get_cursor', side_effect=Exception("DB Error")):
            with pytest.raises(Exception, match="DB Error"):
                pipeline.update_load_metadata(1234567890, 10)


class TestLoadTournament:
    """Tests for load_tournament method"""
    
    def test_load_tournament_success(self, mock_pipeline):
        """Test successful tournament load"""
        pipeline, mock_client = mock_pipeline
        
        tournament = create_mock_tournament(TID='test123', tournamentName='Test')
        
        mock_client.get_tournament_details.return_value = {
            'players': [create_mock_player(id='p1', name='Alice')]
        }
        mock_client.get_tournament_rounds.return_value = [
            create_mock_round(round=1, tables=[])
        ]
        
        with patch('src.etl.etl_pipeline.DatabaseConnection.transaction') as mock_transaction:
            mock_conn = Mock()
            mock_transaction.return_value.__enter__.return_value = mock_conn
            with patch.object(pipeline, 'insert_tournament'):
                with patch.object(pipeline, 'insert_players'):
                    with patch.object(pipeline, 'insert_decklists'):
                        with patch.object(pipeline, 'insert_match_rounds'):
                            with patch('src.etl.etl_pipeline.filter_rounds_data', return_value=[{'round': 1}]):
                                result = pipeline.load_tournament(tournament, include_rounds=True)
                                
                                assert result is True
    
    def test_load_tournament_returns_false_on_missing_tid(self, mock_pipeline):
        """Test that missing TID returns False"""
        pipeline, _ = mock_pipeline
        
        tournament = create_mock_tournament(tournamentName='Test')
        del tournament['TID']
        
        result = pipeline.load_tournament(tournament)
        
        assert result is False
    
    def test_load_tournament_handles_error(self, mock_pipeline):
        """Test that errors are handled gracefully"""
        pipeline, _ = mock_pipeline
        
        tournament = create_mock_tournament(TID='test123')
        
        with patch('src.etl.etl_pipeline.DatabaseConnection.transaction', side_effect=Exception("DB Error")):
            result = pipeline.load_tournament(tournament)
            
            assert result is False
    
    def test_load_tournament_skips_rounds_when_not_requested(self, mock_pipeline):
        """Test that rounds are skipped when include_rounds=False"""
        pipeline, mock_client = mock_pipeline
        
        tournament = create_mock_tournament(TID='test123')
        
        mock_client.get_tournament_details.return_value = None
        
        with patch('src.etl.etl_pipeline.DatabaseConnection.transaction') as mock_transaction:
            mock_conn = Mock()
            mock_transaction.return_value.__enter__.return_value = mock_conn
            with patch.object(pipeline, 'insert_tournament'):
                pipeline.load_tournament(tournament, include_rounds=False)
                
                # Should not call get_tournament_rounds
                mock_client.get_tournament_rounds.assert_not_called()


class TestLoadInitial:
    """Tests for load_initial method"""
    
    def test_load_initial_fetches_tournaments_for_multiple_formats(self, mock_pipeline):
        """Test that tournaments are fetched for multiple formats using 'last' parameter"""
        pipeline, mock_client = mock_pipeline
        
        # Mock get_tournaments to return tournaments for some formats
        def mock_get_tournaments(**kwargs):
            format_name = kwargs.get('format')
            if format_name == 'Standard':
                return [create_mock_tournament(TID='t1', format='Standard', startDate=1000)]
            elif format_name == 'Modern':
                return [create_mock_tournament(TID='t2', format='Modern', startDate=2000)]
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
    
    def test_load_initial_returns_zero_when_no_tournaments(self, mock_pipeline):
        """Test that 0 is returned when no tournaments found"""
        pipeline, mock_client = mock_pipeline
        
        # Mock to return empty list for all formats
        mock_client.get_tournaments.return_value = []
        
        result = pipeline.load_initial(days_back=30)
        
        assert result == 0
    
    def test_load_initial_deduplicates_tournaments_by_tid(self, mock_pipeline):
        """Test that duplicate tournaments (same TID) are deduplicated across formats"""
        pipeline, mock_client = mock_pipeline
        
        # Mock to return same tournament ID for multiple formats
        def mock_get_tournaments(**kwargs):
            return [create_mock_tournament(TID='t1', format=kwargs.get('format'), startDate=1000)]
        
        mock_client.get_tournaments.side_effect = mock_get_tournaments
        
        with patch.object(pipeline, 'load_tournament', return_value=True):
            with patch.object(pipeline, 'update_load_metadata'):
                with patch('src.etl.etl_pipeline.filter_tournaments', side_effect=lambda x: x):
                    result = pipeline.load_initial(days_back=30)
                    
                    # Should only load once despite being returned by multiple formats
                    assert pipeline.load_tournament.call_count == 1
    
    def test_load_initial_updates_metadata(self, mock_pipeline):
        """Test that load metadata is updated after successful load"""
        pipeline, mock_client = mock_pipeline
        
        # Mock to return tournaments for at least one format
        def mock_get_tournaments(**kwargs):
            if kwargs.get('format') == 'Standard':
                return [create_mock_tournament(TID='t1', format='Standard', startDate=1000)]
            return []
        
        mock_client.get_tournaments.side_effect = mock_get_tournaments
        
        with patch.object(pipeline, 'load_tournament', return_value=True):
            with patch.object(pipeline, 'update_load_metadata') as mock_update:
                with patch('src.etl.etl_pipeline.filter_tournaments', side_effect=lambda x: x):
                    pipeline.load_initial(days_back=30)
                    
                    mock_update.assert_called_once()
    
    def test_load_initial_handles_format_errors_gracefully(self, mock_pipeline):
        """Test that errors for individual formats don't stop the entire load"""
        pipeline, mock_client = mock_pipeline
        
        # Mock to raise error for some formats but return data for others
        call_count = 0
        def mock_get_tournaments(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("API Error")
            return [create_mock_tournament(TID='t1', format=kwargs.get('format'), startDate=1000)]
        
        mock_client.get_tournaments.side_effect = mock_get_tournaments
        
        with patch.object(pipeline, 'load_tournament', return_value=True):
            with patch.object(pipeline, 'update_load_metadata'):
                with patch('src.etl.etl_pipeline.filter_tournaments', side_effect=lambda x: x):
                    result = pipeline.load_initial(days_back=30)
                    
                    # Should still load tournaments from formats that succeeded
                    assert result >= 1


class TestLoadIncremental:
    """Tests for load_incremental method"""
    
    def test_load_incremental_fetches_since_last_timestamp(self, mock_pipeline):
        """Test that incremental load fetches tournaments for multiple formats since last load"""
        pipeline, mock_client = mock_pipeline
        
        # Mock get_tournaments to return tournaments for some formats
        def mock_get_tournaments(**kwargs):
            format_name = kwargs.get('format')
            if format_name == 'Standard':
                return [create_mock_tournament(TID='t1', format='Standard', startDate=2000)]
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
    
    def test_load_incremental_calls_initial_when_no_previous_load(self, mock_pipeline):
        """Test that initial load is called when no previous load exists"""
        pipeline, _ = mock_pipeline
        
        with patch.object(pipeline, 'get_last_load_timestamp', return_value=None):
            with patch.object(pipeline, 'load_initial', return_value=5) as mock_initial:
                result = pipeline.load_incremental()
                
                mock_initial.assert_called_once()
                assert result == 5
    
    def test_load_incremental_returns_zero_when_no_new_tournaments(self, mock_pipeline):
        """Test that 0 is returned when no new tournaments found"""
        pipeline, mock_client = mock_pipeline
        
        # Mock to return empty list for all formats
        mock_client.get_tournaments.return_value = []
        
        with patch.object(pipeline, 'get_last_load_timestamp', return_value=1000):
            result = pipeline.load_incremental()
            
            assert result == 0


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
            mock_cur.fetchone.return_value = (1,)
            
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
                    batch_data = call_args[0][2]  # Third positional argument is the batch data
                    
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
                    batch_data = call_args[0][2]  # Third positional argument is the batch data
                    
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


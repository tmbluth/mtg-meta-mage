"""Unit tests for Scryfall-related ETL pipeline functions"""

import pytest
from unittest.mock import Mock, MagicMock, patch

from src.etl.etl_pipeline import load_cards_from_bulk_data
from tests.unit.test_etl_pipeline_helpers import create_mock_card, create_mock_bulk_data


class TestLoadCardsFromBulkData:
    """Tests for loading cards from Scryfall bulk data into database"""
    
    def test_load_cards_from_bulk_data_success(self, mock_scryfall_client, mock_db_transaction):
        """Test successful loading of cards from bulk data"""
        mock_conn, mock_cur, mock_transaction = mock_db_transaction
        
        mock_cards = [
            create_mock_card(
                id='card1-uuid',
                name='Lightning Bolt',
                set='M21',
                collector_number='161'
            ),
            create_mock_card(
                id='card2-uuid',
                name='Counterspell',
                set='M21',
                collector_number='48',
                mana_cost='{U}{U}',
                cmc=2.0,
                color_identity=['U']
            )
        ]
        
        mock_scryfall_client.download_oracle_cards.return_value = create_mock_bulk_data(mock_cards)
        mock_scryfall_client.download_rulings.return_value = {'data': [], 'file_path': '/path/to/rulings.json'}
        mock_scryfall_client.join_cards_with_rulings.return_value = mock_cards
        
        with patch('src.etl.etl_pipeline.execute_batch') as mock_execute_batch:
            result = load_cards_from_bulk_data()
            
            assert result['cards_loaded'] == 2
            assert result['cards_processed'] == 2
            mock_execute_batch.assert_called_once()
    
    def test_load_cards_from_bulk_data_with_rulings(self, mock_scryfall_client, mock_db_transaction):
        """Test loading cards with rulings joined"""
        mock_conn, mock_cur, mock_transaction = mock_db_transaction
        
        mock_cards = [
            create_mock_card(
                id='card1-uuid',
                oracle_id='oracle1',
                name='Lightning Bolt',
                rulings=['First ruling', 'Second ruling']
            )
        ]
        
        mock_rulings = [
            {'oracle_id': 'oracle1', 'comment': 'First ruling'},
            {'oracle_id': 'oracle1', 'comment': 'Second ruling'}
        ]
        
        mock_scryfall_client.download_oracle_cards.return_value = create_mock_bulk_data(mock_cards)
        mock_scryfall_client.download_rulings.return_value = {'data': mock_rulings, 'file_path': '/path/to/rulings.json'}
        mock_scryfall_client.join_cards_with_rulings.return_value = mock_cards
        
        with patch('src.etl.etl_pipeline.execute_batch'):
            result = load_cards_from_bulk_data()
            
            # Verify join_cards_with_rulings was called
            mock_scryfall_client.join_cards_with_rulings.assert_called_once()
            assert result['cards_loaded'] == 1


class TestHandleDuplicateCards:
    """Tests for handling duplicate cards (upsert logic)"""
    
    def test_load_cards_handles_duplicate_card_ids(self, mock_scryfall_client, mock_db_transaction):
        """Test that duplicate cards are handled via upsert"""
        mock_conn, mock_cur, mock_transaction = mock_db_transaction
        
        mock_cards = [create_mock_card(id='card1-uuid', name='Lightning Bolt')]
        
        mock_scryfall_client.download_oracle_cards.return_value = create_mock_bulk_data(mock_cards)
        mock_scryfall_client.download_rulings.return_value = {'data': [], 'file_path': '/path/to/rulings.json'}
        mock_scryfall_client.join_cards_with_rulings.return_value = mock_cards
        
        with patch('src.etl.etl_pipeline.execute_batch') as mock_execute_batch:
            # Load same cards twice
            load_cards_from_bulk_data()
            result = load_cards_from_bulk_data()
            
            # Should handle duplicates via upsert (ON CONFLICT DO UPDATE)
            assert result['cards_loaded'] == 1
            # Verify execute_batch was called (for upsert)
            assert mock_execute_batch.call_count == 2
    
    def test_load_cards_updates_existing_card(self, mock_scryfall_client, mock_db_transaction):
        """Test that existing cards are updated when reloaded"""
        mock_conn, mock_cur, mock_transaction = mock_db_transaction
        
        mock_cards = [
            create_mock_card(
                id='card1-uuid',
                name='Lightning Bolt',
                oracle_text='Updated oracle text'
            )
        ]
        
        mock_scryfall_client.download_oracle_cards.return_value = create_mock_bulk_data(mock_cards)
        mock_scryfall_client.download_rulings.return_value = {'data': [], 'file_path': '/path/to/rulings.json'}
        mock_scryfall_client.join_cards_with_rulings.return_value = mock_cards
        
        with patch('src.etl.etl_pipeline.execute_batch') as mock_execute_batch:
            result = load_cards_from_bulk_data()
            
            # Should use ON CONFLICT DO UPDATE for upsert
            assert result['cards_loaded'] == 1
            mock_execute_batch.assert_called_once()


class TestJoinAndStoreRulings:
    """Tests for joining and storing rulings"""
    
    def test_load_cards_joins_rulings_with_cards(self, mock_scryfall_client, mock_db_transaction):
        """Test that rulings are joined with cards by oracle_id"""
        mock_conn, mock_cur, mock_transaction = mock_db_transaction
        
        mock_cards = [
            create_mock_card(id='card1-uuid', oracle_id='oracle1', name='Lightning Bolt', rulings=[])
        ]
        
        mock_rulings = [
            {'oracle_id': 'oracle1', 'comment': 'First ruling'},
            {'oracle_id': 'oracle1', 'comment': 'Second ruling'}
        ]
        
        mock_cards_with_rulings = [
            create_mock_card(
                id='card1-uuid',
                oracle_id='oracle1',
                name='Lightning Bolt',
                rulings=['First ruling', 'Second ruling']
            )
        ]
        
        mock_scryfall_client.download_oracle_cards.return_value = create_mock_bulk_data(mock_cards)
        mock_scryfall_client.download_rulings.return_value = {'data': mock_rulings, 'file_path': '/path/to/rulings.json'}
        mock_scryfall_client.join_cards_with_rulings.return_value = mock_cards_with_rulings
        
        with patch('src.etl.etl_pipeline.execute_batch'):
            result = load_cards_from_bulk_data()
            
            # Verify join_cards_with_rulings was called with correct arguments
            mock_scryfall_client.join_cards_with_rulings.assert_called_once_with(mock_cards, mock_rulings)
            assert result['cards_loaded'] == 1
    
    def test_load_cards_stores_rulings_as_comma_separated(self, mock_scryfall_client, mock_db_transaction):
        """Test that rulings are stored as comma-separated string"""
        mock_conn, mock_cur, mock_transaction = mock_db_transaction
        
        mock_cards = [
            create_mock_card(
                id='card1-uuid',
                oracle_id='oracle1',
                name='Lightning Bolt',
                rulings=['First ruling', 'Second ruling']
            )
        ]
        
        mock_scryfall_client.download_oracle_cards.return_value = create_mock_bulk_data(mock_cards)
        mock_scryfall_client.download_rulings.return_value = {'data': [], 'file_path': '/path/to/rulings.json'}
        mock_scryfall_client.join_cards_with_rulings.return_value = mock_cards
        mock_scryfall_client.transform_card_to_db_row.return_value = {
            'card_id': 'card1-uuid',
            'name': 'Lightning Bolt',
            'rulings': 'First ruling, Second ruling'
        }
        
        with patch('src.etl.etl_pipeline.execute_batch'):
            result = load_cards_from_bulk_data()
            
            # Verify transform_card_to_db_row was called (which concatenates rulings)
            mock_scryfall_client.transform_card_to_db_row.assert_called()
            assert result['cards_loaded'] == 1
    
    def test_load_cards_handles_cards_without_rulings(self, mock_scryfall_client, mock_db_transaction):
        """Test that cards without rulings are handled correctly"""
        mock_conn, mock_cur, mock_transaction = mock_db_transaction
        
        mock_cards = [
            create_mock_card(id='card1-uuid', oracle_id='oracle1', name='Lightning Bolt', rulings=[])
        ]
        
        mock_scryfall_client.download_oracle_cards.return_value = create_mock_bulk_data(mock_cards)
        mock_scryfall_client.download_rulings.return_value = {'data': [], 'file_path': '/path/to/rulings.json'}
        mock_scryfall_client.join_cards_with_rulings.return_value = mock_cards
        mock_scryfall_client.transform_card_to_db_row.return_value = {
            'card_id': 'card1-uuid',
            'name': 'Lightning Bolt',
            'rulings': ''
        }
        
        with patch('src.etl.etl_pipeline.execute_batch'):
            result = load_cards_from_bulk_data()
            
            assert result['cards_loaded'] == 1


class TestScryfallBulkDataUnavailable:
    """Tests for error handling when Scryfall bulk data is unavailable"""
    
    def test_load_cards_handles_oracle_cards_download_failure(self, mock_scryfall_client):
        """Test that error is handled when oracle cards bulk data download fails"""
        mock_scryfall_client.download_oracle_cards.return_value = None  # Download failed
        
        result = load_cards_from_bulk_data()
        
        assert result['cards_loaded'] == 0
        assert result['cards_processed'] == 0
        assert result['errors'] == 1
    
    def test_load_cards_handles_rulings_download_failure(self, mock_scryfall_client, mock_db_transaction):
        """Test that processing continues when rulings download fails"""
        mock_conn, mock_cur, mock_transaction = mock_db_transaction
        
        mock_cards = [create_mock_card(id='card1-uuid', name='Lightning Bolt', rulings=[])]
        
        mock_scryfall_client.download_oracle_cards.return_value = create_mock_bulk_data(mock_cards)
        mock_scryfall_client.download_rulings.return_value = None  # Rulings download failed
        mock_scryfall_client.join_cards_with_rulings.return_value = mock_cards
        
        with patch('src.etl.etl_pipeline.execute_batch'):
            result = load_cards_from_bulk_data()
            
            # Should continue processing without rulings
            assert result['cards_loaded'] == 1
            # Verify join_cards_with_rulings was called with empty rulings
            mock_scryfall_client.join_cards_with_rulings.assert_called_once_with(mock_cards, [])


class TestTransactionRollback:
    """Tests for transaction rollback on card loading errors"""
    
    def test_load_cards_rolls_back_on_database_error(self, mock_scryfall_client):
        """Test that transaction is rolled back on database error"""
        mock_cards = [create_mock_card(id='card1-uuid', name='Lightning Bolt', rulings=[])]
        
        mock_scryfall_client.download_oracle_cards.return_value = create_mock_bulk_data(mock_cards)
        mock_scryfall_client.download_rulings.return_value = {'data': [], 'file_path': '/path/to/rulings.json'}
        mock_scryfall_client.join_cards_with_rulings.return_value = mock_cards
        mock_scryfall_client.transform_card_to_db_row.return_value = {
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
                
                result = load_cards_from_bulk_data()
                
                # Should have errors recorded
                assert result['errors'] > 0
                assert result['cards_loaded'] == 0
                
                # Verify transaction context manager was used (rollback happens in __exit__)
                mock_transaction.__exit__.assert_called()
    
    def test_load_cards_handles_batch_insertion_error(self, mock_scryfall_client):
        """Test that batch insertion errors are handled and logged"""
        mock_cards = [
            create_mock_card(id='card1-uuid', name='Lightning Bolt'),
            create_mock_card(id='card2-uuid', name='Mountain')
        ]
        
        mock_scryfall_client.download_oracle_cards.return_value = create_mock_bulk_data(mock_cards)
        mock_scryfall_client.download_rulings.return_value = {'data': [], 'file_path': '/path/to/rulings.json'}
        mock_scryfall_client.join_cards_with_rulings.return_value = mock_cards
        mock_scryfall_client.transform_card_to_db_row.side_effect = lambda c: {
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
                    result = load_cards_from_bulk_data()
                    
                    # Verify error was logged
                    mock_logger.error.assert_called()
                    
                    # Verify errors are recorded
                    assert result['errors'] > 0


class TestComprehensiveLogging:
    """Tests for comprehensive logging in card data operations"""
    
    def test_load_cards_logs_download_progress(self, mock_scryfall_client, mock_db_transaction):
        """Test that card loading logs download progress"""
        mock_conn, mock_cur, mock_transaction = mock_db_transaction
        
        mock_cards = [create_mock_card(id='card1-uuid', name='Lightning Bolt')]
        
        mock_scryfall_client.download_oracle_cards.return_value = create_mock_bulk_data(mock_cards)
        mock_scryfall_client.download_rulings.return_value = {'data': [], 'file_path': '/path/to/rulings.json'}
        mock_scryfall_client.join_cards_with_rulings.return_value = mock_cards
        
        with patch('src.etl.etl_pipeline.execute_batch'):
            with patch('src.etl.etl_pipeline.logger') as mock_logger:
                load_cards_from_bulk_data()
                
                # Verify info logs were called
                info_calls = [str(call) for call in mock_logger.info.call_args_list]
                assert any('Downloading' in str(call) or 'Downloaded' in str(call) for call in info_calls)


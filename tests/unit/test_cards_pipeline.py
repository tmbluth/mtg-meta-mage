"""Unit tests for cards_pipeline module"""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, List

from src.etl.cards_pipeline import CardsPipeline


@pytest.fixture
def mock_scryfall_client():
    """Create a mock ScryfallClient"""
    with patch('src.etl.cards_pipeline.ScryfallClient') as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        yield mock_client


@pytest.fixture
def mock_db_connection():
    """Create a mock database connection"""
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn


@pytest.fixture
def sample_card_data():
    """Sample card data for testing"""
    return {
        'id': 'card_123',
        'name': 'Lightning Bolt',
        'set': 'M21',
        'collector_number': '161',
        'oracle_text': 'Deal 3 damage',
        'type_line': 'Instant',
        'mana_cost': '{R}',
        'cmc': 1,
        'color_identity': ['R'],
        'scryfall_uri': 'https://scryfall.com/card/m21/161',
        'oracle_id': 'oracle_123'
    }


@pytest.fixture
def sample_rulings_data():
    """Sample rulings data for testing"""
    return [
        {
            'oracle_id': 'oracle_123',
            'comment': 'This is a ruling'
        },
        {
            'oracle_id': 'oracle_123',
            'comment': 'Another ruling'
        }
    ]


@pytest.fixture
def pipeline(mock_scryfall_client):
    """Create a CardsPipeline instance with mocked dependencies"""
    with patch('src.etl.cards_pipeline.DatabaseConnection.initialize_pool'):
        pipeline = CardsPipeline()
        return pipeline


def test_insert_cards_success(pipeline, mock_scryfall_client, mock_db_connection, sample_card_data):
    """Test successful card insertion with update_existing=True"""
    oracle_data = {
        'data': [sample_card_data]
    }
    
    rulings_data = {
        'data': []
    }
    
    transformed_card = {
        'card_id': 'card_123',
        'name': 'Lightning Bolt',
        'set': 'M21',
        'collector_num': '161',
        'oracle_text': 'Deal 3 damage',
        'rulings': '',
        'type_line': 'Instant',
        'mana_cost': '{R}',
        'cmc': 1,
        'color_identity': ['R'],
        'scryfall_uri': 'https://scryfall.com/card/m21/161'
    }
    
    mock_scryfall_client.download_oracle_cards.return_value = oracle_data
    mock_scryfall_client.download_rulings.return_value = rulings_data
    mock_scryfall_client.join_cards_with_rulings.return_value = [sample_card_data]
    mock_scryfall_client.transform_card_to_db_row.return_value = transformed_card
    
    with patch('src.etl.cards_pipeline.DatabaseConnection.transaction') as mock_transaction, \
         patch('src.etl.cards_pipeline.execute_batch') as mock_batch:
        
        mock_transaction.return_value.__enter__.return_value = mock_db_connection
        mock_transaction.return_value.__exit__.return_value = None
        
        result = pipeline.insert_cards(update_existing=True)
        
        assert result['cards_loaded'] == 1
        assert result['cards_processed'] == 1
        assert result['errors'] == 0
        mock_batch.assert_called_once()


def test_insert_cards_with_rulings(pipeline, mock_scryfall_client, mock_db_connection, 
                                   sample_card_data, sample_rulings_data):
    """Test card insertion with rulings"""
    oracle_data = {
        'data': [sample_card_data]
    }
    
    rulings_data = {
        'data': sample_rulings_data
    }
    
    card_with_rulings = sample_card_data.copy()
    card_with_rulings['rulings'] = ['This is a ruling', 'Another ruling']
    
    transformed_card = {
        'card_id': 'card_123',
        'name': 'Lightning Bolt',
        'rulings': 'This is a ruling, Another ruling',
        **{k: v for k, v in sample_card_data.items() if k != 'id' and k != 'oracle_id'}
    }
    
    mock_scryfall_client.download_oracle_cards.return_value = oracle_data
    mock_scryfall_client.download_rulings.return_value = rulings_data
    mock_scryfall_client.join_cards_with_rulings.return_value = [card_with_rulings]
    mock_scryfall_client.transform_card_to_db_row.return_value = transformed_card
    
    with patch('src.etl.cards_pipeline.DatabaseConnection.transaction') as mock_transaction, \
         patch('src.etl.cards_pipeline.execute_batch') as mock_batch:
        
        mock_transaction.return_value.__enter__.return_value = mock_db_connection
        mock_transaction.return_value.__exit__.return_value = None
        
        result = pipeline.insert_cards(update_existing=True)
        
        assert result['cards_loaded'] == 1
        mock_scryfall_client.join_cards_with_rulings.assert_called_once()


def test_insert_cards_handles_missing_oracle_data(pipeline, mock_scryfall_client):
    """Test that insert_cards handles missing oracle cards data"""
    mock_scryfall_client.download_oracle_cards.return_value = None
    
    result = pipeline.insert_cards()
    
    assert result['cards_loaded'] == 0
    assert result['cards_processed'] == 0
    assert result['errors'] == 1


def test_insert_cards_handles_missing_data_key(pipeline, mock_scryfall_client):
    """Test that insert_cards handles oracle data without 'data' key"""
    mock_scryfall_client.download_oracle_cards.return_value = {}
    
    result = pipeline.insert_cards()
    
    assert result['cards_loaded'] == 0
    assert result['cards_processed'] == 0
    assert result['errors'] == 1


def test_insert_cards_handles_missing_rulings(pipeline, mock_scryfall_client, mock_db_connection, 
                                               sample_card_data):
    """Test that insert_cards continues when rulings data is missing"""
    oracle_data = {
        'data': [sample_card_data]
    }
    
    transformed_card = {
        'card_id': 'card_123',
        'name': 'Lightning Bolt',
        'rulings': '',
        **{k: v for k, v in sample_card_data.items() if k != 'id' and k != 'oracle_id'}
    }
    
    mock_scryfall_client.download_oracle_cards.return_value = oracle_data
    mock_scryfall_client.download_rulings.return_value = None
    mock_scryfall_client.join_cards_with_rulings.return_value = [sample_card_data]
    mock_scryfall_client.transform_card_to_db_row.return_value = transformed_card
    
    with patch('src.etl.cards_pipeline.DatabaseConnection.transaction') as mock_transaction, \
         patch('src.etl.cards_pipeline.execute_batch') as mock_batch:
        
        mock_transaction.return_value.__enter__.return_value = mock_db_connection
        mock_transaction.return_value.__exit__.return_value = None
        
        result = pipeline.insert_cards()
        
        assert result['cards_loaded'] == 1
        assert result['errors'] == 0


def test_insert_cards_handles_transformation_errors(pipeline, mock_scryfall_client, mock_db_connection, 
                                                    sample_card_data):
    """Test that insert_cards handles card transformation errors"""
    oracle_data = {
        'data': [sample_card_data, sample_card_data.copy()]
    }
    
    transformed_card = {
        'card_id': 'card_123',
        'name': 'Lightning Bolt'
    }
    
    mock_scryfall_client.download_oracle_cards.return_value = oracle_data
    mock_scryfall_client.download_rulings.return_value = {'data': []}
    mock_scryfall_client.join_cards_with_rulings.return_value = [sample_card_data, sample_card_data]
    mock_scryfall_client.transform_card_to_db_row.side_effect = [
        transformed_card,
        Exception("Transformation error")
    ]
    
    with patch('src.etl.cards_pipeline.DatabaseConnection.transaction') as mock_transaction, \
         patch('src.etl.cards_pipeline.execute_batch') as mock_batch:
        
        mock_transaction.return_value.__enter__.return_value = mock_db_connection
        mock_transaction.return_value.__exit__.return_value = None
        
        result = pipeline.insert_cards()
        
        # Should process 1 card successfully, skip the one with error
        assert result['cards_loaded'] == 1
        assert result['cards_processed'] == 1


def test_insert_cards_batch_processing(pipeline, mock_scryfall_client, mock_db_connection, sample_card_data):
    """Test that insert_cards processes cards in batches"""
    # Create 5 cards
    cards = [sample_card_data.copy() for _ in range(5)]
    for i, card in enumerate(cards):
        card['id'] = f'card_{i}'
        card['name'] = f'Card {i}'
    
    oracle_data = {'data': cards}
    rulings_data = {'data': []}
    
    transformed_cards = [
        {
            'card_id': f'card_{i}',
            'name': f'Card {i}',
            'rulings': '',
            **{k: v for k, v in sample_card_data.items() if k not in ['id', 'name', 'oracle_id']}
        }
        for i in range(5)
    ]
    
    mock_scryfall_client.download_oracle_cards.return_value = oracle_data
    mock_scryfall_client.download_rulings.return_value = rulings_data
    mock_scryfall_client.join_cards_with_rulings.return_value = cards
    mock_scryfall_client.transform_card_to_db_row.side_effect = transformed_cards
    
    with patch('src.etl.cards_pipeline.DatabaseConnection.transaction') as mock_transaction, \
         patch('src.etl.cards_pipeline.execute_batch') as mock_batch:
        
        mock_transaction.return_value.__enter__.return_value = mock_db_connection
        mock_transaction.return_value.__exit__.return_value = None
        
        # Use batch_size of 2 to test batching
        result = pipeline.insert_cards(batch_size=2)
        
        assert result['cards_loaded'] == 5
        # Should be called 3 times (2+2+1)
        assert mock_batch.call_count == 3


def test_insert_cards_handles_batch_insertion_errors(pipeline, mock_scryfall_client, mock_db_connection, 
                                                      sample_card_data):
    """Test that insert_cards handles batch insertion errors"""
    oracle_data = {
        'data': [sample_card_data, sample_card_data.copy()]
    }
    
    transformed_card = {
        'card_id': 'card_123',
        'name': 'Lightning Bolt',
        'rulings': ''
    }
    
    mock_scryfall_client.download_oracle_cards.return_value = oracle_data
    mock_scryfall_client.download_rulings.return_value = {'data': []}
    mock_scryfall_client.join_cards_with_rulings.return_value = [sample_card_data, sample_card_data]
    mock_scryfall_client.transform_card_to_db_row.return_value = transformed_card
    
    with patch('src.etl.cards_pipeline.DatabaseConnection.transaction') as mock_transaction, \
         patch('src.etl.cards_pipeline.execute_batch') as mock_batch:
        
        mock_batch.side_effect = [
            Exception("Database error"),
            None  # Second batch succeeds
        ]
        
        mock_transaction.return_value.__enter__.return_value = mock_db_connection
        mock_transaction.return_value.__exit__.return_value = None
        
        result = pipeline.insert_cards(batch_size=1)
        
        # First batch fails, second succeeds
        assert result['cards_loaded'] == 1
        assert result['errors'] == 1


def test_insert_cards_update_existing_false(pipeline, mock_scryfall_client, mock_db_connection, sample_card_data):
    """Test insert_cards with update_existing=False uses DO NOTHING"""
    oracle_data = {'data': [sample_card_data]}
    rulings_data = {'data': []}
    
    transformed_card = {
        'card_id': 'card_123',
        'name': 'Lightning Bolt',
        'rulings': ''
    }
    
    mock_scryfall_client.download_oracle_cards.return_value = oracle_data
    mock_scryfall_client.download_rulings.return_value = rulings_data
    mock_scryfall_client.join_cards_with_rulings.return_value = [sample_card_data]
    mock_scryfall_client.transform_card_to_db_row.return_value = transformed_card
    
    with patch('src.etl.cards_pipeline.DatabaseConnection.transaction') as mock_transaction, \
         patch('src.etl.cards_pipeline.execute_batch') as mock_batch:
        
        mock_transaction.return_value.__enter__.return_value = mock_db_connection
        mock_transaction.return_value.__exit__.return_value = None
        
        result = pipeline.insert_cards(update_existing=False)
        
        assert result['cards_loaded'] == 1
        # Check that execute_batch was called with DO NOTHING query
        call_args = mock_batch.call_args[0]
        assert 'DO NOTHING' in call_args[1]


def test_insert_cards_update_existing_true(pipeline, mock_scryfall_client, mock_db_connection, sample_card_data):
    """Test insert_cards with update_existing=True uses DO UPDATE"""
    oracle_data = {'data': [sample_card_data]}
    rulings_data = {'data': []}
    
    transformed_card = {
        'card_id': 'card_123',
        'name': 'Lightning Bolt',
        'rulings': ''
    }
    
    mock_scryfall_client.download_oracle_cards.return_value = oracle_data
    mock_scryfall_client.download_rulings.return_value = rulings_data
    mock_scryfall_client.join_cards_with_rulings.return_value = [sample_card_data]
    mock_scryfall_client.transform_card_to_db_row.return_value = transformed_card
    
    with patch('src.etl.cards_pipeline.DatabaseConnection.transaction') as mock_transaction, \
         patch('src.etl.cards_pipeline.execute_batch') as mock_batch:
        
        mock_transaction.return_value.__enter__.return_value = mock_db_connection
        mock_transaction.return_value.__exit__.return_value = None
        
        result = pipeline.insert_cards(update_existing=True)
        
        assert result['cards_loaded'] == 1
        # Check that execute_batch was called with DO UPDATE query
        call_args = mock_batch.call_args[0]
        assert 'DO UPDATE' in call_args[1]


def test_insert_cards_handles_transaction_failure(pipeline, mock_scryfall_client, sample_card_data):
    """Test that insert_cards handles database transaction failures"""
    oracle_data = {'data': [sample_card_data]}
    rulings_data = {'data': []}
    
    transformed_card = {
        'card_id': 'card_123',
        'name': 'Lightning Bolt',
        'rulings': ''
    }
    
    mock_scryfall_client.download_oracle_cards.return_value = oracle_data
    mock_scryfall_client.download_rulings.return_value = rulings_data
    mock_scryfall_client.join_cards_with_rulings.return_value = [sample_card_data]
    mock_scryfall_client.transform_card_to_db_row.return_value = transformed_card
    
    with patch('src.etl.cards_pipeline.DatabaseConnection.transaction') as mock_transaction:
        mock_transaction.return_value.__enter__.side_effect = Exception("Database connection error")
        
        with pytest.raises(Exception):
            pipeline.insert_cards()


def test_load_initial_success(pipeline, mock_scryfall_client, mock_db_connection, sample_card_data):
    """Test successful initial load"""
    oracle_data = {'data': [sample_card_data]}
    rulings_data = {'data': []}
    
    transformed_card = {
        'card_id': 'card_123',
        'name': 'Lightning Bolt',
        'rulings': ''
    }
    
    mock_scryfall_client.download_oracle_cards.return_value = oracle_data
    mock_scryfall_client.download_rulings.return_value = rulings_data
    mock_scryfall_client.join_cards_with_rulings.return_value = [sample_card_data]
    mock_scryfall_client.transform_card_to_db_row.return_value = transformed_card
    
    with patch('src.etl.cards_pipeline.DatabaseConnection.transaction') as mock_transaction, \
         patch('src.etl.cards_pipeline.execute_batch'), \
         patch('src.etl.cards_pipeline.update_load_metadata') as mock_update_metadata:
        
        mock_transaction.return_value.__enter__.return_value = mock_db_connection
        mock_transaction.return_value.__exit__.return_value = None
        
        result = pipeline.load_initial()
        
        assert result['success'] is True
        assert result['objects_loaded'] == 1
        assert result['objects_processed'] == 1
        assert result['errors'] == 0
        mock_update_metadata.assert_called_once()
        # Verify metadata was called with datetime object
        call_args = mock_update_metadata.call_args
        assert isinstance(call_args[1]['last_timestamp'], datetime)


def test_load_initial_updates_metadata(pipeline, mock_scryfall_client, mock_db_connection, sample_card_data):
    """Test that load_initial updates load metadata"""
    oracle_data = {'data': [sample_card_data]}
    rulings_data = {'data': []}
    
    transformed_card = {
        'card_id': 'card_123',
        'name': 'Lightning Bolt',
        'rulings': ''
    }
    
    mock_scryfall_client.download_oracle_cards.return_value = oracle_data
    mock_scryfall_client.download_rulings.return_value = rulings_data
    mock_scryfall_client.join_cards_with_rulings.return_value = [sample_card_data]
    mock_scryfall_client.transform_card_to_db_row.return_value = transformed_card
    
    with patch('src.etl.cards_pipeline.DatabaseConnection.transaction') as mock_transaction, \
         patch('src.etl.cards_pipeline.execute_batch'), \
         patch('src.etl.cards_pipeline.update_load_metadata') as mock_update_metadata:
        
        mock_transaction.return_value.__enter__.return_value = mock_db_connection
        mock_transaction.return_value.__exit__.return_value = None
        
        pipeline.load_initial()
        
        # Verify update_load_metadata was called with correct parameters
        mock_update_metadata.assert_called_once()
        call_args = mock_update_metadata.call_args
        assert call_args[1]['data_type'] == 'cards'
        assert call_args[1]['load_type'] == 'initial'
        assert isinstance(call_args[1]['last_timestamp'], datetime)


def test_load_initial_handles_no_cards_loaded(pipeline, mock_scryfall_client):
    """Test that load_initial handles case when no cards are loaded"""
    mock_scryfall_client.download_oracle_cards.return_value = None
    
    with patch('src.etl.cards_pipeline.update_load_metadata') as mock_update_metadata:
        result = pipeline.load_initial()
        
        assert result['success'] is False
        assert result['objects_loaded'] == 0
        # Should not update metadata if no cards loaded
        mock_update_metadata.assert_not_called()


def test_load_initial_uses_update_existing_true(pipeline, mock_scryfall_client, mock_db_connection, sample_card_data):
    """Test that load_initial uses update_existing=True"""
    oracle_data = {'data': [sample_card_data]}
    rulings_data = {'data': []}
    
    transformed_card = {
        'card_id': 'card_123',
        'name': 'Lightning Bolt',
        'rulings': ''
    }
    
    mock_scryfall_client.download_oracle_cards.return_value = oracle_data
    mock_scryfall_client.download_rulings.return_value = rulings_data
    mock_scryfall_client.join_cards_with_rulings.return_value = [sample_card_data]
    mock_scryfall_client.transform_card_to_db_row.return_value = transformed_card
    
    with patch('src.etl.cards_pipeline.DatabaseConnection.transaction') as mock_transaction, \
         patch('src.etl.cards_pipeline.execute_batch') as mock_batch, \
         patch('src.etl.cards_pipeline.update_load_metadata'):
        
        mock_transaction.return_value.__enter__.return_value = mock_db_connection
        mock_transaction.return_value.__exit__.return_value = None
        
        pipeline.load_initial()
        
        # Verify execute_batch was called with DO UPDATE (update_existing=True)
        call_args = mock_batch.call_args[0]
        assert 'DO UPDATE' in call_args[1]


def test_load_incremental_success(pipeline, mock_scryfall_client, mock_db_connection, sample_card_data):
    """Test successful incremental load"""
    oracle_data = {'data': [sample_card_data]}
    rulings_data = {'data': []}
    
    transformed_card = {
        'card_id': 'card_123',
        'name': 'Lightning Bolt',
        'rulings': ''
    }
    
    mock_scryfall_client.download_oracle_cards.return_value = oracle_data
    mock_scryfall_client.download_rulings.return_value = rulings_data
    mock_scryfall_client.join_cards_with_rulings.return_value = [sample_card_data]
    mock_scryfall_client.transform_card_to_db_row.return_value = transformed_card
    
    with patch('src.etl.cards_pipeline.DatabaseConnection.transaction') as mock_transaction, \
         patch('src.etl.cards_pipeline.execute_batch'), \
         patch('src.etl.cards_pipeline.get_last_load_timestamp') as mock_get_timestamp, \
         patch('src.etl.cards_pipeline.update_load_metadata') as mock_update_metadata:
        
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


def test_load_incremental_falls_back_to_initial(pipeline, mock_scryfall_client):
    """Test that incremental load falls back to initial if no previous load"""
    with patch('src.etl.cards_pipeline.get_last_load_timestamp') as mock_get_timestamp, \
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


def test_load_incremental_uses_update_existing_false(pipeline, mock_scryfall_client, mock_db_connection, 
                                                     sample_card_data):
    """Test that load_incremental uses update_existing=False"""
    oracle_data = {'data': [sample_card_data]}
    rulings_data = {'data': []}
    
    transformed_card = {
        'card_id': 'card_123',
        'name': 'Lightning Bolt',
        'rulings': ''
    }
    
    mock_scryfall_client.download_oracle_cards.return_value = oracle_data
    mock_scryfall_client.download_rulings.return_value = rulings_data
    mock_scryfall_client.join_cards_with_rulings.return_value = [sample_card_data]
    mock_scryfall_client.transform_card_to_db_row.return_value = transformed_card
    
    with patch('src.etl.cards_pipeline.DatabaseConnection.transaction') as mock_transaction, \
         patch('src.etl.cards_pipeline.execute_batch') as mock_batch, \
         patch('src.etl.cards_pipeline.get_last_load_timestamp') as mock_get_timestamp, \
         patch('src.etl.cards_pipeline.update_load_metadata'):
        
        mock_get_timestamp.return_value = datetime.fromtimestamp(1234560000)
        mock_transaction.return_value.__enter__.return_value = mock_db_connection
        mock_transaction.return_value.__exit__.return_value = None
        
        pipeline.load_incremental()
        
        # Verify execute_batch was called with DO NOTHING (update_existing=False)
        call_args = mock_batch.call_args[0]
        assert 'DO NOTHING' in call_args[1]


def test_load_incremental_updates_metadata(pipeline, mock_scryfall_client, mock_db_connection, sample_card_data):
    """Test that load_incremental updates load metadata"""
    oracle_data = {'data': [sample_card_data]}
    rulings_data = {'data': []}
    
    transformed_card = {
        'card_id': 'card_123',
        'name': 'Lightning Bolt',
        'rulings': ''
    }
    
    mock_scryfall_client.download_oracle_cards.return_value = oracle_data
    mock_scryfall_client.download_rulings.return_value = rulings_data
    mock_scryfall_client.join_cards_with_rulings.return_value = [sample_card_data]
    mock_scryfall_client.transform_card_to_db_row.return_value = transformed_card
    
    with patch('src.etl.cards_pipeline.DatabaseConnection.transaction') as mock_transaction, \
         patch('src.etl.cards_pipeline.execute_batch'), \
         patch('src.etl.cards_pipeline.get_last_load_timestamp') as mock_get_timestamp, \
         patch('src.etl.cards_pipeline.update_load_metadata') as mock_update_metadata:
        
        mock_get_timestamp.return_value = datetime.fromtimestamp(1234560000)
        mock_transaction.return_value.__enter__.return_value = mock_db_connection
        mock_transaction.return_value.__exit__.return_value = None
        
        pipeline.load_incremental()
        
        # Verify update_load_metadata was called with correct parameters
        mock_update_metadata.assert_called_once()
        call_args = mock_update_metadata.call_args
        assert call_args[1]['data_type'] == 'cards'
        assert call_args[1]['load_type'] == 'incremental'
        assert isinstance(call_args[1]['last_timestamp'], datetime)


def test_load_incremental_handles_no_cards_loaded(pipeline, mock_scryfall_client):
    """Test that load_incremental handles case when no cards are loaded"""
    mock_scryfall_client.download_oracle_cards.return_value = None
    
    with patch('src.etl.cards_pipeline.get_last_load_timestamp') as mock_get_timestamp, \
         patch('src.etl.cards_pipeline.update_load_metadata') as mock_update_metadata:
        
        mock_get_timestamp.return_value = datetime.fromtimestamp(1234560000)
        
        result = pipeline.load_incremental()
        
        assert result['success'] is False
        assert result['objects_loaded'] == 0
        # Should not update metadata if no cards loaded
        mock_update_metadata.assert_not_called()


def test_insert_cards_custom_batch_size(pipeline, mock_scryfall_client, mock_db_connection, sample_card_data):
    """Test that insert_cards respects custom batch_size parameter"""
    # Create 10 cards
    cards = [sample_card_data.copy() for _ in range(10)]
    for i, card in enumerate(cards):
        card['id'] = f'card_{i}'
        card['name'] = f'Card {i}'
    
    oracle_data = {'data': cards}
    rulings_data = {'data': []}
    
    transformed_cards = [
        {
            'card_id': f'card_{i}',
            'name': f'Card {i}',
            'rulings': '',
            **{k: v for k, v in sample_card_data.items() if k not in ['id', 'name', 'oracle_id']}
        }
        for i in range(10)
    ]
    
    mock_scryfall_client.download_oracle_cards.return_value = oracle_data
    mock_scryfall_client.download_rulings.return_value = rulings_data
    mock_scryfall_client.join_cards_with_rulings.return_value = cards
    mock_scryfall_client.transform_card_to_db_row.side_effect = transformed_cards
    
    with patch('src.etl.cards_pipeline.DatabaseConnection.transaction') as mock_transaction, \
         patch('src.etl.cards_pipeline.execute_batch') as mock_batch:
        
        mock_transaction.return_value.__enter__.return_value = mock_db_connection
        mock_transaction.return_value.__exit__.return_value = None
        
        # Use batch_size of 3
        result = pipeline.insert_cards(batch_size=3)
        
        assert result['cards_loaded'] == 10
        # Should be called 4 times (3+3+3+1)
        assert mock_batch.call_count == 4


"""Unit tests for etl utils module"""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock

from src.etl.etl_utils import get_last_load_timestamp, update_load_metadata


def test_get_last_load_timestamp_tournaments():
    """Test getting last load timestamp for tournaments"""
    mock_cursor = Mock()
    test_datetime = datetime.fromtimestamp(1234567890)
    mock_cursor.fetchone.return_value = (test_datetime,)
    
    with patch('src.etl.etl_utils.DatabaseConnection.get_cursor') as mock_get_cursor:
        mock_get_cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_cursor.return_value.__exit__.return_value = None
        
        result = get_last_load_timestamp('tournaments')
        
        assert result == test_datetime
        mock_cursor.execute.assert_called_once()
        # Verify it queries by data_type
        call_args = mock_cursor.execute.call_args
        assert 'data_type' in call_args[0][0].lower()
        assert call_args[0][1] == ('tournaments',)


def test_get_last_load_timestamp_cards():
    """Test getting last load timestamp for cards"""
    mock_cursor = Mock()
    test_datetime = datetime.fromtimestamp(1234567890)
    mock_cursor.fetchone.return_value = (test_datetime,)
    
    with patch('src.etl.etl_utils.DatabaseConnection.get_cursor') as mock_get_cursor:
        mock_get_cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_cursor.return_value.__exit__.return_value = None
        
        result = get_last_load_timestamp('cards')
        
        assert result == test_datetime
        mock_cursor.execute.assert_called_once()
        # Verify it queries by data_type
        call_args = mock_cursor.execute.call_args
        assert 'data_type' in call_args[0][0].lower()
        assert call_args[0][1] == ('cards',)


def test_get_last_load_timestamp_no_previous_load():
    """Test getting last load timestamp when no previous load exists"""
    mock_cursor = Mock()
    mock_cursor.fetchone.return_value = None
    
    with patch('src.etl.etl_utils.DatabaseConnection.get_cursor') as mock_get_cursor:
        mock_get_cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_cursor.return_value.__exit__.return_value = None
        
        result = get_last_load_timestamp('cards')
        
        assert result is None


def test_get_last_load_timestamp_database_error():
    """Test that database errors are handled gracefully"""
    with patch('src.etl.etl_utils.DatabaseConnection.get_cursor') as mock_get_cursor:
        mock_get_cursor.return_value.__enter__.side_effect = Exception("Database error")
        
        result = get_last_load_timestamp('cards')
        
        assert result is None


def test_get_last_load_timestamp_archetypes():
    """Test getting last load timestamp for archetypes"""
    mock_cursor = Mock()
    test_datetime = datetime.fromtimestamp(1234567890)
    mock_cursor.fetchone.return_value = (test_datetime,)
    
    with patch('src.etl.etl_utils.DatabaseConnection.get_cursor') as mock_get_cursor:
        mock_get_cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_cursor.return_value.__exit__.return_value = None
        
        result = get_last_load_timestamp('archetypes')
        
        assert result == test_datetime
        mock_cursor.execute.assert_called_once()
        # Verify it queries by data_type
        call_args = mock_cursor.execute.call_args
        assert 'data_type' in call_args[0][0].lower()
        assert call_args[0][1] == ('archetypes',)


def test_update_load_metadata_success():
    """Test successful update of load metadata"""
    mock_cursor = Mock()
    test_datetime = datetime.fromtimestamp(1234567890)
    
    with patch('src.etl.etl_utils.DatabaseConnection.get_cursor') as mock_get_cursor:
        mock_get_cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_cursor.return_value.__exit__.return_value = None
        
        update_load_metadata(
            last_timestamp=test_datetime,
            objects_loaded=100,
            data_type='tournaments',
            load_type='initial'
        )
        
        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args
        assert 'INSERT INTO load_metadata' in call_args[0][0]
        # Verify parameters are passed correctly: (last_load_date, objects_loaded, data_type, load_type)
        assert call_args[0][1] == (test_datetime, 100, 'tournaments', 'initial')


def test_update_load_metadata_default_load_type():
    """Test update_load_metadata with default load_type"""
    mock_cursor = Mock()
    test_datetime = datetime.fromtimestamp(1234567890)
    
    with patch('src.etl.etl_utils.DatabaseConnection.get_cursor') as mock_get_cursor:
        mock_get_cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_cursor.return_value.__exit__.return_value = None
        
        update_load_metadata(
            last_timestamp=test_datetime,
            objects_loaded=50,
            data_type='cards'
        )
        
        call_args = mock_cursor.execute.call_args
        # Verify default load_type 'incremental' is used: (last_load_date, objects_loaded, data_type, load_type)
        assert call_args[0][1] == (test_datetime, 50, 'cards', 'incremental')


def test_update_load_metadata_uses_commit():
    """Test that update_load_metadata uses commit=True"""
    mock_cursor = Mock()
    test_datetime = datetime.fromtimestamp(1234567890)
    
    with patch('src.etl.etl_utils.DatabaseConnection.get_cursor') as mock_get_cursor:
        mock_get_cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_cursor.return_value.__exit__.return_value = None
        
        update_load_metadata(
            last_timestamp=test_datetime,
            objects_loaded=100,
            data_type='tournaments'
        )
        
        # Verify commit=True is passed
        mock_get_cursor.assert_called_once_with(commit=True)


def test_update_load_metadata_database_error():
    """Test that database errors are raised"""
    test_datetime = datetime.fromtimestamp(1234567890)
    with patch('src.etl.etl_utils.DatabaseConnection.get_cursor') as mock_get_cursor:
        mock_get_cursor.return_value.__enter__.side_effect = Exception("Database error")
        
        with pytest.raises(Exception):
            update_load_metadata(
                last_timestamp=test_datetime,
                objects_loaded=100,
                data_type='tournaments'
            )

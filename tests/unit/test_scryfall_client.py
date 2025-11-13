"""Unit tests for Scryfall API client"""

import pytest
import time
import json
import os
from unittest.mock import Mock, patch, mock_open, MagicMock
import requests

from src.services.scryfall_client import ScryfallClient


class TestScryfallClientInit:
    """Tests for ScryfallClient initialization"""
    
    def test_init_creates_session(self):
        """Test that initialization creates a session"""
        client = ScryfallClient()
        assert client.session is not None
        assert isinstance(client.session, requests.Session)
    
    def test_init_sets_last_request_time(self):
        """Test that last request time is initialized"""
        client = ScryfallClient()
        assert client.last_request_time == 0.0


class TestRateLimit:
    """Tests for rate limiting functionality"""
    
    def test_rate_limit_enforces_delay(self):
        """Test that rate limiting enforces minimum delay between requests"""
        client = ScryfallClient()
        client.last_request_time = time.time()
        
        start_time = time.time()
        client._rate_limit()
        elapsed = time.time() - start_time
        
        assert elapsed >= ScryfallClient.RATE_LIMIT_DELAY * 0.9  # Allow 10% margin
    
    def test_rate_limit_updates_last_request_time(self):
        """Test that rate limit updates last request time"""
        client = ScryfallClient()
        client.last_request_time = 0
        
        client._rate_limit()
        assert client.last_request_time > 0
    
    def test_rate_limit_no_delay_when_enough_time_passed(self):
        """Test that no delay is added when enough time has passed"""
        client = ScryfallClient()
        client.last_request_time = time.time() - 1.0  # 1 second ago
        
        start_time = time.time()
        client._rate_limit()
        elapsed = time.time() - start_time
        
        assert elapsed < 0.05  # Should be very quick


class TestRequest:
    """Tests for _request method"""
    
    def test_successful_request(self):
        """Test successful HTTP request"""
        client = ScryfallClient()
        mock_response = Mock()
        mock_response.json.return_value = {'object': 'card', 'name': 'Lightning Bolt'}
        mock_response.status_code = 200
        
        with patch.object(client.session, 'request', return_value=mock_response):
            result = client._request('GET', '/cards/named')
            assert result == {'object': 'card', 'name': 'Lightning Bolt'}
    
    def test_404_returns_none(self):
        """Test that 404 errors return None"""
        client = ScryfallClient()
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError()
        
        with patch.object(client.session, 'request', return_value=mock_response):
            result = client._request('GET', '/cards/named')
            assert result is None
    
    def test_429_retries_with_backoff(self):
        """Test that 429 errors trigger retry with exponential backoff"""
        client = ScryfallClient()
        
        responses = [
            Mock(status_code=429, raise_for_status=Mock(side_effect=requests.exceptions.HTTPError())),
            Mock(status_code=200, json=Mock(return_value={'success': True}))
        ]
        
        with patch.object(client.session, 'request', side_effect=responses):
            with patch('time.sleep'):
                result = client._request('GET', '/test')
                assert result == {'success': True}
    
    def test_500_retries_with_backoff(self):
        """Test that 500 errors trigger retry"""
        client = ScryfallClient()
        
        responses = [
            Mock(status_code=500, raise_for_status=Mock(side_effect=requests.exceptions.HTTPError())),
            Mock(status_code=200, json=Mock(return_value={'success': True}))
        ]
        
        with patch.object(client.session, 'request', side_effect=responses):
            with patch('time.sleep'):
                result = client._request('GET', '/test')
                assert result == {'success': True}
    
    def test_max_retries_exceeded(self):
        """Test that max retries limit is enforced and returns None"""
        client = ScryfallClient()
        
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError()
        
        with patch.object(client.session, 'request', return_value=mock_response):
            with patch('time.sleep'):
                result = client._request('GET', '/test')
                assert result is None, "Should return None after max retries exceeded"


class TestGetBulkDataUrl:
    """Tests for get_bulk_data_url method"""
    
    def test_get_oracle_cards_url(self):
        """Test getting oracle cards bulk data URL"""
        client = ScryfallClient()
        mock_data = {
            'data': [
                {'type': 'oracle_cards', 'download_uri': 'https://example.com/oracle-cards.json'},
                {'type': 'rulings', 'download_uri': 'https://example.com/rulings.json'}
            ]
        }
        
        with patch.object(client, '_request', return_value=mock_data):
            result = client.get_bulk_data_url('oracle_cards')
            assert result == 'https://example.com/oracle-cards.json'
    
    def test_get_rulings_url(self):
        """Test getting rulings bulk data URL"""
        client = ScryfallClient()
        mock_data = {
            'data': [
                {'type': 'oracle_cards', 'download_uri': 'https://example.com/oracle-cards.json'},
                {'type': 'rulings', 'download_uri': 'https://example.com/rulings.json'}
            ]
        }
        
        with patch.object(client, '_request', return_value=mock_data):
            result = client.get_bulk_data_url('rulings')
            assert result == 'https://example.com/rulings.json'
    
    def test_data_type_not_found(self):
        """Test that None is returned when data type not found"""
        client = ScryfallClient()
        mock_data = {'data': [{'type': 'other', 'download_uri': 'https://example.com/other.json'}]}
        
        with patch.object(client, '_request', return_value=mock_data):
            result = client.get_bulk_data_url('oracle_cards')
            assert result is None
    
    def test_handles_error(self):
        """Test that errors are handled gracefully"""
        client = ScryfallClient()
        
        with patch.object(client, '_request', side_effect=Exception("API Error")):
            result = client.get_bulk_data_url('oracle_cards')
            assert result is None


class TestDownloadBulkData:
    """Tests for download_bulk_data method"""
    
    def test_download_bulk_data_with_url(self):
        """Test downloading bulk data with explicit URL"""
        client = ScryfallClient()
        
        mock_cards = [{'id': '1', 'name': 'Card 1'}, {'id': '2', 'name': 'Card 2'}]
        mock_response = Mock()
        mock_response.json.return_value = mock_cards
        mock_response.raise_for_status = Mock()
        
        with patch('requests.get', return_value=mock_response):
            with patch('builtins.open', mock_open()) as m:
                with patch('os.makedirs'):
                    result = client.download_bulk_data('oracle_cards', 'https://example.com/cards.json')
                    
                    assert result is not None
                    assert result['data'] == mock_cards
                    assert 'file_path' in result
    
    def test_download_bulk_data_fetches_url_when_not_provided(self):
        """Test that URL is fetched when not provided"""
        client = ScryfallClient()
        
        mock_cards = [{'id': '1', 'name': 'Card 1'}]
        mock_response = Mock()
        mock_response.json.return_value = mock_cards
        mock_response.raise_for_status = Mock()
        
        with patch.object(client, 'get_bulk_data_url', return_value='https://example.com/cards.json'):
            with patch('requests.get', return_value=mock_response):
                with patch('builtins.open', mock_open()):
                    with patch('os.makedirs'):
                        result = client.download_bulk_data('oracle_cards')
                        assert result is not None
    
    def test_download_bulk_data_handles_dict_with_data_key(self):
        """Test handling of dict response with 'data' key"""
        client = ScryfallClient()
        
        mock_response_data = {'data': [{'id': '1', 'name': 'Card 1'}]}
        mock_response = Mock()
        mock_response.json.return_value = mock_response_data
        mock_response.raise_for_status = Mock()
        
        with patch('requests.get', return_value=mock_response):
            with patch('builtins.open', mock_open()):
                with patch('os.makedirs'):
                    result = client.download_bulk_data('oracle_cards', 'https://example.com/cards.json')
                    
                    assert result is not None
                    assert result['data'] == [{'id': '1', 'name': 'Card 1'}]
    
    def test_download_bulk_data_returns_none_on_error(self):
        """Test that None is returned on download error"""
        client = ScryfallClient()
        
        with patch('requests.get', side_effect=Exception("Download failed")):
            result = client.download_bulk_data('oracle_cards', 'https://example.com/cards.json')
            assert result is None
    
    def test_download_bulk_data_creates_directory(self):
        """Test that data directory is created"""
        client = ScryfallClient()
        
        mock_cards = [{'id': '1'}]
        mock_response = Mock()
        mock_response.json.return_value = mock_cards
        mock_response.raise_for_status = Mock()
        
        with patch('requests.get', return_value=mock_response):
            with patch('builtins.open', mock_open()) as m:
                with patch('os.makedirs') as mock_makedirs:
                    client.download_bulk_data('oracle_cards', 'https://example.com/oracle-cards-20251105.json')
                    mock_makedirs.assert_called_once()


class TestDownloadOracleCards:
    """Tests for download_oracle_cards convenience method"""
    
    def test_download_oracle_cards_calls_download_bulk_data(self):
        """Test that download_oracle_cards calls download_bulk_data correctly"""
        client = ScryfallClient()
        
        with patch.object(client, 'download_bulk_data', return_value={'data': [], 'file_path': 'test.json'}) as mock_download:
            result = client.download_oracle_cards('https://example.com/cards.json')
            
            mock_download.assert_called_once_with('oracle_cards', 'https://example.com/cards.json')
            assert result == {'data': [], 'file_path': 'test.json'}


class TestDownloadRulings:
    """Tests for download_rulings convenience method"""
    
    def test_download_rulings_calls_download_bulk_data(self):
        """Test that download_rulings calls download_bulk_data correctly"""
        client = ScryfallClient()
        
        with patch.object(client, 'download_bulk_data', return_value={'data': [], 'file_path': 'test.json'}) as mock_download:
            result = client.download_rulings('https://example.com/rulings.json')
            
            mock_download.assert_called_once_with('rulings', 'https://example.com/rulings.json')
            assert result == {'data': [], 'file_path': 'test.json'}


class TestJoinCardsWithRulings:
    """Tests for join_cards_with_rulings method"""
    
    def test_join_adds_rulings_to_cards(self):
        """Test that rulings are correctly joined to cards"""
        client = ScryfallClient()
        
        cards = [
            {'oracle_id': 'abc123', 'name': 'Lightning Bolt'},
            {'oracle_id': 'def456', 'name': 'Counterspell'}
        ]
        
        rulings = [
            {'oracle_id': 'abc123', 'comment': 'Deals 3 damage'},
            {'oracle_id': 'abc123', 'comment': 'Cannot target planeswalkers'},
            {'oracle_id': 'def456', 'comment': 'Counters any spell'}
        ]
        
        result = client.join_cards_with_rulings(cards, rulings)
        
        assert len(result) == 2
        assert len(result[0]['rulings']) == 2
        assert 'Deals 3 damage' in result[0]['rulings']
        assert len(result[1]['rulings']) == 1
    
    def test_join_handles_cards_without_rulings(self):
        """Test that cards without rulings get empty list"""
        client = ScryfallClient()
        
        cards = [
            {'oracle_id': 'abc123', 'name': 'Lightning Bolt'},
            {'oracle_id': 'xyz999', 'name': 'New Card'}
        ]
        
        rulings = [
            {'oracle_id': 'abc123', 'comment': 'Some ruling'}
        ]
        
        result = client.join_cards_with_rulings(cards, rulings)
        
        assert len(result[0]['rulings']) == 1
        assert len(result[1]['rulings']) == 0
    
    def test_join_handles_empty_inputs(self):
        """Test handling of empty inputs"""
        client = ScryfallClient()
        
        result = client.join_cards_with_rulings([], [])
        assert result == []
    
    def test_join_preserves_card_data(self):
        """Test that original card data is preserved"""
        client = ScryfallClient()
        
        cards = [
            {'oracle_id': 'abc123', 'name': 'Lightning Bolt', 'cmc': 1, 'colors': ['R']}
        ]
        rulings = []
        
        result = client.join_cards_with_rulings(cards, rulings)
        
        assert result[0]['name'] == 'Lightning Bolt'
        assert result[0]['cmc'] == 1
        assert result[0]['colors'] == ['R']


class TestGetCardPrice:
    """Tests for get_card_price method"""
    
    def test_get_card_price_success(self):
        """Test successful price retrieval"""
        client = ScryfallClient()
        
        mock_data = {
            'name': 'Lightning Bolt',
            'prices': {'usd': '0.50', 'usd_foil': '2.00'}
        }
        
        with patch.object(client, '_request', return_value=mock_data):
            result = client.get_card_price('Lightning Bolt')
            assert result == 0.50
    
    def test_get_card_price_returns_foil_when_usd_unavailable(self):
        """Test that foil price is returned when regular price unavailable"""
        client = ScryfallClient()
        
        mock_data = {
            'name': 'Foil Only Card',
            'prices': {'usd': None, 'usd_foil': '5.00'}
        }
        
        with patch.object(client, '_request', return_value=mock_data):
            result = client.get_card_price('Foil Only Card')
            assert result == 5.00
    
    def test_get_card_price_returns_none_when_no_price(self):
        """Test that None is returned when no price available"""
        client = ScryfallClient()
        
        mock_data = {
            'name': 'No Price Card',
            'prices': {'usd': None, 'usd_foil': None}
        }
        
        with patch.object(client, '_request', return_value=mock_data):
            result = client.get_card_price('No Price Card')
            assert result is None
    
    def test_get_card_price_handles_card_not_found(self):
        """Test handling of card not found"""
        client = ScryfallClient()
        
        with patch.object(client, '_request', return_value=None):
            result = client.get_card_price('Nonexistent Card')
            assert result is None
    
    def test_get_card_price_handles_error(self):
        """Test error handling"""
        client = ScryfallClient()
        
        with patch.object(client, '_request', side_effect=Exception("API Error")):
            result = client.get_card_price('Lightning Bolt')
            assert result is None


class TestGetCardByName:
    """Tests for get_card_by_name method"""
    
    def test_get_card_by_name_success(self):
        """Test successful card retrieval"""
        client = ScryfallClient()
        
        mock_data = {
            'name': 'Lightning Bolt',
            'mana_cost': '{R}',
            'type_line': 'Instant'
        }
        
        with patch.object(client, '_request', return_value=mock_data):
            result = client.get_card_by_name('Lightning Bolt')
            assert result == mock_data
    
    def test_get_card_by_name_not_found(self):
        """Test card not found returns None"""
        client = ScryfallClient()
        
        with patch.object(client, '_request', return_value=None):
            result = client.get_card_by_name('Nonexistent Card')
            assert result is None
    
    def test_get_card_by_name_handles_error(self):
        """Test error handling"""
        client = ScryfallClient()
        
        with patch.object(client, '_request', side_effect=Exception("API Error")):
            result = client.get_card_by_name('Lightning Bolt')
            assert result is None


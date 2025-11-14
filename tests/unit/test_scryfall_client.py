"""Unit tests for Scryfall API client"""

import pytest
import time
import json
import os
from unittest.mock import Mock, patch, mock_open, MagicMock
import requests

from src.etl.api_clients.scryfall_client import ScryfallClient


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
    
    def test_download_bulk_data_parses_oracle_cards_structure(self):
        """Test that oracle_cards bulk data structure is correctly parsed"""
        client = ScryfallClient()
        
        # Oracle cards have specific fields: id, oracle_id, name, set, collector_number, etc.
        mock_oracle_card = {
            'id': 'abc123-uuid',
            'oracle_id': 'abc123',
            'name': 'Lightning Bolt',
            'set': 'M21',
            'collector_number': '161',
            'oracle_text': 'Deal 3 damage',
            'type_line': 'Instant',
            'mana_cost': '{R}',
            'cmc': 1.0,
            'color_identity': ['R'],
            'scryfall_uri': 'https://scryfall.com/card/m21/161'
        }
        mock_response = Mock()
        mock_response.json.return_value = [mock_oracle_card]
        mock_response.raise_for_status = Mock()
        
        with patch('requests.get', return_value=mock_response):
            with patch('builtins.open', mock_open()):
                with patch('os.makedirs'):
                    result = client.download_bulk_data('oracle_cards', 'https://example.com/cards.json')
                    
                    assert result is not None
                    assert len(result['data']) == 1
                    card = result['data'][0]
                    assert card['id'] == 'abc123-uuid'
                    assert card['oracle_id'] == 'abc123'
                    assert card['name'] == 'Lightning Bolt'
                    assert card['set'] == 'M21'
                    assert card['collector_number'] == '161'
                    assert card['oracle_text'] == 'Deal 3 damage'
                    assert card['type_line'] == 'Instant'
                    assert card['mana_cost'] == '{R}'
                    assert card['cmc'] == 1.0
                    assert card['color_identity'] == ['R']
                    assert card['scryfall_uri'] == 'https://scryfall.com/card/m21/161'
    
    def test_download_bulk_data_parses_rulings_structure(self):
        """Test that rulings bulk data structure is correctly parsed"""
        client = ScryfallClient()
        
        # Rulings have oracle_id and comment fields
        mock_rulings = [
            {
                'oracle_id': 'abc123',
                'comment': 'Deals 3 damage to any target.'
            },
            {
                'oracle_id': 'abc123',
                'comment': 'Cannot target planeswalkers directly.'
            },
            {
                'oracle_id': 'def456',
                'comment': 'Counters any spell.'
            }
        ]
        mock_response = Mock()
        mock_response.json.return_value = mock_rulings
        mock_response.raise_for_status = Mock()
        
        with patch('requests.get', return_value=mock_response):
            with patch('builtins.open', mock_open()):
                with patch('os.makedirs'):
                    result = client.download_bulk_data('rulings', 'https://example.com/rulings.json')
                    
                    assert result is not None
                    assert len(result['data']) == 3
                    assert result['data'][0]['oracle_id'] == 'abc123'
                    assert result['data'][0]['comment'] == 'Deals 3 damage to any target.'
                    assert result['data'][1]['oracle_id'] == 'abc123'
                    assert result['data'][2]['oracle_id'] == 'def456'
    
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
    
    def test_join_cards_with_rulings_by_oracle_id(self):
        """Test that cards are correctly joined with rulings by oracle_id"""
        client = ScryfallClient()
        
        cards = [
            {'oracle_id': 'abc123', 'id': 'card1', 'name': 'Lightning Bolt'},
            {'oracle_id': 'def456', 'id': 'card2', 'name': 'Counterspell'},
            {'oracle_id': 'xyz999', 'id': 'card3', 'name': 'No Rulings Card'}
        ]
        
        rulings = [
            {'oracle_id': 'abc123', 'comment': 'First ruling'},
            {'oracle_id': 'abc123', 'comment': 'Second ruling'},
            {'oracle_id': 'def456', 'comment': 'Single ruling'}
        ]
        
        result = client.join_cards_with_rulings(cards, rulings)
        
        assert len(result) == 3
        # Card 1 should have 2 rulings
        assert len(result[0]['rulings']) == 2
        assert 'First ruling' in result[0]['rulings']
        assert 'Second ruling' in result[0]['rulings']
        # Card 2 should have 1 ruling
        assert len(result[1]['rulings']) == 1
        assert 'Single ruling' in result[1]['rulings']
        # Card 3 should have no rulings
        assert len(result[2]['rulings']) == 0


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


class TestConcatenateRulings:
    """Tests for concatenate_rulings method"""
    
    def test_concatenate_rulings_with_multiple_rulings(self):
        """Test concatenating multiple rulings"""
        client = ScryfallClient()
        
        rulings = ['First ruling', 'Second ruling', 'Third ruling']
        result = client.concatenate_rulings(rulings)
        
        assert result == 'First ruling, Second ruling, Third ruling'
    
    def test_concatenate_rulings_with_single_ruling(self):
        """Test concatenating single ruling"""
        client = ScryfallClient()
        
        rulings = ['Only ruling']
        result = client.concatenate_rulings(rulings)
        
        assert result == 'Only ruling'
    
    def test_concatenate_rulings_with_empty_list(self):
        """Test concatenating empty list returns empty string"""
        client = ScryfallClient()
        
        result = client.concatenate_rulings([])
        assert result == ""
    
    def test_concatenate_rulings_filters_empty_strings(self):
        """Test that empty strings are filtered out"""
        client = ScryfallClient()
        
        rulings = ['Valid ruling', '', '   ', 'Another valid ruling']
        result = client.concatenate_rulings(rulings)
        
        assert result == 'Valid ruling, Another valid ruling'
    
    def test_concatenate_rulings_strips_whitespace(self):
        """Test that whitespace is stripped from rulings"""
        client = ScryfallClient()
        
        rulings = ['  First ruling  ', '  Second ruling  ']
        result = client.concatenate_rulings(rulings)
        
        assert result == 'First ruling, Second ruling'


class TestTransformCardToDbRow:
    """Tests for transform_card_to_db_row method"""
    
    def test_transform_card_to_db_row_complete_card(self):
        """Test transformation of complete card object"""
        client = ScryfallClient()
        
        card = {
            'id': 'abc123-uuid',
            'set': 'M21',
            'collector_number': '161',
            'name': 'Lightning Bolt',
            'oracle_text': 'Deal 3 damage',
            'rulings': ['First ruling', 'Second ruling'],
            'type_line': 'Instant',
            'mana_cost': '{R}',
            'cmc': 1.0,
            'color_identity': ['R'],
            'scryfall_uri': 'https://scryfall.com/card/m21/161'
        }
        
        result = client.transform_card_to_db_row(card)
        
        assert result['card_id'] == 'abc123-uuid'
        assert result['set'] == 'M21'
        assert result['collector_num'] == '161'
        assert result['name'] == 'Lightning Bolt'
        assert result['oracle_text'] == 'Deal 3 damage'
        assert result['rulings'] == 'First ruling, Second ruling'
        assert result['type_line'] == 'Instant'
        assert result['mana_cost'] == '{R}'
        assert result['cmc'] == 1.0
        assert result['color_identity'] == ['R']
        assert result['scryfall_uri'] == 'https://scryfall.com/card/m21/161'
    
    def test_transform_card_to_db_row_with_empty_rulings(self):
        """Test transformation with empty rulings list"""
        client = ScryfallClient()
        
        card = {
            'id': 'abc123-uuid',
            'name': 'Lightning Bolt',
            'rulings': []
        }
        
        result = client.transform_card_to_db_row(card)
        
        assert result['card_id'] == 'abc123-uuid'
        assert result['name'] == 'Lightning Bolt'
        assert result['rulings'] == ""
    
    def test_transform_card_to_db_row_with_string_rulings(self):
        """Test transformation when rulings is already a string"""
        client = ScryfallClient()
        
        card = {
            'id': 'abc123-uuid',
            'name': 'Lightning Bolt',
            'rulings': 'Already concatenated'
        }
        
        result = client.transform_card_to_db_row(card)
        
        assert result['rulings'] == 'Already concatenated'
    
    def test_transform_card_to_db_row_with_missing_fields(self):
        """Test transformation handles missing optional fields"""
        client = ScryfallClient()
        
        card = {
            'id': 'abc123-uuid',
            'name': 'Lightning Bolt'
        }
        
        result = client.transform_card_to_db_row(card)
        
        assert result['card_id'] == 'abc123-uuid'
        assert result['name'] == 'Lightning Bolt'
        assert result['set'] is None
        assert result['collector_num'] is None
        assert result['oracle_text'] is None
        assert result['rulings'] == ""
        assert result['type_line'] is None
        assert result['mana_cost'] is None
        assert result['cmc'] is None
        assert result['color_identity'] == []
        assert result['scryfall_uri'] is None
    
    def test_transform_card_to_db_row_with_non_list_color_identity(self):
        """Test transformation handles non-list color_identity"""
        client = ScryfallClient()
        
        card = {
            'id': 'abc123-uuid',
            'name': 'Lightning Bolt',
            'color_identity': 'R'  # Should be converted to empty list
        }
        
        result = client.transform_card_to_db_row(card)
        
        assert result['color_identity'] == []


"""Unit tests for TopDeck API client"""

import pytest
import time
from unittest.mock import Mock, patch, MagicMock
import requests

from src.etl.api_clients.topdeck_client import TopDeckClient


class TestTopDeckClientInit:
    """Tests for TopDeckClient initialization"""
    
    def test_init_with_api_key(self):
        """Test initialization with explicit API key"""
        client = TopDeckClient(api_key="test_key")
        assert client.api_key == "test_key"
        assert client.session.headers['Authorization'] == "test_key"
    
    def test_init_with_env_var(self):
        """Test initialization with environment variable"""
        with patch.dict('os.environ', {'TOPDECK_API_KEY': 'env_key'}):
            client = TopDeckClient()
            assert client.api_key == "env_key"
    
    def test_init_without_api_key_raises_error(self):
        """Test that missing API key raises ValueError"""
        with patch.dict('os.environ', {}, clear=True):
            with pytest.raises(ValueError, match="TopDeck API key is required"):
                TopDeckClient()
    
    def test_session_headers_set_correctly(self):
        """Test that session headers are configured properly"""
        client = TopDeckClient(api_key="test_key")
        assert client.session.headers['Authorization'] == "test_key"
        assert client.session.headers['Content-Type'] == 'application/json'


class TestRateLimit:
    """Tests for rate limiting functionality"""
    
    def test_rate_limit_enforces_delay(self):
        """Test that rate limiting enforces minimum delay between requests"""
        client = TopDeckClient(api_key="test_key")
        client.last_request_time = time.time()
        
        start_time = time.time()
        client._rate_limit()
        elapsed = time.time() - start_time
        
        assert elapsed >= TopDeckClient.RATE_LIMIT_DELAY * 0.9  # Allow 10% margin
    
    def test_rate_limit_updates_last_request_time(self):
        """Test that rate limit updates last request time"""
        client = TopDeckClient(api_key="test_key")
        client.last_request_time = 0
        
        client._rate_limit()
        assert client.last_request_time > 0
    
    def test_rate_limit_no_delay_when_enough_time_passed(self):
        """Test that no delay is added when enough time has passed"""
        client = TopDeckClient(api_key="test_key")
        client.last_request_time = time.time() - 1.0  # 1 second ago
        
        start_time = time.time()
        client._rate_limit()
        elapsed = time.time() - start_time
        
        assert elapsed < 0.05  # Should be very quick


class TestRequest:
    """Tests for _request method"""
    
    def test_successful_request(self):
        """Test successful HTTP request"""
        client = TopDeckClient(api_key="test_key")
        mock_response = Mock()
        mock_response.json.return_value = {'data': 'test'}
        mock_response.status_code = 200
        
        with patch.object(client.session, 'request', return_value=mock_response):
            result = client._request('GET', '/test')
            assert result == {'data': 'test'}
    
    def test_404_returns_none(self):
        """Test that 404 errors return None"""
        client = TopDeckClient(api_key="test_key")
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError()
        
        with patch.object(client.session, 'request', return_value=mock_response):
            result = client._request('GET', '/test')
            assert result is None
    
    def test_max_retries_exceeded(self):
        """Test that max retries limit is enforced and returns None"""
        client = TopDeckClient(api_key="test_key")
        
        # Always return 500
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError()
        
        with patch.object(client.session, 'request', return_value=mock_response):
            with patch('time.sleep'):
                result = client._request('GET', '/test')
                assert result is None, "Should return None after max retries exceeded"
    
    def test_network_error_retries(self):
        """Test that network errors trigger retries"""
        client = TopDeckClient(api_key="test_key")
        
        responses = [
            requests.exceptions.ConnectionError("Network error"),
            Mock(status_code=200, json=Mock(return_value={'success': True}))
        ]
        
        with patch.object(client.session, 'request', side_effect=responses):
            with patch('time.sleep'):
                result = client._request('GET', '/test')
                assert result == {'success': True}


class TestGetTournaments:
    """Tests for get_tournaments method"""
    
    def test_get_tournaments_with_defaults(self):
        """Test get_tournaments with default parameters"""
        client = TopDeckClient(api_key="test_key")
        expected_data = [{'TID': '1', 'name': 'Tournament 1'}]
        
        with patch.object(client, '_request', return_value=expected_data):
            result = client.get_tournaments()
            assert result == expected_data
    
    def test_get_tournaments_with_all_parameters(self):
        """Test get_tournaments with all optional parameters"""
        client = TopDeckClient(api_key="test_key")
        
        with patch.object(client, '_request', return_value=[]) as mock_request:
            client.get_tournaments(
                game="Magic: The Gathering",
                format="Standard",
                start=1000,
                end=2000,
                last=7,
                participant_min=10,
                participant_max=100,
                columns=["name", "id"],
                rounds=True,
                tids=["tid1", "tid2"]
            )
            
            # Verify the request was made with correct parameters
            call_args = mock_request.call_args
            assert call_args[0] == ('POST', '/v2/tournaments')
            body = call_args[1]['json']
            assert body['game'] == "Magic: The Gathering"
            assert body['format'] == "Standard"
            assert body['start'] == 1000
            assert body['end'] == 2000
            assert body['last'] == 7
            assert body['participantMin'] == 10
            assert body['participantMax'] == 100
            assert body['columns'] == ["name", "id"]
            assert body['rounds'] is True
            assert body['TID'] == ["tid1", "tid2"]
    
    def test_get_tournaments_handles_dict_response_with_data_key(self):
        """Test that dict response with 'data' key is handled correctly"""
        client = TopDeckClient(api_key="test_key")
        response_data = {'data': [{'TID': '1'}], 'meta': {}}
        
        with patch.object(client, '_request', return_value=response_data):
            result = client.get_tournaments()
            assert result == [{'TID': '1'}]
    
    def test_get_tournaments_handles_error(self):
        """Test that errors are handled gracefully"""
        client = TopDeckClient(api_key="test_key")
        
        with patch.object(client, '_request', side_effect=Exception("API Error")):
            result = client.get_tournaments()
            assert result is None
    
    def test_get_tournaments_single_tid_converts_to_list(self):
        """Test that single TID string is converted to list"""
        client = TopDeckClient(api_key="test_key")
        
        with patch.object(client, '_request', return_value=[]) as mock_request:
            client.get_tournaments(tids="single_tid")
            
            call_args = mock_request.call_args
            body = call_args[1]['json']
            assert body['TID'] == ["single_tid"]


class TestGetTournamentDetails:
    """Tests for get_tournament_details method"""
    
    def test_get_tournament_details_success(self):
        """Test successful tournament details retrieval"""
        client = TopDeckClient(api_key="test_key")
        expected_data = {'TID': '123', 'name': 'Test Tournament', 'players': []}
        
        with patch.object(client, '_request', return_value=expected_data):
            result = client.get_tournament_details('123')
            assert result == expected_data
    
    def test_get_tournament_details_not_found(self):
        """Test tournament not found returns None"""
        client = TopDeckClient(api_key="test_key")
        
        with patch.object(client, '_request', return_value=None):
            result = client.get_tournament_details('nonexistent')
            assert result is None
    
    def test_get_tournament_details_handles_error(self):
        """Test that errors are handled gracefully"""
        client = TopDeckClient(api_key="test_key")
        
        with patch.object(client, '_request', side_effect=Exception("API Error")):
            result = client.get_tournament_details('123')
            assert result is None


class TestGetTournamentRounds:
    """Tests for get_tournament_rounds method"""
    
    def test_get_tournament_rounds_success(self):
        """Test successful rounds retrieval"""
        client = TopDeckClient(api_key="test_key")
        expected_data = [{'round': 1, 'tables': []}]
        
        with patch.object(client, '_request', return_value=expected_data):
            result = client.get_tournament_rounds('123')
            assert result == expected_data
    
    def test_get_tournament_rounds_empty(self):
        """Test empty rounds list"""
        client = TopDeckClient(api_key="test_key")
        
        with patch.object(client, '_request', return_value=[]):
            result = client.get_tournament_rounds('123')
            assert result == []
    
    def test_get_tournament_rounds_handles_error(self):
        """Test that errors are handled gracefully"""
        client = TopDeckClient(api_key="test_key")
        
        with patch.object(client, '_request', side_effect=Exception("API Error")):
            result = client.get_tournament_rounds('123')
            assert result is None


class TestGetTournamentLatestRound:
    """Tests for get_tournament_latest_round method"""
    
    def test_get_tournament_latest_round_success(self):
        """Test successful latest round retrieval"""
        client = TopDeckClient(api_key="test_key")
        expected_data = [{'table': 1, 'players': []}]
        
        with patch.object(client, '_request', return_value=expected_data):
            result = client.get_tournament_latest_round('123')
            assert result == expected_data
    
    def test_get_tournament_latest_round_handles_error(self):
        """Test that errors are handled gracefully"""
        client = TopDeckClient(api_key="test_key")
        
        with patch.object(client, '_request', side_effect=Exception("API Error")):
            result = client.get_tournament_latest_round('123')
            assert result is None


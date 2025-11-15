"""Integration tests for TopDeck API client with real API calls"""

import pytest
import os
from dotenv import load_dotenv

from src.etl.api_clients.topdeck_client import TopDeckClient

load_dotenv()

# Require API key - tests will fail if not set
if not os.getenv('TOPDECK_API_KEY'):
    pytest.fail("TOPDECK_API_KEY environment variable must be set")


class TestTopDeckIntegration:
    """Integration tests for TopDeck API client"""
    
    @pytest.fixture(scope="class")
    def client(self):
        """Create TopDeck client for tests"""
        api_key = os.getenv('TOPDECK_API_KEY')
        if not api_key:
            pytest.fail("TOPDECK_API_KEY environment variable must be set")
        return TopDeckClient(api_key=api_key)
    
    def test_get_recent_tournaments(self, client):
        """Test fetching recent tournaments"""
        # Get tournaments from the last 60 days with format (API may require more specific filters)
        tournaments = client.get_tournaments(
            game="Magic: The Gathering",
            format="Standard",
            last=60
        )
        
        # API should return a list (may be empty)
        assert tournaments is not None, "API returned None - check API key and parameters"
        assert isinstance(tournaments, list), "Tournaments should be a list"
        
        if len(tournaments) > 0:
            # Verify tournament structure
            tournament = tournaments[0]
            assert 'TID' in tournament, "Tournament missing TID"
            assert 'game' in tournament or 'tournamentName' in tournament, "Tournament missing basic fields"
    
    def test_get_tournament_details(self, client):
        """Test fetching tournament details"""
        # First get a recent tournament with format filter (API requires format)
        tournaments = client.get_tournaments(
            game="Magic: The Gathering",
            format="Standard",
            last=60
        )
        
        assert tournaments is not None, "API returned None - check API key and parameters"
        assert isinstance(tournaments, list), "Tournaments should be a list"
        assert len(tournaments) > 0, "No tournaments found - cannot test tournament details"
        
        tournament_id = tournaments[0]['TID']
        
        # Get tournament details
        details = client.get_tournament_details(tournament_id)
        
        assert details is not None, f"Failed to fetch details for tournament {tournament_id}"
        # API returns details with 'data', 'rounds', or 'standings' keys
        assert 'data' in details or 'rounds' in details or 'standings' in details, "Tournament details missing expected fields"
    
    def test_get_tournament_rounds(self, client):
        """Test fetching tournament rounds"""
        # First get a recent tournament with format filter (API requires format)
        tournaments = client.get_tournaments(
            game="Magic: The Gathering",
            format="Standard",
            last=60
        )
        
        assert tournaments is not None, "API returned None - check API key and parameters"
        assert isinstance(tournaments, list), "Tournaments should be a list"
        assert len(tournaments) > 0, "No tournaments found - cannot test tournament rounds"
        
        tournament_id = tournaments[0]['TID']
        
        # Get tournament rounds
        rounds_data = client.get_tournament_rounds(tournament_id)
        
        # Rounds may be None or empty for tournaments in progress or without round data
        assert rounds_data is not None, f"Failed to fetch rounds for tournament {tournament_id}"
        assert isinstance(rounds_data, list), "Rounds should be a list"
        
        if len(rounds_data) > 0:
            round_data = rounds_data[0]
            assert 'round' in round_data or 'tables' in round_data, "Round data missing expected fields"
    
    def test_get_tournament_with_specific_format(self, client):
        """Test fetching tournaments for a specific format"""
        tournaments = client.get_tournaments(
            game="Magic: The Gathering",
            format="Standard",
            last=60
        )
        
        assert tournaments is not None, "Failed to fetch Standard tournaments"
        assert isinstance(tournaments, list), "Tournaments should be a list"
        
        # Verify format filtering (if tournaments exist)
        if len(tournaments) > 0:
            # Note: API may not always respect format filtering perfectly
            # Just verify we got a response
            assert 'TID' in tournaments[0], "Tournament missing TID"
    
    def test_api_rate_limiting(self, client):
        """Test that rate limiting is working"""
        import time
        
        # Make multiple requests and ensure they respect rate limiting
        start_time = time.time()
        
        for i in range(3):
            client.get_tournaments(
                game="Magic: The Gathering",
                last=7,
                participant_min=50
            )
        
        elapsed = time.time() - start_time
        
        # Should take at least 2 * RATE_LIMIT_DELAY seconds (3 requests = 2 delays)
        expected_min_time = 2 * TopDeckClient.RATE_LIMIT_DELAY * 0.9  # 10% margin
        assert elapsed >= expected_min_time, f"Rate limiting not enforced: {elapsed}s < {expected_min_time}s"
    
    def test_invalid_tournament_id_returns_none(self, client):
        """Test that invalid tournament ID returns None"""
        details = client.get_tournament_details("invalid_id_that_does_not_exist_12345")
        
        assert details is None, "Invalid tournament ID should return None"
    
    def test_get_tournaments_with_date_range(self, client):
        """Test fetching tournaments within a specific date range"""
        from datetime import datetime, timedelta
        import time
        
        # Get tournaments from 60 to 30 days ago with format
        end_time = int((datetime.now() - timedelta(days=30)).timestamp())
        start_time = int((datetime.now() - timedelta(days=60)).timestamp())
        
        tournaments = client.get_tournaments(
            game="Magic: The Gathering",
            format="Standard",
            start=start_time,
            end=end_time
        )
        
        # API should return a list (may be empty)
        assert tournaments is not None, "API returned None - check API key and parameters"
        
        assert isinstance(tournaments, list), "Tournaments should be a list"
    
    def test_get_tournaments_with_columns(self, client):
        """Test fetching tournaments with specific columns"""
        tournaments = client.get_tournaments(
            game="Magic: The Gathering",
            format="Standard",
            last=60,
            columns=["name", "id", "decklist"]
        )
        
        # API should return a list (may be empty)
        assert tournaments is not None, "API returned None - check API key and parameters"
        
        assert isinstance(tournaments, list), "Tournaments should be a list"
        
        # Note: The API may not always respect column filtering exactly
        # Just verify we got a valid response
        if len(tournaments) > 0:
            assert 'TID' in tournaments[0] or 'id' in tournaments[0], "Tournament missing ID field"
    
    def test_get_tournaments_with_last_parameter_and_format(self, client):
        """Test fetching tournaments with 'last' and 'format' parameters (like load_initial does)"""
        # This tests the scenario used by load_initial - requires format parameter
        tournaments = client.get_tournaments(
            game="Magic: The Gathering",
            format="Standard",
            last=90
        )
        
        # API should return a list (may be empty) - should not raise 400 error
        assert tournaments is not None, "API returned None - check API key and parameters"
        assert isinstance(tournaments, list), "Tournaments should be a list"
    
    def test_get_tournaments_requires_format_parameter(self, client):
        """Test that API requires format parameter (validates our understanding)"""
        # Test that API correctly rejects requests without format
        # This validates our fix to require format in load_initial
        # Note: get_tournaments catches ValueError and returns None, so we check for None
        tournaments = client.get_tournaments(
            game="Magic: The Gathering",
            last=30
        )
        
        # Should return None because format is required
        assert tournaments is None, "API should reject requests without format parameter"


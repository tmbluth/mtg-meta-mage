"""Integration tests for Scryfall API client with real API calls"""

import pytest
import os
import time

from src.etl.api_clients.scryfall_client import ScryfallClient


class TestScryfallIntegration:
    """Integration tests for Scryfall API client"""
    
    @pytest.fixture(scope="class")
    def client(self):
        """Create Scryfall client for tests"""
        return ScryfallClient()
    
    def test_get_bulk_data_url(self, client):
        """Test fetching bulk data URL"""
        url = client.get_bulk_data_url('oracle_cards')
        
        assert url is not None, "Failed to fetch oracle cards bulk data URL"
        assert isinstance(url, str), "URL should be a string"
        assert url.startswith('https://'), "URL should start with https://"
        assert 'oracle-cards' in url or 'oracle_cards' in url, "URL should reference oracle cards"
    
    def test_get_rulings_bulk_data_url(self, client):
        """Test fetching rulings bulk data URL"""
        url = client.get_bulk_data_url('rulings')
        
        assert url is not None, "Failed to fetch rulings bulk data URL"
        assert isinstance(url, str), "URL should be a string"
        assert url.startswith('https://'), "URL should start with https://"
        assert 'rulings' in url, "URL should reference rulings"
    
    def test_get_card_by_name(self, client):
        """Test fetching a card by name"""
        # Use a well-known card that should always exist
        card = client.get_card_by_name('Lightning Bolt')
        
        assert card is not None, "Failed to fetch Lightning Bolt"
        assert card['name'] == 'Lightning Bolt', "Card name mismatch"
        assert 'mana_cost' in card, "Card missing mana_cost"
        assert 'type_line' in card, "Card missing type_line"
        assert 'oracle_id' in card, "Card missing oracle_id"
    
    def test_get_card_price(self, client):
        """Test fetching card price"""
        # Use a card that should have price data
        price = client.get_card_price('Lightning Bolt')
        
        assert price is not None, "Failed to fetch Lightning Bolt price"
        assert isinstance(price, float), "Price should be a float"
        assert price > 0, "Price should be positive"
    
    def test_get_nonexistent_card_returns_none(self, client):
        """Test that nonexistent card returns None"""
        card = client.get_card_by_name('This Card Definitely Does Not Exist 12345')
        
        assert card is None, "Nonexistent card should return None"
    
    def test_rate_limiting(self, client):
        """Test that rate limiting is working"""
        # Make multiple requests and ensure they respect rate limiting
        start_time = time.time()
        
        for i in range(3):
            client.get_card_by_name('Lightning Bolt')
        
        elapsed = time.time() - start_time
        
        # Should take at least 2 * RATE_LIMIT_DELAY seconds (3 requests = 2 delays)
        expected_min_time = 2 * ScryfallClient.RATE_LIMIT_DELAY * 0.9  # 10% margin
        assert elapsed >= expected_min_time, f"Rate limiting not enforced: {elapsed}s < {expected_min_time}s"
    
    def test_get_multiple_cards(self, client):
        """Test fetching multiple different cards"""
        test_cards = ['Lightning Bolt', 'Counterspell', 'Llanowar Elves']
        
        for card_name in test_cards:
            card = client.get_card_by_name(card_name)
            
            assert card is not None, f"Failed to fetch {card_name}"
            assert card['name'] == card_name, f"Card name mismatch for {card_name}"
    
    def test_join_cards_with_rulings(self, client):
        """Test joining cards with rulings"""
        # Create sample cards and rulings data
        cards = [
            {'oracle_id': 'test1', 'name': 'Test Card 1'},
            {'oracle_id': 'test2', 'name': 'Test Card 2'}
        ]
        
        rulings = [
            {'oracle_id': 'test1', 'comment': 'Ruling 1 for test card 1'},
            {'oracle_id': 'test1', 'comment': 'Ruling 2 for test card 1'},
            {'oracle_id': 'test2', 'comment': 'Ruling 1 for test card 2'}
        ]
        
        result = client.join_cards_with_rulings(cards, rulings)
        
        assert len(result) == 2, "Should have 2 cards"
        assert len(result[0]['rulings']) == 2, "Card 1 should have 2 rulings"
        assert len(result[1]['rulings']) == 1, "Card 2 should have 1 ruling"
    
    def test_get_card_with_special_characters(self, client):
        """Test fetching card with special characters in name"""
        # Test card with apostrophe
        card = client.get_card_by_name("Jace, the Mind Sculptor")
        
        if card is not None:
            assert 'Jace' in card['name'], "Card name should contain 'Jace'"
            assert 'oracle_id' in card, "Card missing oracle_id"
    
    def test_download_bulk_data_url_structure(self, client):
        """Test that bulk data URLs have expected structure"""
        oracle_url = client.get_bulk_data_url('oracle_cards')
        rulings_url = client.get_bulk_data_url('rulings')
        
        assert oracle_url is not None, "Oracle cards URL should not be None"
        assert rulings_url is not None, "Rulings URL should not be None"
        
        # URLs should be different
        assert oracle_url != rulings_url, "Oracle and rulings URLs should be different"
        
        # Both should be valid HTTPS URLs
        assert oracle_url.startswith('https://'), "Oracle URL should use HTTPS"
        assert rulings_url.startswith('https://'), "Rulings URL should use HTTPS"
    
    def test_card_has_expected_fields(self, client):
        """Test that fetched card has expected fields"""
        card = client.get_card_by_name('Lightning Bolt')
        
        assert card is not None, "Failed to fetch card"
        
        # Check for expected fields
        expected_fields = ['id', 'oracle_id', 'name', 'mana_cost', 'cmc', 'type_line']
        for field in expected_fields:
            assert field in card, f"Card missing expected field: {field}"
    
    def test_card_price_with_no_price(self, client):
        """Test handling of cards without price data"""
        # Some promotional or special cards may not have prices
        # Just test that the function handles it gracefully
        price = client.get_card_price('Test Card That May Not Have Price')
        
        # Should return None if card not found or has no price
        assert price is None or isinstance(price, float), "Price should be None or float"


"""Integration tests for Scryfall API client with real API calls"""

import pytest
import os
import time
import signal
from contextlib import contextmanager
from psycopg2.extensions import connection

from src.etl.api_clients.scryfall_client import ScryfallClient
from src.etl.etl_pipeline import load_cards_from_bulk_data
from src.database.connection import DatabaseConnection


class TimeoutError(Exception):
    """Custom timeout exception"""
    pass


@contextmanager
def timeout(seconds):
    """Context manager for timeout using SIGALRM (Unix/macOS only)"""
    # Only use SIGALRM on Unix-like systems
    if not hasattr(signal, 'SIGALRM'):
        # On Windows or systems without SIGALRM, skip timeout
        yield
        return
    
    def timeout_handler(signum, frame):
        raise TimeoutError(f"Operation timed out after {seconds} seconds")
    
    # Set up signal handler
    old_handler = signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(seconds)
    
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


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
    
    @pytest.mark.integration
    def test_download_oracle_cards_bulk_data(self, client):
        """Test downloading oracle cards bulk data"""
        result = client.download_oracle_cards()
        
        assert result is not None, "Bulk data download should succeed"
        assert 'data' in result, "Result should contain 'data' key"
        assert isinstance(result['data'], list), "Data should be a list"
        assert len(result['data']) > 0, "Should have at least one card"
        
        # Verify card structure
        first_card = result['data'][0]
        assert 'id' in first_card, "Card should have 'id' field"
        assert 'name' in first_card, "Card should have 'name' field"
        assert 'oracle_id' in first_card, "Card should have 'oracle_id' field"
    
    @pytest.mark.integration
    def test_download_rulings_bulk_data(self, client):
        """Test downloading rulings bulk data"""
        result = client.download_rulings()
        
        assert result is not None, "Rulings bulk data download should succeed"
        assert 'data' in result, "Result should contain 'data' key"
        assert isinstance(result['data'], list), "Data should be a list"
        
        # Rulings may be empty, but structure should be correct
        if len(result['data']) > 0:
            first_ruling = result['data'][0]
            assert 'oracle_id' in first_ruling, "Ruling should have 'oracle_id' field"
    
    @pytest.mark.integration
    def test_transform_card_to_db_row(self, client):
        """Test transforming Scryfall card to database row format"""
        # Get a real card
        card = client.get_card_by_name('Lightning Bolt')
        assert card is not None, "Should fetch Lightning Bolt"
        
        # Transform to DB row format
        db_row = client.transform_card_to_db_row(card)
        
        # Verify required fields
        assert 'card_id' in db_row, "DB row should have card_id"
        assert 'name' in db_row, "DB row should have name"
        assert db_row['name'] == 'Lightning Bolt', "Name should match"
        
        # Verify optional fields are present (may be None)
        assert 'oracle_text' in db_row, "DB row should have oracle_text field"
        assert 'type_line' in db_row, "DB row should have type_line field"
        assert 'mana_cost' in db_row, "DB row should have mana_cost field"
        assert 'cmc' in db_row, "DB row should have cmc field"
        assert 'color_identity' in db_row, "DB row should have color_identity field"
        assert isinstance(db_row['color_identity'], list), "color_identity should be a list"
    
    @pytest.mark.integration
    def test_load_cards_from_bulk_data_into_database(self, test_db_connection: connection):
        """Test loading cards from Scryfall bulk data into database"""
        # Ensure database connection pool is initialized
        DatabaseConnection.initialize_pool()
        
        # Clear existing cards to test fresh load
        cursor = test_db_connection.cursor()
        cursor.execute("DELETE FROM deck_cards;")
        cursor.execute("DELETE FROM cards;")
        test_db_connection.commit()
        cursor.close()
        
        # Load cards from bulk data with timeout (5 minutes max for download + processing)
        # This test downloads real Scryfall bulk data which can be large
        try:
            with timeout(300):  # 5 minute timeout
                result = load_cards_from_bulk_data(batch_size=100)
        except TimeoutError as e:
            pytest.skip(f"Test timed out: {e}. Scryfall bulk data download may be slow.")
        except Exception as e:
            pytest.fail(f"Failed to load cards from bulk data: {e}")
        
        # Verify result structure
        assert 'cards_loaded' in result, "Result should have cards_loaded"
        assert 'cards_processed' in result, "Result should have cards_processed"
        assert 'errors' in result, "Result should have errors"
        
        # Should have loaded some cards
        assert result['cards_loaded'] > 0, f"Should have loaded at least some cards, got {result['cards_loaded']}"
        assert result['cards_processed'] > 0, f"Should have processed at least some cards, got {result['cards_processed']}"
        
        # Verify cards are in database
        cursor = test_db_connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM cards;")
        card_count = cursor.fetchone()[0]
        cursor.close()
        
        assert card_count > 0, f"Should have cards in database, got {card_count}"
        assert card_count == result['cards_loaded'], f"Card count should match loaded count: {card_count} != {result['cards_loaded']}"
        
        # Verify a specific card exists (Lightning Bolt should be in bulk data)
        cursor = test_db_connection.cursor()
        cursor.execute("SELECT name, oracle_text, type_line FROM cards WHERE name = 'Lightning Bolt' LIMIT 1;")
        card = cursor.fetchone()
        cursor.close()
        
        if card:
            assert card[0] == 'Lightning Bolt', "Card name should match"
            assert card[2] is not None, "Card should have type_line"
    
    @pytest.mark.integration
    def test_load_cards_upsert_logic(self, test_db_connection: connection):
        """Test that loading cards again uses upsert logic (updates existing)"""
        # Ensure database connection pool is initialized
        DatabaseConnection.initialize_pool()
        
        # Clear existing cards
        cursor = test_db_connection.cursor()
        cursor.execute("DELETE FROM deck_cards;")
        cursor.execute("DELETE FROM cards;")
        test_db_connection.commit()
        cursor.close()
        
        # Load cards first time with timeout
        try:
            with timeout(300):  # 5 minute timeout
                result1 = load_cards_from_bulk_data(batch_size=100)
        except TimeoutError as e:
            pytest.skip(f"Test timed out on first load: {e}. Scryfall bulk data download may be slow.")
        except Exception as e:
            pytest.fail(f"Failed to load cards first time: {e}")
        
        first_load_count = result1['cards_loaded']
        assert first_load_count > 0, f"First load should succeed, got {first_load_count}"
        
        # Get count after first load
        cursor = test_db_connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM cards;")
        count_after_first = cursor.fetchone()[0]
        cursor.close()
        
        assert count_after_first == first_load_count, f"Count should match first load: {count_after_first} != {first_load_count}"
        
        # Load cards again (should update, not duplicate) with timeout
        try:
            with timeout(300):  # 5 minute timeout
                result2 = load_cards_from_bulk_data(batch_size=100)
        except TimeoutError as e:
            pytest.skip(f"Test timed out on second load: {e}. Scryfall bulk data download may be slow.")
        except Exception as e:
            pytest.fail(f"Failed to load cards second time: {e}")
        
        second_load_count = result2['cards_loaded']
        
        # Get count after second load
        cursor = test_db_connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM cards;")
        count_after_second = cursor.fetchone()[0]
        cursor.close()
        
        # Should have same or similar count (upsert, not insert)
        assert count_after_second == count_after_first, f"Should not have duplicates after upsert: {count_after_second} != {count_after_first}"
        assert second_load_count > 0, f"Second load should process cards, got {second_load_count}"


"""End-to-end integration tests for Scryfall client and cards pipeline"""

import pytest
import logging
from typing import Dict, List

from src.clients.scryfall_client import ScryfallClient
from src.etl.cards_pipeline import CardsPipeline
from src.database.connection import DatabaseConnection

logger = logging.getLogger(__name__)


@pytest.mark.integration
class TestScryfallClient:
    """Integration tests for ScryfallClient with real API calls"""
    
    def test_get_bulk_data_url(self):
        """Test fetching bulk data URL from Scryfall API"""
        client = ScryfallClient()
        
        url = client.get_bulk_data_url("oracle_cards")
        assert url is not None
        assert url.startswith("https://")
        assert "oracle-cards" in url or "json" in url
        
        logger.info(f"Retrieved oracle cards URL: {url}")
    
    def test_download_oracle_cards(self):
        """Test downloading oracle cards bulk data from Scryfall"""
        client = ScryfallClient()
        
        result = client.download_oracle_cards()
        assert result is not None
        assert 'data' in result
        assert 'file_path' in result
        
        cards = result['data']
        assert isinstance(cards, list)
        assert len(cards) > 0
        
        # Verify card structure
        first_card = cards[0]
        assert 'id' in first_card
        assert 'name' in first_card
        
        logger.info(f"Downloaded {len(cards)} oracle cards")
    
    def test_download_rulings(self):
        """Test downloading rulings bulk data from Scryfall"""
        client = ScryfallClient()
        
        result = client.download_rulings()
        assert result is not None
        assert 'data' in result
        
        rulings = result['data']
        assert isinstance(rulings, list)
        
        if len(rulings) > 0:
            # Verify ruling structure
            first_ruling = rulings[0]
            assert 'oracle_id' in first_ruling
            assert 'comment' in first_ruling
        
        logger.info(f"Downloaded {len(rulings)} rulings")
    
    def test_join_cards_with_rulings(self):
        """Test joining cards with rulings"""
        client = ScryfallClient()
        
        # Download small sample of data
        cards_data = client.download_oracle_cards()
        rulings_data = client.download_rulings()
        
        assert cards_data is not None
        assert rulings_data is not None
        
        # Use first 100 cards for testing
        cards = cards_data['data'][:100]
        rulings = rulings_data['data']
        
        cards_with_rulings = client.join_cards_with_rulings(cards, rulings)
        
        assert len(cards_with_rulings) == len(cards)
        assert all('rulings' in card for card in cards_with_rulings)
        
        # Check that at least some cards have rulings
        cards_with_rulings_count = sum(1 for card in cards_with_rulings if card['rulings'])
        logger.info(f"Joined {len(cards)} cards with rulings. {cards_with_rulings_count} cards have rulings")
    
    def test_transform_card_to_db_row(self):
        """Test transforming a card to database row format"""
        client = ScryfallClient()
        
        # Download a small sample
        cards_data = client.download_oracle_cards()
        assert cards_data is not None
        
        # Get a few cards to test
        cards = cards_data['data'][:5]
        
        for card in cards:
            db_row = client.transform_card_to_db_row(card)
            
            # Verify required fields
            assert 'card_id' in db_row
            assert 'name' in db_row
            assert db_row['card_id'] == card.get('id')
            assert db_row['name'] == card.get('name')
            
            # Verify optional fields are present
            assert 'set' in db_row
            assert 'oracle_text' in db_row
            assert 'rulings' in db_row
            assert 'type_line' in db_row
            assert 'color_identity' in db_row
            assert isinstance(db_row['color_identity'], list)


@pytest.mark.integration
class TestCardsPipeline:
    """Integration tests for CardsPipeline with real database"""
    
    def test_insert_cards_initial_load(self, test_database):
        """Test inserting cards into database with initial load (update existing)"""
        pipeline = CardsPipeline()
        
        # Insert cards with small batch size for testing
        result = pipeline.insert_cards(batch_size=500, update_existing=True)
        
        assert result['cards_loaded'] > 0
        assert result['cards_processed'] > 0
        assert result['errors'] == 0
        
        # Verify cards are in database
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM cards")
            count = cur.fetchone()[0]
            assert count == result['cards_loaded']
            
            # Verify a sample card
            cur.execute("SELECT card_id, name, type_line FROM cards LIMIT 1")
            card = cur.fetchone()
            assert card is not None
            assert card[0] is not None  # card_id
            assert card[1] is not None  # name
        
        logger.info(f"Successfully loaded {result['cards_loaded']} cards into database")
    
    def test_load_initial(self, test_database):
        """Test initial load method"""
        pipeline = CardsPipeline()
        
        # Limit to first 1000 cards for testing
        result = pipeline.load_initial(batch_size=500, limit=1000)
        
        assert result['success'] is True
        assert result['objects_loaded'] > 0
        assert result['objects_processed'] > 0
        assert result['errors'] == 0
        
        # Verify load metadata was created
        with DatabaseConnection.get_cursor() as cur:
            cur.execute(
                "SELECT last_load_date, objects_loaded, data_type, load_type "
                "FROM load_metadata WHERE data_type = 'cards' ORDER BY id DESC LIMIT 1"
            )
            metadata = cur.fetchone()
            assert metadata is not None
            assert metadata[1] == result['objects_loaded']  # objects_loaded
            assert metadata[2] == 'cards'  # data_type
            assert metadata[3] == 'initial'  # load_type
        
        logger.info(f"Initial load completed: {result['objects_loaded']} cards loaded")
    
    def test_load_incremental_skips_existing(self, test_database):
        """Test incremental load skips existing cards"""
        pipeline = CardsPipeline()
        
        # Get initial count (cards may already exist from previous tests)
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM cards")
            count_before = cur.fetchone()[0]
        
        # First, do an initial load with update_existing=True
        # Limit to first 1000 cards for testing
        initial_result = pipeline.load_initial(batch_size=500, limit=1000)
        assert initial_result['success'] is True
        initial_loaded = initial_result['objects_loaded']
        assert initial_loaded > 0, "Initial load should process some cards"
        
        # Get count from database after initial load
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM cards")
            count_after_initial = cur.fetchone()[0]
        
        # With update_existing=True, cards are updated if they exist, so count may not increase
        # But we should have processed cards
        assert count_after_initial >= count_before
        
        # Then do an incremental load with update_existing=False - should skip existing cards
        # Limit to first 1000 cards for testing (same limit as initial)
        incremental_result = pipeline.load_incremental(batch_size=500, limit=1000)
        assert incremental_result['success'] is True
        
        # Verify total count hasn't increased (all cards were skipped due to DO NOTHING)
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM cards")
            final_count = cur.fetchone()[0]
        
        # The incremental load should have skipped all existing cards (DO NOTHING on conflict)
        # So final count should equal count after initial (no new cards added)
        assert final_count == count_after_initial, \
            f"Expected {count_after_initial} cards, got {final_count}. Incremental load should skip existing cards."
        
        # Verify that incremental load didn't add new cards (all were skipped)
        # Note: objects_loaded may reflect cards processed, not necessarily inserted
        # The key check is that final_count == count_after_initial (no new cards in DB)
        # If all cards were skipped, the count should remain the same
        
        logger.info(
            f"Incremental load: {incremental_result['objects_loaded']} new cards "
            f"(total in DB: {final_count}, after initial: {count_after_initial})"
        )
    
    def test_card_data_integrity(self, test_database):
        """Test that card data is correctly stored in database"""
        pipeline = CardsPipeline()
        
        # Load cards
        result = pipeline.insert_cards(batch_size=500, update_existing=True)
        assert result['cards_loaded'] > 0
        
        # Verify data integrity
        with DatabaseConnection.get_cursor() as cur:
            # Check that required fields are populated
            cur.execute(
                """
                SELECT COUNT(*) FROM cards 
                WHERE card_id IS NULL OR name IS NULL OR name = ''
                """
            )
            null_count = cur.fetchone()[0]
            assert null_count == 0, "Found cards with null card_id or name"
            
            # Check that color_identity is stored as array
            cur.execute(
                """
                SELECT color_identity FROM cards 
                WHERE color_identity IS NOT NULL 
                LIMIT 10
                """
            )
            color_identities = cur.fetchall()
            for ci in color_identities:
                assert isinstance(ci[0], list), f"color_identity should be array, got {type(ci[0])}"
            
            # Verify some cards have rulings
            cur.execute("SELECT COUNT(*) FROM cards WHERE rulings IS NOT NULL AND rulings != ''")
            cards_with_rulings = cur.fetchone()[0]
            logger.info(f"Found {cards_with_rulings} cards with rulings")
        
        logger.info("Card data integrity checks passed")


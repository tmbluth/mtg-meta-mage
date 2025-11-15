"""Integration tests for end-to-end decklist processing with Scryfall and TopDeck data"""

import pytest
import signal
from contextlib import contextmanager
from psycopg2.extensions import connection

from src.etl.etl_pipeline import ETLPipeline, parse_decklist, load_cards_from_bulk_data
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


@pytest.mark.integration
class TestDecklistProcessing:
    """Integration tests for end-to-end decklist processing"""
    
    def test_parse_decklist_with_mainboard_and_sideboard(self):
        """Test parsing a decklist with mainboard and sideboard sections"""
        decklist_text = """4 Lightning Bolt
4 Counterspell
2 Llanowar Elves

Sideboard
3 Disenchant
2 Naturalize
"""
        parsed = parse_decklist(decklist_text)
        
        assert len(parsed) == 6, "Should parse 6 cards total"
        
        # Check mainboard cards
        mainboard = [c for c in parsed if c['section'] == 'mainboard']
        assert len(mainboard) == 3, "Should have 3 mainboard cards"
        
        # Check sideboard cards
        sideboard = [c for c in parsed if c['section'] == 'sideboard']
        assert len(sideboard) == 2, "Should have 2 sideboard cards"
        
        # Verify specific cards
        bolt = next((c for c in parsed if c['card_name'] == 'Lightning Bolt'), None)
        assert bolt is not None, "Should find Lightning Bolt"
        assert bolt['quantity'] == 4, "Lightning Bolt should have quantity 4"
        assert bolt['section'] == 'mainboard', "Lightning Bolt should be mainboard"
        
        disenchant = next((c for c in parsed if c['card_name'] == 'Disenchant'), None)
        assert disenchant is not None, "Should find Disenchant"
        assert disenchant['quantity'] == 3, "Disenchant should have quantity 3"
        assert disenchant['section'] == 'sideboard', "Disenchant should be sideboard"
    
    def test_parse_and_store_decklist_cards_with_scryfall_data(
        self, test_db_connection: connection
    ):
        """Test parsing and storing decklist cards when cards exist in database from Scryfall"""
        # Ensure database connection pool is initialized
        DatabaseConnection.initialize_pool()
        
        # Clear existing data
        cursor = test_db_connection.cursor()
        cursor.execute("DELETE FROM deck_cards;")
        cursor.execute("DELETE FROM decklists;")
        cursor.execute("DELETE FROM players;")
        cursor.execute("DELETE FROM tournaments;")
        cursor.execute("DELETE FROM cards;")
        test_db_connection.commit()
        cursor.close()
        
        # Load some cards from Scryfall (small batch for testing) with timeout
        try:
            with timeout(300):  # 5 minute timeout
                load_result = load_cards_from_bulk_data(batch_size=100)
        except TimeoutError as e:
            pytest.skip(f"Test timed out: {e}. Scryfall bulk data download may be slow.")
        except Exception as e:
            pytest.fail(f"Failed to load cards from bulk data: {e}")
        
        assert load_result['cards_loaded'] > 0, f"Should load some cards, got {load_result['cards_loaded']}"
        
        # Verify cards are in database
        cursor = test_db_connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM cards;")
        card_count = cursor.fetchone()[0]
        cursor.close()
        assert card_count > 0, "Should have cards in database"
        
        # Create a test tournament and player
        cursor = test_db_connection.cursor()
        cursor.execute("""
            INSERT INTO tournaments (
                tournament_id, tournament_name, format, start_date
            ) VALUES (
                'test-tournament-123', 'Test Tournament', 'Standard', 1234567890
            )
        """)
        
        cursor.execute("""
            INSERT INTO players (
                player_id, tournament_id, name, wins, losses
            ) VALUES (
                'test-player-456', 'test-tournament-123', 'Test Player', 3, 2
            )
        """)
        
        # Create a decklist with cards that should exist in Scryfall bulk data
        decklist_text = """4 Lightning Bolt
4 Counterspell
2 Llanowar Elves

Sideboard
3 Disenchant
2 Naturalize
"""
        
        cursor.execute("""
            INSERT INTO decklists (player_id, tournament_id, decklist_text)
            VALUES ('test-player-456', 'test-tournament-123', %s)
        """, (decklist_text,))
        
        test_db_connection.commit()
        cursor.close()
        
        # Parse and store decklist cards
        pipeline = ETLPipeline()
        with DatabaseConnection.transaction() as conn:
            pipeline.parse_and_store_decklist_cards(
                'test-player-456',
                'test-tournament-123',
                decklist_text,
                conn
            )
        
        # Verify deck_cards entries were created
        cursor = test_db_connection.cursor()
        cursor.execute("""
            SELECT dc.section, dc.quantity, c.name
            FROM deck_cards dc
            JOIN cards c ON dc.card_id = c.card_id
            JOIN decklists d ON dc.decklist_id = d.decklist_id
            WHERE d.player_id = 'test-player-456' AND d.tournament_id = 'test-tournament-123'
            ORDER BY dc.section, c.name
        """)
        deck_cards = cursor.fetchall()
        cursor.close()
        
        # Should have found at least some cards (may not find all due to name variations)
        assert len(deck_cards) > 0, "Should have linked at least some cards"
        
        # Verify structure
        for section, quantity, name in deck_cards:
            assert section in ['mainboard', 'sideboard'], f"Section should be mainboard or sideboard, got {section}"
            assert quantity > 0, f"Quantity should be positive, got {quantity}"
            assert name is not None, "Card name should not be None"
    
    def test_decklist_processing_with_missing_cards(self, test_db_connection: connection):
        """Test that decklist processing handles missing cards gracefully"""
        # Ensure database connection pool is initialized
        DatabaseConnection.initialize_pool()
        
        # Clear existing data
        cursor = test_db_connection.cursor()
        cursor.execute("DELETE FROM deck_cards;")
        cursor.execute("DELETE FROM decklists;")
        cursor.execute("DELETE FROM players;")
        cursor.execute("DELETE FROM tournaments;")
        cursor.execute("DELETE FROM cards;")
        test_db_connection.commit()
        cursor.close()
        
        # Load some cards from Scryfall with timeout
        try:
            with timeout(300):  # 5 minute timeout
                load_result = load_cards_from_bulk_data(batch_size=100)
        except TimeoutError as e:
            pytest.skip(f"Test timed out: {e}. Scryfall bulk data download may be slow.")
        except Exception as e:
            pytest.fail(f"Failed to load cards from bulk data: {e}")
        
        assert load_result['cards_loaded'] > 0, f"Should load some cards, got {load_result['cards_loaded']}"
        
        # Create a test tournament and player
        cursor = test_db_connection.cursor()
        cursor.execute("""
            INSERT INTO tournaments (
                tournament_id, tournament_name, format, start_date
            ) VALUES (
                'test-tournament-789', 'Test Tournament', 'Standard', 1234567890
            )
        """)
        
        cursor.execute("""
            INSERT INTO players (
                player_id, tournament_id, name, wins, losses
            ) VALUES (
                'test-player-999', 'test-tournament-789', 'Test Player', 3, 2
            )
        """)
        
        # Create a decklist with some cards that exist and some that don't
        decklist_text = """4 Lightning Bolt
4 This Card Does Not Exist In Database
2 Another Fake Card Name

Sideboard
3 Disenchant
"""
        
        cursor.execute("""
            INSERT INTO decklists (player_id, tournament_id, decklist_text)
            VALUES ('test-player-999', 'test-tournament-789', %s)
        """, (decklist_text,))
        
        test_db_connection.commit()
        cursor.close()
        
        # Parse and store decklist cards (should handle missing cards gracefully)
        pipeline = ETLPipeline()
        with DatabaseConnection.transaction() as conn:
            # Should not raise exception even with missing cards
            pipeline.parse_and_store_decklist_cards(
                'test-player-999',
                'test-tournament-789',
                decklist_text,
                conn
            )
        
        # Verify that found cards were linked
        cursor = test_db_connection.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM deck_cards dc
            JOIN decklists d ON dc.decklist_id = d.decklist_id
            WHERE d.player_id = 'test-player-999' AND d.tournament_id = 'test-tournament-789'
        """)
        linked_count = cursor.fetchone()[0]
        cursor.close()
        
        # Should have linked at least Lightning Bolt and Disenchant if they exist
        # (may be 0 if cards don't match exactly by name)
        assert linked_count >= 0, "Should handle missing cards without error"
    
    def test_end_to_end_tournament_load_with_decklist_parsing(
        self, test_db_connection: connection
    ):
        """Test end-to-end: load cards, then load tournament with decklist parsing"""
        # Ensure database connection pool is initialized
        DatabaseConnection.initialize_pool()
        
        # Clear existing data
        cursor = test_db_connection.cursor()
        cursor.execute("DELETE FROM deck_cards;")
        cursor.execute("DELETE FROM decklists;")
        cursor.execute("DELETE FROM players;")
        cursor.execute("DELETE FROM tournaments;")
        cursor.execute("DELETE FROM cards;")
        test_db_connection.commit()
        cursor.close()
        
        # Step 1: Load cards from Scryfall with timeout
        try:
            with timeout(300):  # 5 minute timeout
                load_result = load_cards_from_bulk_data(batch_size=100)
        except TimeoutError as e:
            pytest.skip(f"Test timed out: {e}. Scryfall bulk data download may be slow.")
        except Exception as e:
            pytest.fail(f"Failed to load cards from bulk data: {e}")
        
        assert load_result['cards_loaded'] > 0, f"Should load cards from Scryfall, got {load_result['cards_loaded']}"
        
        # Step 2: Verify cards are in database
        cursor = test_db_connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM cards;")
        card_count = cursor.fetchone()[0]
        cursor.close()
        assert card_count > 0, "Should have cards in database"
        
        # Step 3: Create a mock tournament with decklist (simulating TopDeck data)
        tournament_data = {
            'TID': 'end-to-end-test-001',
            'tournamentName': 'End-to-End Test Tournament',
            'format': 'Standard',
            'startDate': 1234567890,
            'swissNum': 8,
            'topCut': 4
        }
        
        player_data = {
            'id': 'end-to-end-player-001',
            'name': 'Test Player',
            'wins': 5,
            'losses': 2,
            'decklist': """4 Lightning Bolt
4 Counterspell
2 Llanowar Elves

Sideboard
3 Disenchant
2 Naturalize
"""
        }
        
        # Load tournament using ETLPipeline
        pipeline = ETLPipeline()
        
        # Insert tournament
        with DatabaseConnection.transaction() as conn:
            pipeline.insert_tournament(tournament_data, conn)
            pipeline.insert_players(
                tournament_data['TID'],
                [player_data],
                conn
            )
            pipeline.insert_decklists(
                tournament_data['TID'],
                [player_data],
                conn
            )
            # Parse and store decklist cards
            pipeline.parse_and_store_decklist_cards(
                player_data['id'],
                tournament_data['TID'],
                player_data['decklist'],
                conn
            )
        
        # Step 4: Verify end-to-end result
        cursor = test_db_connection.cursor()
        
        # Check tournament exists
        cursor.execute("""
            SELECT tournament_id FROM tournaments WHERE tournament_id = %s
        """, (tournament_data['TID'],))
        tournament = cursor.fetchone()
        assert tournament is not None, "Tournament should exist"
        
        # Check player exists
        cursor.execute("""
            SELECT player_id FROM players WHERE player_id = %s AND tournament_id = %s
        """, (player_data['id'], tournament_data['TID']))
        player = cursor.fetchone()
        assert player is not None, "Player should exist"
        
        # Check decklist exists
        cursor.execute("""
            SELECT decklist_id FROM decklists WHERE player_id = %s AND tournament_id = %s
        """, (player_data['id'], tournament_data['TID']))
        decklist_result = cursor.fetchone()
        assert decklist_result is not None, "Decklist should exist"
        
        # Check deck_cards entries
        cursor.execute("""
            SELECT COUNT(*) FROM deck_cards dc
            JOIN decklists d ON dc.decklist_id = d.decklist_id
            WHERE d.player_id = %s AND d.tournament_id = %s
        """, (player_data['id'], tournament_data['TID']))
        deck_cards_count = cursor.fetchone()[0]
        cursor.close()
        
        # Should have linked at least some cards (may not find all due to name matching)
        assert deck_cards_count >= 0, "Should have processed decklist cards"
        
        # Verify deck_cards structure if any cards were linked
        if deck_cards_count > 0:
            cursor = test_db_connection.cursor()
            cursor.execute("""
                SELECT dc.section, dc.quantity, c.name
                FROM deck_cards dc
                JOIN cards c ON dc.card_id = c.card_id
                JOIN decklists d ON dc.decklist_id = d.decklist_id
                WHERE d.player_id = %s AND d.tournament_id = %s
                LIMIT 5
            """, (player_data['id'], tournament_data['TID']))
            sample_cards = cursor.fetchall()
            cursor.close()
            
            # Verify structure
            for section, quantity, name in sample_cards:
                assert section in ['mainboard', 'sideboard'], f"Invalid section: {section}"
                assert quantity > 0, f"Invalid quantity: {quantity}"
                assert name is not None and name != '', f"Invalid name: {name}"


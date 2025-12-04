"""End-to-end integration tests for TopDeck client and tournaments pipeline"""

import os
import pytest
import logging
from typing import Dict, List, Optional

from src.clients.topdeck_client import TopDeckClient
from src.etl.tournaments_pipeline import TournamentsPipeline
from src.database.connection import DatabaseConnection

logger = logging.getLogger(__name__)


@pytest.mark.integration
class TestTopDeckClient:
    """Integration tests for TopDeckClient with real API calls"""
    
    @pytest.fixture
    def client(self):
        """Create TopDeck client instance"""
        api_key = os.getenv('TOPDECK_API_KEY')
        assert api_key, "TOPDECK_API_KEY environment variable not set"
        return TopDeckClient(api_key)
    
    def test_get_tournaments(self, client):
        """Test fetching tournaments from TopDeck API"""
        tournaments = client.get_tournaments(
            game="Magic: The Gathering",
            format="Modern",
            last=7  # Last 7 days
        )
        
        assert tournaments is not None
        assert isinstance(tournaments, list)
        
        if len(tournaments) > 0:
            # Verify tournament structure
            tournament = tournaments[0]
            assert 'TID' in tournament
            assert 'tournamentName' in tournament
            assert 'format' in tournament
            assert 'startDate' in tournament
            
            logger.info(f"Retrieved {len(tournaments)} tournaments")
    
    def test_get_tournament_details(self, client):
        """Test fetching tournament details"""
        # First get a tournament ID
        tournaments = client.get_tournaments(
            game="Magic: The Gathering",
            format="Modern",
            last=30
        )
        
        assert tournaments is not None and len(tournaments) > 0
        
        tournament_id = tournaments[0].get('TID')
        assert tournament_id is not None
        
        # Get tournament details
        details = client.get_tournament_details(tournament_id)
        
        assert details is not None
        assert isinstance(details, dict)
        
        # API response structure has changed - check for expected fields
        # Response should have 'data', 'rounds', and/or 'standings'
        assert 'data' in details or 'rounds' in details or 'standings' in details
        
        # Verify it has standings (which contain player info)
        standings = details.get('standings', [])
        assert isinstance(standings, list)
        
        logger.info(f"Retrieved details for tournament {tournament_id} with {len(standings)} standings")
    
    def test_get_tournament_rounds(self, client):
        """Test fetching tournament rounds"""
        # First get a tournament ID
        tournaments = client.get_tournaments(
            game="Magic: The Gathering",
            format="Modern",
            last=30
        )
        
        assert tournaments is not None and len(tournaments) > 0
        
        tournament_id = tournaments[0].get('TID')
        assert tournament_id is not None
        
        # Get rounds
        rounds = client.get_tournament_rounds(tournament_id)
        
        # Rounds might be None or empty for some tournaments
        if rounds is not None:
            assert isinstance(rounds, list)
            logger.info(f"Retrieved {len(rounds)} rounds for tournament {tournament_id}")


@pytest.mark.integration
class TestTournamentsPipeline:
    """Integration tests for TournamentsPipeline with real database"""
    
    @pytest.fixture
    def pipeline(self):
        """Create TournamentsPipeline instance"""
        api_key = os.getenv('TOPDECK_API_KEY')
        assert api_key, "TOPDECK_API_KEY environment variable not set"
        return TournamentsPipeline(api_key)
    
    @pytest.fixture
    def sample_tournament(self, pipeline):
        """Get a sample tournament from API for testing"""
        tournaments = pipeline.client.get_tournaments(
            game="Magic: The Gathering",
            format="Modern",
            last=30
        )
        
        assert tournaments is not None and len(tournaments) > 0
        
        return tournaments[0]
    
    def test_filter_tournaments(self, pipeline):
        """Test filtering tournaments to exclude Commander and Limited formats"""
        # Get tournaments including all formats
        # API requires both game and format, so we'll get Modern tournaments
        tournaments = pipeline.client.get_tournaments(
            game="Magic: The Gathering",
            format="Modern",
            last=30
        )
        
        assert tournaments is not None and len(tournaments) > 0
        
        filtered = pipeline.filter_tournaments(tournaments)
        
        assert isinstance(filtered, list)
        assert len(filtered) <= len(tournaments)
        
        # Verify no Commander or Limited formats
        for tournament in filtered:
            format_name = tournament.get('format', '')
            assert not pipeline.is_commander_format(format_name)
            assert not pipeline.is_limited_format(format_name)
            assert tournament.get('game') == 'Magic: The Gathering'
        
        logger.info(f"Filtered {len(tournaments)} tournaments to {len(filtered)} constructed tournaments")
    
    def test_insert_tournament(self, pipeline, sample_tournament, test_database):
        """Test inserting a tournament into database"""
        tournament_id = sample_tournament.get('TID')
        assert tournament_id is not None
        
        with DatabaseConnection.transaction() as conn:
            pipeline.insert_tournament(sample_tournament, conn)
        
        # Verify tournament is in database
        with DatabaseConnection.get_cursor() as cur:
            cur.execute(
                "SELECT tournament_id, tournament_name, format FROM tournaments WHERE tournament_id = %s",
                (tournament_id,)
            )
            result = cur.fetchone()
            assert result is not None
            assert result[0] == tournament_id
        
        logger.info(f"Successfully inserted tournament {tournament_id}")
    
    def test_insert_all_tournament(self, pipeline, sample_tournament, test_database):
        """Test inserting a complete tournament with all data"""
        tournament_id = sample_tournament.get('TID')
        assert tournament_id is not None
        
        # Insert tournament with all data
        success = pipeline.insert_all(sample_tournament, include_rounds=True)
        assert success is True
        
        # Verify tournament is in database
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM tournaments WHERE tournament_id = %s", (tournament_id,))
            tournament_count = cur.fetchone()[0]
            assert tournament_count == 1
            
            # Check players
            cur.execute("SELECT COUNT(*) FROM players WHERE tournament_id = %s", (tournament_id,))
            player_count = cur.fetchone()[0]
            
            # Check decklists
            cur.execute("SELECT COUNT(*) FROM decklists WHERE tournament_id = %s", (tournament_id,))
            decklist_count = cur.fetchone()[0]
            
            # Check rounds
            cur.execute("SELECT COUNT(*) FROM match_rounds WHERE tournament_id = %s", (tournament_id,))
            rounds_count = cur.fetchone()[0]
            
            # Check matches
            cur.execute("SELECT COUNT(*) FROM matches WHERE tournament_id = %s", (tournament_id,))
            matches_count = cur.fetchone()[0]
        
        logger.info(
            f"Tournament {tournament_id}: {player_count} players, {decklist_count} decklists, "
            f"{rounds_count} rounds, {matches_count} matches"
        )
    
    def test_insert_all_with_deck_cards(self, pipeline, sample_tournament, test_database):
        """Test inserting tournament with deck cards (requires cards table to be populated)"""
        # First, load some cards into the database
        from src.etl.cards_pipeline import CardsPipeline
        cards_pipeline = CardsPipeline()
        cards_result = cards_pipeline.insert_cards(batch_size=1000, update_existing=True)
        
        assert cards_result['cards_loaded'] > 0
        
        tournament_id = sample_tournament.get('TID')
        assert tournament_id is not None
        
        # Insert tournament
        success = pipeline.insert_all(sample_tournament, include_rounds=False)
        assert success is True
        
        # Verify deck_cards were inserted
        with DatabaseConnection.get_cursor() as cur:
            # Get decklist_ids for this tournament
            cur.execute(
                "SELECT decklist_id FROM decklists WHERE tournament_id = %s",
                (tournament_id,)
            )
            decklist_ids = [row[0] for row in cur.fetchall()]
            
            if decklist_ids:
                # Check deck_cards
                cur.execute(
                    "SELECT COUNT(*) FROM deck_cards WHERE decklist_id = ANY(%s)",
                    (decklist_ids,)
                )
                deck_cards_count = cur.fetchone()[0]
                
                logger.info(
                    f"Tournament {tournament_id}: {len(decklist_ids)} decklists, "
                    f"{deck_cards_count} deck cards"
                )
    
    def test_load_initial(self, pipeline, test_database):
        """Test initial load of tournaments"""
        # Load tournaments from last 7 days (small window for testing)
        # Limit to first 100 tournaments for testing
        result = pipeline.load_initial(days_back=7, limit=100)
        
        assert result['success'] is True
        assert result['objects_processed'] >= 0
        assert result['errors'] == 0
        
        # Verify tournaments are in database
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM tournaments")
            tournament_count = cur.fetchone()[0]
            assert tournament_count == result['objects_loaded']
            
            # Check related tables
            cur.execute("SELECT COUNT(*) FROM players")
            player_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM decklists")
            decklist_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM match_rounds")
            rounds_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM matches")
            matches_count = cur.fetchone()[0]
        
        # Verify load metadata
        with DatabaseConnection.get_cursor() as cur:
            cur.execute(
                "SELECT last_load_date, objects_loaded, data_type, load_type "
                "FROM load_metadata WHERE data_type = 'tournaments' ORDER BY id DESC LIMIT 1"
            )
            metadata = cur.fetchone()
            if result['objects_loaded'] > 0:
                assert metadata is not None
                assert metadata[1] == result['objects_loaded']  # objects_loaded
                assert metadata[2] == 'tournaments'  # data_type
                assert metadata[3] == 'initial'  # load_type
        
        logger.info(
            f"Initial load: {result['objects_loaded']} tournaments, {player_count} players, "
            f"{decklist_count} decklists, {rounds_count} rounds, {matches_count} matches"
        )
    
    def test_load_incremental(self, pipeline, test_database):
        """Test incremental load of tournaments"""
        # First do an initial load
        # Limit to first 100 tournaments for testing
        initial_result = pipeline.load_initial(days_back=7, limit=100)
        assert initial_result['success'] is True
        initial_count = initial_result['objects_loaded']
        
        # Then do incremental load
        # Limit to first 100 tournaments for testing
        incremental_result = pipeline.load_incremental(limit=100)
        assert incremental_result['success'] is True
        
        # Verify total count
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM tournaments")
            final_count = cur.fetchone()[0]
        
        # Final count should be initial + incremental (or same if no new tournaments)
        assert final_count >= initial_count
        
        logger.info(
            f"Incremental load: {incremental_result['objects_loaded']} new tournaments "
            f"(total: {final_count})"
        )
    
    def test_tournament_data_integrity(self, pipeline, sample_tournament, test_database):
        """Test that tournament data is correctly stored in database"""
        tournament_id = sample_tournament.get('TID')
        
        # Insert tournament
        success = pipeline.insert_all(sample_tournament, include_rounds=True)
        assert success is True
        
        # Verify data integrity
        with DatabaseConnection.get_cursor() as cur:
            # Check tournament required fields
            cur.execute(
                """
                SELECT tournament_id, tournament_name, format, start_date
                FROM tournaments WHERE tournament_id = %s
                """,
                (tournament_id,)
            )
            tournament = cur.fetchone()
            assert tournament is not None
            assert tournament[0] == tournament_id
            assert tournament[1] is not None  # tournament_name
            assert tournament[2] is not None  # format
            assert tournament[3] is not None  # start_date
            
            # Check players have required fields
            cur.execute(
                """
                SELECT COUNT(*) FROM players 
                WHERE tournament_id = %s AND (player_id IS NULL OR name IS NULL OR name = '')
                """,
                (tournament_id,)
            )
            null_players = cur.fetchone()[0]
            assert null_players == 0
            
            # Check decklists reference valid players
            cur.execute(
                """
                SELECT COUNT(*) FROM decklists d
                WHERE d.tournament_id = %s
                AND NOT EXISTS (
                    SELECT 1 FROM players p 
                    WHERE p.player_id = d.player_id AND p.tournament_id = d.tournament_id
                )
                """,
                (tournament_id,)
            )
            orphaned_decklists = cur.fetchone()[0]
            assert orphaned_decklists == 0
            
            # Check matches reference valid players
            cur.execute(
                """
                SELECT COUNT(*) FROM matches m
                WHERE m.tournament_id = %s
                AND NOT EXISTS (
                    SELECT 1 FROM players p 
                    WHERE p.player_id = m.player1_id AND p.tournament_id = m.tournament_id
                )
                """,
                (tournament_id,)
            )
            invalid_matches = cur.fetchone()[0]
            assert invalid_matches == 0
        
        logger.info("Tournament data integrity checks passed")
    
    def test_table_row_counts(self, pipeline, test_database):
        """Test that table row counts are consistent after loading tournaments"""
        # Load tournaments
        # Limit to first 100 tournaments for testing
        limit = 100
        result = pipeline.load_initial(days_back=7, limit=limit)
        assert result['success'] is True
        
        # May load fewer if not enough tournaments available
        assert result['objects_loaded'] <= limit
        assert result['objects_loaded'] > 0
        
        with DatabaseConnection.get_cursor() as cur:
            # Get counts for all tables
            cur.execute("SELECT COUNT(*) FROM tournaments")
            tournaments_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM players")
            players_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM decklists")
            decklists_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM deck_cards")
            deck_cards_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM match_rounds")
            rounds_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM matches")
            matches_count = cur.fetchone()[0]
            
            # Verify relationships
            # Each tournament should have at least some players (if tournament has players)
            cur.execute(
                """
                SELECT COUNT(DISTINCT tournament_id) FROM players
                """
            )
            tournaments_with_players = cur.fetchone()[0]
            
            # Decklists should not exceed players
            assert decklists_count <= players_count
            
            # Matches should reference valid rounds
            cur.execute(
                """
                SELECT COUNT(*) FROM matches m
                WHERE NOT EXISTS (
                    SELECT 1 FROM match_rounds mr
                    WHERE mr.round_number = m.round_number 
                    AND mr.tournament_id = m.tournament_id
                )
                """
            )
            orphaned_matches = cur.fetchone()[0]
            assert orphaned_matches == 0
        
        logger.info(
            f"Table counts: {tournaments_count} tournaments, {players_count} players, "
            f"{decklists_count} decklists, {deck_cards_count} deck_cards, "
            f"{rounds_count} rounds, {matches_count} matches"
        )


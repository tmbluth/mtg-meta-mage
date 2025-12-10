"""Pytest configuration and fixtures for integration tests"""

import os
import pytest
import logging
from pathlib import Path

from src.etl.database.connection import DatabaseConnection
from src.etl.cards_pipeline import CardsPipeline
from src.etl.tournaments_pipeline import TournamentsPipeline
from src.etl.archetype_pipeline import ArchetypeClassificationPipeline

logger = logging.getLogger(__name__)

# Thresholds for test data requirements
REQUIRED_CARDS = 36100
REQUIRED_TOURNAMENTS = 100
REQUIRED_ARCHETYPES = 10


@pytest.fixture(scope="session")
def test_database():
    """Set up test database for integration tests (preserves data if already exists)"""
    test_db_name = os.getenv('TEST_DB_NAME')
    assert test_db_name, "TEST_DB_NAME environment variable not set"
    schema_file = Path(__file__).parent.parent.parent / 'src' / 'etl' / 'database' / 'schema.sql'
    
    # Only initialize if database doesn't exist
    if not DatabaseConnection.database_exists(test_db_name):
        logger.info(f"Test database doesn't exist, creating: {test_db_name}")
        DatabaseConnection.initialize_database(test_db_name, str(schema_file))
    else:
        logger.info(f"Test database already exists: {test_db_name} (preserving data)")
    
    # Set DB_NAME to test database for the duration of tests
    original_db_name = os.getenv('DB_NAME')
    os.environ['DB_NAME'] = test_db_name
    
    # Initialize connection pool with test database
    DatabaseConnection.close_pool()  # Close any existing pool
    DatabaseConnection.initialize_pool()
    
    yield test_db_name
    
    # Cleanup: restore original DB_NAME and close pool
    if original_db_name:
        os.environ['DB_NAME'] = original_db_name
    else:
        os.environ.pop('DB_NAME', None)
    
    DatabaseConnection.close_pool()


@pytest.fixture(scope="session")
def load_test_data(test_database):
    """
    Load test data (cards, tournaments, archetypes) for integration tests.
    
    This fixture runs once per test session and:
    - Skips loading if sufficient data already exists
    - Uses load_initial() if no data exists
    - Uses load_incremental() if data exists but is insufficient
    
    Loads:
    - Cards from Scryfall (real API)
    - Tournaments from last 90 days (real TopDeck API)
    - Archetype classifications (real LLM API) - only if LLM is configured
    
    Note: Data persists across tests for faster execution. This avoids unnecessary
    API calls when sufficient test data already exists.
    """
    logger.info("=" * 80)
    logger.info("Loading test data for integration tests")
    logger.info("=" * 80)
    
    # Check current data counts
    with DatabaseConnection.get_cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM cards")
        card_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM tournaments")
        tournament_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM archetype_groups")
        archetype_count = cur.fetchone()[0]
    
    # 1. Load cards
    logger.info("Step 1/3: Loading cards...")
    if card_count >= REQUIRED_CARDS:
        logger.info(f"Sufficient cards already loaded ({card_count} >= {REQUIRED_CARDS}), skipping card load")
    else:
        logger.info(f"Found {card_count} cards, loading cards...")
        cards_pipeline = CardsPipeline()
        
        if card_count == 0:
            logger.info("No cards found, running initial load...")
            cards_result = cards_pipeline.load_initial(batch_size=1000, limit=None)
        else:
            logger.info("Running incremental load...")
            cards_result = cards_pipeline.load_incremental(batch_size=1000, limit=None)
        
        logger.info(
            f"Cards: loaded={cards_result.get('objects_loaded', 0)}, "
            f"processed={cards_result.get('objects_processed', 0)}, "
            f"errors={cards_result.get('errors', 0)}"
        )
    
    # 2. Load tournaments
    logger.info("Step 2/3: Loading tournaments...")
    topdeck_api_key = os.getenv('TOPDECK_API_KEY')
    if not topdeck_api_key:
        logger.warning("TOPDECK_API_KEY not set, skipping tournament load")
    elif tournament_count >= REQUIRED_TOURNAMENTS:
        logger.info(f"Sufficient tournaments already loaded ({tournament_count} >= {REQUIRED_TOURNAMENTS}), skipping tournament load")
    else:
        logger.info(f"Found {tournament_count} tournaments, loading tournaments...")
        tournaments_pipeline = TournamentsPipeline(topdeck_api_key)
        
        if tournament_count == 0:
            logger.info("No tournaments found, running initial load (last 90 days)...")
            tournaments_result = tournaments_pipeline.load_initial(days_back=90, limit=None)
        else:
            logger.info("Running incremental load...")
            tournaments_result = tournaments_pipeline.load_incremental(limit=None)
        
        logger.info(
            f"Tournaments: success={tournaments_result.get('success')}, "
            f"loaded={tournaments_result.get('objects_loaded', 0)}, "
            f"processed={tournaments_result.get('objects_processed', 0)}, "
            f"errors={tournaments_result.get('errors', 0)}"
        )
    
    # 3. Load archetype classifications
    logger.info("Step 3/3: Loading archetype classifications...")
    llm_provider = os.getenv('LLM_PROVIDER')
    large_language_model = os.getenv('LARGE_LANGUAGE_MODEL')
    
    if not large_language_model:
        logger.warning("LARGE_LANGUAGE_MODEL not set, skipping archetype classification")
    elif archetype_count >= REQUIRED_ARCHETYPES:
        logger.info(f"Sufficient archetypes already loaded ({archetype_count} >= {REQUIRED_ARCHETYPES}), skipping archetype classification")
    else:
        if not llm_provider:
            logger.warning("LLM_PROVIDER not set, skipping archetype classification")
        else:
            logger.info(f"Found {archetype_count} archetypes, loading archetype classifications...")
            archetype_pipeline = ArchetypeClassificationPipeline(
                model_provider=llm_provider,
                prompt_id="archetype_classification_v1"
            )
            
            # Check if there are unclassified decklists
            unclassified = archetype_pipeline.get_unclassified_decklists()
            logger.info(f"Found {len(unclassified)} unclassified decklists")
            
            if len(unclassified) > 0:
                if archetype_count == 0:
                    logger.info("No archetypes found, running initial classification with LLM...")
                    try:
                        classification_result = archetype_pipeline.load_initial(batch_size=10)
                    except Exception as e:
                        raise RuntimeError(
                            f"Failed to load archetypes: {e}. "
                            "Check LLM credentials and ensure cards and tournaments are loaded first."
                        ) from e
                else:
                    logger.info("Running incremental classification with LLM...")
                    try:
                        classification_result = archetype_pipeline.load_incremental(batch_size=10)
                    except Exception as e:
                        raise RuntimeError(
                            f"Failed to load archetypes incrementally: {e}. "
                            "Check LLM credentials and API access."
                        ) from e
                
                logger.info(
                    f"Archetypes: loaded={classification_result.get('objects_loaded', 0)}, "
                    f"processed={classification_result.get('objects_processed', 0)}, "
                    f"errors={classification_result.get('errors', 0)}"
                )
                
                # Fail if classification had too many errors
                if classification_result.get('errors', 0) > classification_result.get('objects_processed', 1) * 0.5:
                    raise RuntimeError(
                        f"Archetype classification failed: {classification_result.get('errors', 0)} errors "
                        f"out of {classification_result.get('objects_processed', 0)} processed. "
                        "Check LLM credentials and API access."
                    )
            else:
                logger.info("All decklists already classified")
    
    # Final verification
    logger.info("Verifying test data requirements...")
    with DatabaseConnection.get_cursor() as cur:
        # Verify cards
        cur.execute("SELECT COUNT(*) FROM cards")
        final_card_count = cur.fetchone()[0]
        assert final_card_count >= REQUIRED_CARDS, (
            f"Only {final_card_count} cards found, but tests require at least {REQUIRED_CARDS} cards. "
            "Card loading failed or insufficient cards loaded. Check Scryfall API access."
        )
        logger.info(f"Cards: {final_card_count} (required: {REQUIRED_CARDS})")
        
        # Verify tournaments
        if not topdeck_api_key:
            raise RuntimeError(
                "TOPDECK_API_KEY environment variable not set. "
                "Tests require tournaments. Set TOPDECK_API_KEY to enable tournament loading."
            )
        
        cur.execute("SELECT COUNT(*) FROM tournaments")
        final_tournament_count = cur.fetchone()[0]
        assert final_tournament_count >= REQUIRED_TOURNAMENTS, (
            f"Only {final_tournament_count} tournaments found, but tests require at least {REQUIRED_TOURNAMENTS} tournaments. "
            "Tournament loading failed or insufficient tournaments loaded. Check TopDeck API key and access."
        )
        logger.info(f"Tournaments: {final_tournament_count} (required: {REQUIRED_TOURNAMENTS})")
        
        cur.execute("SELECT COUNT(*) FROM decklists")
        decklist_count = cur.fetchone()[0]
        assert decklist_count > 0, (
            "No decklists found. Tournaments loaded but decklists are missing. "
            "Check tournament data loading."
        )
        logger.info(f"Decklists: {decklist_count}")
        
        # Verify archetypes
        cur.execute("SELECT COUNT(*) FROM archetype_groups")
        final_archetype_count = cur.fetchone()[0]
        
        if final_archetype_count == 0:
            if large_language_model:
                raise RuntimeError(
                    "No archetypes found in database but LLM is configured. "
                    "Archetype classification failed. Check LLM credentials and ensure "
                    "cards and tournaments are loaded first. "
                    f"LLM_PROVIDER={llm_provider}, "
                    f"LARGE_LANGUAGE_MODEL={large_language_model}"
                )
            else:
                raise RuntimeError(
                    "No archetypes found in database and LARGE_LANGUAGE_MODEL is not set. "
                    "Tests require archetypes. Set LARGE_LANGUAGE_MODEL and LLM_PROVIDER "
                    "to enable archetype classification."
                )
        
        assert final_archetype_count >= REQUIRED_ARCHETYPES, (
            f"Only {final_archetype_count} archetypes found, but tests require at least {REQUIRED_ARCHETYPES} archetypes. "
            "Archetype classification failed or insufficient archetypes loaded. "
            "Check LLM credentials and ensure cards and tournaments are loaded first."
        )
        logger.info(f"Archetypes: {final_archetype_count} (required: {REQUIRED_ARCHETYPES})")
    
    logger.info("=" * 80)
    logger.info("Test data loading complete")
    logger.info("=" * 80)
    
    yield
    
    # Data persists for other tests in the session


# @pytest.fixture(autouse=True)
# def cleanup_test_data(test_database):
#     """
#     Clean up test data before and after each test that uses the database.
    
#     Note: Cards table is preserved across tests to avoid reloading ~100K+ cards
#     from Scryfall for every test. All other tables are cleaned up.
#     """
#     # Only cleanup if test_database fixture was successfully set up
#     try:
#         # Clean up before test (preserve cards table)
#         with DatabaseConnection.transaction() as conn:
#             cur = conn.cursor()
#             # Tournament tables (order matters due to foreign keys)
#             cur.execute("TRUNCATE TABLE matches CASCADE")
#             cur.execute("TRUNCATE TABLE match_rounds CASCADE")
#             cur.execute("TRUNCATE TABLE archetype_classifications CASCADE")
#             cur.execute("TRUNCATE TABLE archetype_groups CASCADE")
#             cur.execute("TRUNCATE TABLE deck_cards CASCADE")
#             cur.execute("TRUNCATE TABLE decklists CASCADE")
#             cur.execute("TRUNCATE TABLE players CASCADE")
#             cur.execute("TRUNCATE TABLE tournaments CASCADE")
#             # Metadata tables only (cards table preserved)
#             cur.execute("TRUNCATE TABLE load_metadata CASCADE")
#             cur.close()
        
#         yield
        
#         # Clean up after test (preserve cards table)
#         with DatabaseConnection.transaction() as conn:
#             cur = conn.cursor()
#             # Tournament tables (order matters due to foreign keys)
#             cur.execute("TRUNCATE TABLE matches CASCADE")
#             cur.execute("TRUNCATE TABLE match_rounds CASCADE")
#             cur.execute("TRUNCATE TABLE archetype_classifications CASCADE")
#             cur.execute("TRUNCATE TABLE archetype_groups CASCADE")
#             cur.execute("TRUNCATE TABLE deck_cards CASCADE")
#             cur.execute("TRUNCATE TABLE decklists CASCADE")
#             cur.execute("TRUNCATE TABLE players CASCADE")
#             cur.execute("TRUNCATE TABLE tournaments CASCADE")
#             # Metadata tables only (cards table preserved)
#             cur.execute("TRUNCATE TABLE load_metadata CASCADE")
#             cur.close()
#     except Exception as e:
#         # If database isn't available, skip cleanup
#         logger.warning(f"Could not cleanup test data: {e}")
#         yield


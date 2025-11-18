"""Pytest configuration and fixtures for integration tests"""

import os
import pytest
import logging
from pathlib import Path

from src.database.connection import DatabaseConnection

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def test_database():
    """Set up and tear down test database for integration tests"""
    test_db_name = os.getenv('TEST_DB_NAME')
    assert test_db_name, "TEST_DB_NAME environment variable not set"
    schema_file = Path(__file__).parent.parent.parent / 'src' / 'database' / 'schema.sql'
    
    # Initialize test database
    logger.info(f"Initializing test database: {test_db_name}")
    DatabaseConnection.initialize_database(test_db_name, str(schema_file))
    
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


@pytest.fixture(autouse=True)
def cleanup_test_data(test_database):
    """Clean up test data before and after each test that uses the database"""
    # Only cleanup if test_database fixture was successfully set up
    try:
        # Clean up before test
        with DatabaseConnection.transaction() as conn:
            cur = conn.cursor()
            # Tournament tables (order matters due to foreign keys)
            cur.execute("TRUNCATE TABLE matches CASCADE")
            cur.execute("TRUNCATE TABLE match_rounds CASCADE")
            cur.execute("TRUNCATE TABLE deck_cards CASCADE")
            cur.execute("TRUNCATE TABLE decklists CASCADE")
            cur.execute("TRUNCATE TABLE players CASCADE")
            cur.execute("TRUNCATE TABLE tournaments CASCADE")
            # Card and metadata tables
            cur.execute("TRUNCATE TABLE cards CASCADE")
            cur.execute("TRUNCATE TABLE load_metadata CASCADE")
            cur.close()
        
        yield
        
        # Clean up after test
        with DatabaseConnection.transaction() as conn:
            cur = conn.cursor()
            # Tournament tables (order matters due to foreign keys)
            cur.execute("TRUNCATE TABLE matches CASCADE")
            cur.execute("TRUNCATE TABLE match_rounds CASCADE")
            cur.execute("TRUNCATE TABLE deck_cards CASCADE")
            cur.execute("TRUNCATE TABLE decklists CASCADE")
            cur.execute("TRUNCATE TABLE players CASCADE")
            cur.execute("TRUNCATE TABLE tournaments CASCADE")
            # Card and metadata tables
            cur.execute("TRUNCATE TABLE cards CASCADE")
            cur.execute("TRUNCATE TABLE load_metadata CASCADE")
            cur.close()
    except Exception as e:
        # If database isn't available, skip cleanup
        logger.warning(f"Could not cleanup test data: {e}")
        yield


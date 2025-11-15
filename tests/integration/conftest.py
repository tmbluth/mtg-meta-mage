"""Pytest configuration and shared fixtures for integration tests"""

import pytest
import os
from dotenv import load_dotenv
from psycopg2.extensions import connection
from pathlib import Path

load_dotenv()

from src.database.connection import DatabaseConnection


@pytest.fixture(scope="function")
def test_db_connection() -> connection:
    """Fixture providing a test database connection with schema initialized"""
    # Use test database name from environment
    test_db_name = os.getenv('TEST_DB_NAME')
    if not test_db_name:
        pytest.fail("TEST_DB_NAME environment variable must be set in .env file")
    
    # Temporarily set DB_NAME to TEST_DB_NAME
    original_db_name = os.getenv('DB_NAME')
    os.environ['DB_NAME'] = test_db_name
    
    try:
        # Ensure clean state
        DatabaseConnection._connection_pool = None
        
        # Initialize schema
        schema_file = Path(__file__).parent.parent.parent / 'src' / 'database' / 'schema.sql'
        DatabaseConnection.execute_schema_file(str(schema_file))
        
        # Get connection
        conn = DatabaseConnection.get_connection()
        yield conn
        
        # Cleanup: rollback any uncommitted changes
        conn.rollback()
        DatabaseConnection.return_connection(conn)
    finally:
        # Restore original DB_NAME
        if original_db_name:
            os.environ['DB_NAME'] = original_db_name
        elif 'DB_NAME' in os.environ:
            del os.environ['DB_NAME']
        DatabaseConnection.close_pool()


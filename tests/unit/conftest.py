"""Pytest configuration and shared fixtures"""

import pytest
import os
from dotenv import load_dotenv
from psycopg2.extensions import connection, cursor
from unittest.mock import Mock, MagicMock, patch

load_dotenv()

from src.database.connection import DatabaseConnection
from pathlib import Path


@pytest.fixture(scope="session")
def db_config():
    """Database configuration fixture"""
    return {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }


@pytest.fixture(scope="session")
def has_db_config(db_config):
    """Check if database configuration is available"""
    required = ['database', 'user', 'password']
    return all(db_config.get(key) for key in required)


@pytest.fixture(scope="session")
def has_topdeck_api_key():
    """Check if TopDeck API key is available"""
    return os.getenv('TOPDECK_API_KEY') is not None


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


@pytest.fixture(scope="function")
def test_db_cursor(test_db_connection: connection) -> cursor:
    """Fixture providing a test database cursor"""
    cur = test_db_connection.cursor()
    yield cur
    cur.close()


def pytest_configure(config):
    """Register custom markers"""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test (makes real API/DB calls)"
    )
    config.addinivalue_line(
        "markers", "unit: mark test as unit test (no external dependencies)"
    )


# ETL Pipeline Test Fixtures

@pytest.fixture
def mock_pipeline():
    """Fixture that creates an ETLPipeline with mocked dependencies"""
    from src.etl.etl_pipeline import ETLPipeline
    
    with patch('src.etl.etl_pipeline.TopDeckClient') as mock_client_class:
        with patch('src.etl.etl_pipeline.DatabaseConnection.initialize_pool'):
            mock_client = Mock()
            mock_client_class.return_value = mock_client
            pipeline = ETLPipeline(api_key="test_key")
            pipeline.client = mock_client
            yield pipeline, mock_client


@pytest.fixture
def mock_db_connection():
    """Fixture for mock database connection and cursor"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn, mock_cursor


@pytest.fixture
def mock_db_transaction():
    """Fixture for mock database transaction context manager"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    
    with patch('src.etl.etl_pipeline.DatabaseConnection.transaction') as mock_transaction:
        mock_transaction.return_value.__enter__.return_value = mock_conn
        mock_transaction.return_value.__exit__.return_value = None
        yield mock_conn, mock_cursor, mock_transaction


@pytest.fixture
def mock_scryfall_client():
    """Fixture for mock Scryfall client"""
    with patch('src.etl.etl_pipeline.ScryfallClient') as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        yield mock_client


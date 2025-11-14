"""Pytest configuration and shared fixtures"""

import pytest
import os
from dotenv import load_dotenv

load_dotenv()


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


def pytest_configure(config):
    """Register custom markers"""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test (makes real API/DB calls)"
    )
    config.addinivalue_line(
        "markers", "unit: mark test as unit test (no external dependencies)"
    )


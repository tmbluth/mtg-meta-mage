"""Initialize the database schema"""

import os
import sys
import logging
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.database.connection import DatabaseConnection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def init_database():
    """Initialize the database schema"""
    try:
        # Get schema file path
        schema_file = Path(__file__).parent / 'schema.sql'
        
        if not schema_file.exists():
            logger.error(f"Schema file not found: {schema_file}")
            return False
        
        logger.info("Initializing database schema...")
        DatabaseConnection.execute_schema_file(str(schema_file))
        logger.info("Database schema initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        return False
    finally:
        DatabaseConnection.close_pool()


if __name__ == '__main__':
    success = init_database()
    sys.exit(0 if success else 1)


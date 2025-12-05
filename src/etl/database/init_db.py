"""Initialize the database schema"""

import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.etl.database.connection import DatabaseConnection

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def init_database(database_name: str, schema_file: Path) -> bool:
    """
    Initialize a single database: check if it exists, create if needed, and apply schema
    
    Args:
        database_name: Name of the database to initialize
        schema_file: Path to the schema SQL file
        
    Returns:
        True if successful, False otherwise
    """
    try:
        if not schema_file.exists():
            logger.error(f"Schema file not found: {schema_file}")
            return False
        
        DatabaseConnection.initialize_database(database_name, str(schema_file))
        return True
    except Exception as e:
        logger.error(f"Failed to initialize database '{database_name}': {e}")
        return False


def init_all_databases():
    """
    Initialize both main and test databases:
    - Check if databases exist
    - Create them if they don't exist
    - Establish schema, tables, indexes, functions, and triggers in both
    """
    try:
        # Get schema file path
        schema_file = Path(__file__).parent / 'schema.sql'
        
        if not schema_file.exists():
            logger.error(f"Schema file not found: {schema_file}")
            return False
        
        # Get database names from environment
        db_name = os.getenv('DB_NAME')
        test_db_name = os.getenv('TEST_DB_NAME')
        
        if not db_name:
            logger.error("DB_NAME environment variable is not set")
            return False
        
        if not test_db_name:
            logger.error("TEST_DB_NAME environment variable is not set")
            return False
        
        success = True
        
        # Initialize main database
        logger.info("=" * 60)
        logger.info(f"Initializing main database: {db_name}")
        logger.info("=" * 60)
        if not init_database(db_name, schema_file):
            success = False
        
        # Initialize test database
        logger.info("=" * 60)
        logger.info(f"Initializing test database: {test_db_name}")
        logger.info("=" * 60)
        if not init_database(test_db_name, schema_file):
            success = False
        
        if success:
            logger.info("=" * 60)
            logger.info("All databases initialized successfully")
            logger.info("=" * 60)
        
        return success
    except Exception as e:
        logger.error(f"Failed to initialize databases: {e}")
        return False
    finally:
        DatabaseConnection.close_pool()


if __name__ == '__main__':
    success = init_all_databases()
    sys.exit(0 if success else 1)


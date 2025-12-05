"""ETL-specific utility functions for pipeline operations"""

import logging
from datetime import datetime
from typing import Optional

from src.etl.database.connection import DatabaseConnection

# Import shared utilities from core_utils
from src.core_utils import normalize_card_name, parse_decklist, find_fuzzy_card_match

logger = logging.getLogger(__name__)

# Re-export shared functions for backward compatibility
__all__ = [
    'normalize_card_name',
    'parse_decklist', 
    'find_fuzzy_card_match',
    'get_last_load_timestamp',
    'update_load_metadata'
]


def get_last_load_timestamp(data_type: str) -> Optional[datetime]:
    """
    Get the timestamp of the last successful load for a specific data type
    
    Args:
        data_type: Type of data ('tournaments', 'cards', 'archetypes')
    
    Returns:
        datetime of last load, or None if no previous load
    """
    try:
        with DatabaseConnection.get_cursor() as cur:
            cur.execute(
                """
                SELECT last_load_date FROM load_metadata 
                WHERE data_type = %s 
                ORDER BY id DESC LIMIT 1
                """,
                (data_type,)
            )
            result = cur.fetchone()
            if result:
                return result[0]
            return None
    except Exception as e:
        logger.error(f"Error getting last load timestamp for {data_type}: {e}")
        return None


def update_load_metadata(
    last_timestamp: datetime,
    objects_loaded: int,
    data_type: str,
    load_type: str = 'incremental'
) -> None:
    """
    Update load metadata after successful load
    
    Args:
        last_timestamp: datetime of the latest item loaded
        objects_loaded: Number of items loaded in this batch
        data_type: Type of data ('tournaments', 'cards', 'archetypes')
        load_type: Type of load ('incremental', 'initial')
    """
    try:
        with DatabaseConnection.get_cursor(commit=True) as cur:
            cur.execute(
                """
                INSERT INTO load_metadata (last_load_date, objects_loaded, data_type, load_type)
                VALUES (%s, %s, %s, %s)
                """,
                (last_timestamp, objects_loaded, data_type, load_type)
            )
    except Exception as e:
        logger.error(f"Error updating load metadata for {load_type}, {data_type}: {e}")
        raise

